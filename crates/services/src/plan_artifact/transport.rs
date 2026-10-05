use crate::{Result, ServiceError};
use api_types::MAX_EXECUTION_PLAN_BYTES;
use db::{Execution, TaskRepo};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub(crate) fn digest(content: &str) -> String {
    hex::encode(Sha256::digest(content.as_bytes()))
}

pub(crate) async fn remember_seed(
    db: &db::SqliteDb,
    execution_id: &str,
    seed: Option<&str>,
    env: &BTreeMap<String, String>,
) -> Result<()> {
    let seed = seed.map(|text| executors::environment::redact_environment_values(text, env));
    sqlx::query(
        "INSERT INTO execution_plan_transport (execution_id, seed_digest, seed_size, updated_at)
        SELECT id, ?, ?, ? FROM execution WHERE id=? AND status='running'
        ON CONFLICT(execution_id) DO NOTHING",
    )
    .bind(seed.as_deref().map(digest))
    .bind(seed.as_ref().map(|text| text.len() as i64))
    .bind(db::now_rfc3339())
    .bind(execution_id)
    .execute(db.pool())
    .await?;
    Ok(())
}

pub(crate) async fn candidate(
    db: &db::SqliteDb,
    execution: &Execution,
    text: Option<&str>,
    env: &BTreeMap<String, String>,
) -> Result<db::TransportedExecutionPlan> {
    let size = text.map(|text| text.len() as i64);
    let mut error = None;
    let mut content = text.map(|text| executors::environment::redact_environment_values(text, env));
    if let Some(text) = &content {
        if size.is_some_and(|size| size as u64 > MAX_EXECUTION_PLAN_BYTES)
            || text.len() as u64 > MAX_EXECUTION_PLAN_BYTES
        {
            error = Some(format!(
                "execution plan exceeds the {MAX_EXECUTION_PLAN_BYTES}-byte limit: {} bytes",
                size.unwrap_or_default().max(text.len() as i64)
            ));
        } else if !executors::plan_has_checklist(text) {
            error = Some("this execution's plan candidate has no checklist items".into());
        } else {
            let seed: Option<String> = sqlx::query_scalar(
                "SELECT seed_digest FROM execution_plan_transport WHERE execution_id=?",
            )
            .bind(&execution.id)
            .fetch_optional(db.pool())
            .await?
            .flatten();
            if seed.as_deref() == Some(&digest(text)) {
                error = Some(
                    "this execution did not write a plan: its candidate is identical to the seed"
                        .into(),
                );
            }
        }
    }
    if error.is_some() {
        content = None;
    }
    Ok(db::TransportedExecutionPlan {
        content,
        error,
        size,
    })
}

pub(crate) async fn stored(
    db: &db::SqliteDb,
    execution_id: &str,
) -> Result<(Option<String>, Option<String>)> {
    Ok(sqlx::query_as::<_, (Option<String>, Option<String>)>(
        "SELECT candidate_text, candidate_error FROM execution_plan_transport WHERE execution_id=?",
    )
    .bind(execution_id)
    .fetch_optional(db.pool())
    .await?
    .unwrap_or_default())
}

pub(crate) async fn retry_due(
    db: &db::SqliteDb,
    execution_id: &str,
    operation: &str,
) -> Result<()> {
    let retry: Option<String> = sqlx::query_scalar(
        "SELECT retry_at FROM execution_plan_transport WHERE execution_id=? AND retry_operation=?",
    )
    .bind(execution_id)
    .bind(operation)
    .fetch_optional(db.pool())
    .await?
    .flatten();
    if let Some(at) = retry.filter(|at| at.as_str() > db::now_rfc3339().as_str()) {
        return Err(ServiceError::invalid_operation(format!(
            "execution plan settlement is waiting until {at}"
        )));
    }
    Ok(())
}

pub(crate) async fn record_retry(
    db: &db::SqliteDb,
    execution_id: &str,
    operation: &str,
    daemon_id: &str,
    error: &crate::workspace_backend::WorkspaceBackendError,
) -> Result<()> {
    let task_id: Option<String> = sqlx::query_scalar("SELECT task_id FROM execution WHERE id=?")
        .bind(execution_id)
        .fetch_optional(db.pool())
        .await?;
    let Some(task_id) = task_id else {
        return Ok(());
    };
    let Some(task) = TaskRepo::get_by_id(db, &task_id, false).await? else {
        return Ok(());
    };
    let now = chrono::Utc::now();
    let mut tx = db::begin_immediate(db.pool()).await?;
    let attempts: Option<i64> = sqlx::query_scalar("SELECT retry_count FROM execution_plan_transport WHERE execution_id=? AND retry_operation=?")
        .bind(execution_id).bind(operation).fetch_optional(&mut *tx).await?;
    let attempts = attempts.unwrap_or(0).saturating_add(1);
    let delay = (5_i64 * (1_i64 << (attempts - 1).min(6))).min(300);
    let retry_at = (now + chrono::Duration::seconds(delay)).to_rfc3339();
    let message = format!("Waiting to {operation} execution plan on daemon {daemon_id}: {error}");
    sqlx::query("INSERT INTO execution_plan_transport (execution_id, retry_operation, retry_count, retry_at, retry_error, updated_at)
        VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(execution_id) DO UPDATE SET retry_operation=excluded.retry_operation,
        retry_count=excluded.retry_count, retry_at=excluded.retry_at, retry_error=excluded.retry_error, updated_at=excluded.updated_at")
        .bind(execution_id).bind(operation).bind(attempts).bind(&retry_at).bind(&message).bind(now.to_rfc3339())
        .execute(&mut *tx).await?;
    let offline = matches!(
        error,
        crate::workspace_backend::WorkspaceBackendError::OwnerUnreachable { .. }
            | crate::workspace_backend::WorkspaceBackendError::RpcTimeoutBeforeStart { .. }
    ) || matches!(error, crate::workspace_backend::WorkspaceBackendError::Other(error)
            if matches!(error.as_ref(), ServiceError::DaemonUnavailable { .. } | ServiceError::DaemonTimeout { .. }));
    let marker = serde_json::json!({"execution_id":execution_id,"operation":operation,"daemon_id":daemon_id,"message":message,"retry_at":retry_at});
    let annotation = serde_json::json!({"type":"plan_settlement_wait","code":if offline {"runtime_offline"} else {"plan_owner_error"}, "message":message,"daemon_id":daemon_id,"execution_id":execution_id,"retry_at":retry_at});
    let changed = db::task_writer::TaskQuery::new(db,&task.id,"UPDATE task SET metadata_json=json_set(COALESCE(metadata_json, '{}'), '$.plan_settlement_wait', json(?), '$.deferred_dispatch', json(?)),
        error_annotation=CASE WHEN error_annotation IS NULL OR json_extract(error_annotation, '$.type')='plan_settlement_wait' THEN ? ELSE error_annotation END,
        version=version+1, updated_at=? WHERE id=? AND version=?")
        .bind(marker.to_string()).bind(serde_json::json!({"target_state":task.status,"reason":message,"not_before":retry_at}).to_string())
        .bind(annotation.to_string()).bind(now.to_rfc3339()).bind(&task.id).bind(task.version)
        .identity_fenced().execute_in_tx(&mut tx).await?;
    // A blocking wait must never be lost: outside the Task lease it is
    // queued identity-fenced, so it applies even after a status change and
    // the attention recorded below stays truthful.
    if changed.applied().is_some_and(|rows| rows != 1) {
        return Err(db::DbError::VersionConflict.into());
    }
    if offline {
        let started_at = task
            .metadata_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .and_then(|metadata| {
                metadata["owner_wait"]["started_at"]
                    .as_str()
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| now.to_rfc3339());
        let _queued_or_applied = db::task_writer::TaskQuery::new(db,&task.id,"UPDATE task SET metadata_json=json_set(metadata_json, '$.owner_wait', json(?)) WHERE id=?")
            .bind(serde_json::json!({"daemon_id":daemon_id,"started_at":started_at,"plan_execution_id":execution_id}).to_string()).bind(&task.id)
            .identity_fenced().execute_in_tx(&mut tx).await?;
    }
    crate::placement::admission::record_wait_attention_in_tx(
        db,
        &mut tx,
        &task,
        if offline {
            "runtime_offline"
        } else {
            "plan_settlement_wait"
        },
        &message,
        &format!("task-plan-wait:{execution_id}"),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn clear_retry(db: &db::SqliteDb, execution_id: &str) -> Result<()> {
    let task_id: String = sqlx::query_scalar("SELECT task_id FROM execution WHERE id=?")
        .bind(execution_id)
        .fetch_one(db.pool())
        .await?;
    sqlx::query("UPDATE execution_plan_transport SET retry_operation=NULL,retry_count=0,retry_at=NULL,retry_error=NULL WHERE execution_id=?")
        .bind(execution_id).execute(db.pool()).await?;
    let task_version: Option<i64> = sqlx::query_scalar("SELECT version FROM task WHERE json_extract(metadata_json, '$.plan_settlement_wait.execution_id')=?")
        .bind(execution_id).fetch_optional(db.pool()).await?;
    db::task_writer::TaskQuery::new(db,&task_id,"UPDATE task SET error_annotation=CASE WHEN json_extract(error_annotation, '$.type')='plan_settlement_wait' AND json_extract(error_annotation, '$.execution_id')=? THEN NULL ELSE error_annotation END,
        metadata_json=CASE WHEN json_extract(metadata_json, '$.owner_wait.plan_execution_id')=?
          THEN json_remove(metadata_json, '$.plan_settlement_wait', '$.deferred_dispatch', '$.owner_wait')
          ELSE json_remove(metadata_json, '$.plan_settlement_wait', '$.deferred_dispatch') END,
        version=version+1,updated_at=? WHERE json_extract(metadata_json, '$.plan_settlement_wait.execution_id')=? AND version=?")
        .bind(execution_id).bind(execution_id).bind(db::now_rfc3339()).bind(execution_id).bind(task_version).execute(db.pool()).await?;
    sqlx::query("UPDATE attention_projection SET status='resolved', resolved_at=?, updated_at=? WHERE dedupe_key=? AND status<>'resolved'")
        .bind(db::now_rfc3339()).bind(db::now_rfc3339()).bind(format!("task-plan-wait:{execution_id}")).execute(db.pool()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fixture() -> (db::SqliteDb, db::Task, db::Execution) {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = db::SqliteDb::new(pool);
        let (task, _, execution) = crate::recovery::tests::daemon_owned_fixture(&db).await;
        (db, task, execution)
    }

    #[tokio::test]
    async fn remote_plan_owner_offline_wait_is_durable_and_cleared_on_success() {
        let (db, task, execution) = fixture().await;
        record_retry(
            &db,
            &execution.id,
            "publish",
            "offline-owner",
            &crate::workspace_backend::WorkspaceBackendError::OwnerUnreachable {
                daemon_id: "offline-owner".into(),
            },
        )
        .await
        .unwrap();
        crate::test_support::drain_task_steps(&db, &task.id).await;
        let waiting = TaskRepo::get_by_id(&db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let metadata: serde_json::Value =
            serde_json::from_str(waiting.metadata_json.as_deref().unwrap()).unwrap();
        assert_eq!(metadata["owner_wait"]["daemon_id"], "offline-owner");
        assert_eq!(metadata["owner_wait"]["plan_execution_id"], execution.id);
        assert!(metadata["deferred_dispatch"].is_object());
        assert!(waiting
            .error_annotation
            .unwrap()
            .contains("runtime_offline"));
        assert!(retry_due(&db, &execution.id, "publish").await.is_err());
        clear_retry(&db, &execution.id).await.unwrap();
        crate::test_support::drain_task_steps(&db, &task.id).await;
        let current = TaskRepo::get_by_id(&db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(current.error_annotation.is_none());
        let metadata: serde_json::Value =
            serde_json::from_str(current.metadata_json.as_deref().unwrap()).unwrap();
        assert!(metadata["owner_wait"].is_null());
        assert!(metadata["deferred_dispatch"].is_null());
        retry_due(&db, &execution.id, "publish").await.unwrap();
    }

    #[tokio::test]
    async fn remote_plan_discard_cleaned_placement_without_handle_succeeds_offline() {
        let (db, _, execution) = fixture().await;
        let mut placement = db::WorkspacePlacementRepo::get_by_workspace_id(
            &db,
            execution.workspace_id.as_deref().unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        placement.state = db::PlacementState::Cleaned;
        placement.workspace_handle = None;
        let resolved = crate::workspace_backend::ResolvedWorkspace {
            placement,
            backend: std::sync::Arc::new(crate::workspace_backend::DaemonWorkspaceBackend::new(
                std::sync::Arc::new(db::SqliteDb::new(db.pool().clone())),
                std::sync::Arc::new(
                    crate::daemon_transport::DaemonConnectionRegistry::without_handlers(),
                ),
            )),
        };
        crate::plan_artifact::ExecutionPlan::new(&db, &resolved)
            .discard(&execution.id)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn remote_plan_candidate_redacts_and_rejects_unchanged_seed_and_raw_oversize() {
        let (db, _, execution) = fixture().await;
        let env = BTreeMap::from([("TOKEN".into(), "secret-token".into())]);
        remember_seed(&db, &execution.id, Some("- [ ] secret-token seed"), &env)
            .await
            .unwrap();
        let unchanged = candidate(&db, &execution, Some("- [ ] secret-token seed"), &env)
            .await
            .unwrap();
        assert!(unchanged.error.unwrap().contains("identical"));
        let changed = candidate(&db, &execution, Some("- [x] secret-token seed"), &env)
            .await
            .unwrap();
        assert_eq!(changed.content.as_deref(), Some("- [x] [REDACTED] seed"));
        let oversized = format!(
            "- [ ] {}",
            "secret-token".repeat(MAX_EXECUTION_PLAN_BYTES as usize / 12 + 1)
        );
        let rejected = candidate(&db, &execution, Some(&oversized), &env)
            .await
            .unwrap();
        assert!(rejected.content.is_none());
        assert!(rejected
            .error
            .unwrap()
            .contains(&oversized.len().to_string()));
        assert_eq!(rejected.size, Some(oversized.len() as i64));
    }
}
