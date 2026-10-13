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
    db.remember_execution_plan_seed(db::RememberExecutionPlanSeedInput {
        seed_digest: seed.as_deref().map(digest),
        seed_size: seed.as_ref().map(|text| text.len() as i64),
        updated_at: db::now_rfc3339(),
        execution_id,
    })
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
            let seed: Option<String> = db
                .execution_plan_seed_digest(&execution.id)
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
    Ok(db
        .stored_execution_plan_candidate(execution_id)
        .await?
        .unwrap_or_default())
}

pub(crate) async fn retry_due(
    db: &db::SqliteDb,
    execution_id: &str,
    operation: &str,
) -> Result<()> {
    let retry: Option<String> = db
        .execution_plan_retry_at(execution_id, operation)
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
    let task_id: Option<String> = db.execution_plan_task_id(execution_id).await?;
    let Some(task_id) = task_id else {
        return Ok(());
    };
    let Some(task) = TaskRepo::get_by_id(db, &task_id, false).await? else {
        return Ok(());
    };
    let now = chrono::Utc::now();
    let mut tx = db::begin_immediate(db.pool()).await?;
    let attempts: Option<i64> = db
        .execution_plan_retry_count_in_tx(&mut tx, execution_id, operation)
        .await?;
    let attempts = attempts.unwrap_or(0).saturating_add(1);
    let delay = (5_i64 * (1_i64 << (attempts - 1).min(6))).min(300);
    let retry_at = (now + chrono::Duration::seconds(delay)).to_rfc3339();
    let message = format!("Waiting to {operation} execution plan on daemon {daemon_id}: {error}");
    db.record_execution_plan_transport_retry_in_tx(
        &mut tx,
        db::RecordExecutionPlanTransportRetryInTxInput {
            execution_id,
            operation,
            attempts,
            retry_at: &retry_at,
            message: &message,
            updated_at: now.to_rfc3339(),
        },
    )
    .await?;
    let offline = matches!(
        error,
        crate::workspace_backend::WorkspaceBackendError::OwnerUnreachable { .. }
            | crate::workspace_backend::WorkspaceBackendError::RpcTimeoutBeforeStart { .. }
    ) || matches!(error, crate::workspace_backend::WorkspaceBackendError::Other(error)
            if matches!(error.as_ref(), ServiceError::DaemonUnavailable { .. } | ServiceError::DaemonTimeout { .. }));
    let marker = serde_json::json!({"execution_id":execution_id,"operation":operation,"daemon_id":daemon_id,"message":message,"retry_at":retry_at});
    let annotation = serde_json::json!({"type":"plan_settlement_wait","code":if offline {"runtime_offline"} else {"plan_owner_error"}, "message":message,"daemon_id":daemon_id,"execution_id":execution_id,"retry_at":retry_at});
    let changed = db.record_execution_plan_task_wait_in_tx(&mut tx, db::RecordExecutionPlanTaskWaitInTxInput { task_id: &task.id, marker: marker.to_string(), disposition: serde_json::json!({"target_state":task.status,"reason":message,"not_before":retry_at}).to_string(), annotation: annotation.to_string(), updated_at: now.to_rfc3339(), version: task.version }).await?;
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
        let _queued_or_applied = db.record_execution_plan_owner_wait_in_tx(&mut tx, &task.id, serde_json::json!({"daemon_id":daemon_id,"started_at":started_at,"plan_execution_id":execution_id}).to_string()).await?;
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
    let task_id: String = db.execution_plan_required_task_id(execution_id).await?;
    db.clear_execution_plan_transport_retry(execution_id)
        .await?;
    let task_version: Option<i64> = db.execution_plan_wait_task_version(execution_id).await?;
    db.clear_execution_plan_task_wait(&task_id, execution_id, db::now_rfc3339(), task_version)
        .await?;
    db.resolve_execution_plan_wait_attention(
        db::now_rfc3339(),
        db::now_rfc3339(),
        format!("task-plan-wait:{execution_id}"),
    )
    .await?;
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
