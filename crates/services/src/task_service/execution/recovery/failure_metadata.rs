//! Recovery annotation clearing and Task failure transitions.

use super::*;

impl TaskService {
    pub(super) fn parse_blocking_annotation(
        &self,
        task: &Task,
    ) -> Option<api_types::TaskBlockingAnnotation> {
        task.condition.read().diagnostic
    }

    /// Clear the interruption projection using the exact Task snapshot that
    /// authorized the recovery attempt.  The version predicate is important:
    /// a caller that loaded an older annotation must never clear a newer one
    /// that won a concurrent update.
    pub(crate) async fn clear_recovery_metadata_at_version(&self, task: &Task) -> Result<Task> {
        if Self::is_replaying_recovery(&task.id) {
            let mut cleared = task.clone();
            cleared.error_annotation = None;
            cleared.blocked_json = None;
            cleared.condition = db::map_legacy_condition(&db::LegacyConditionInput::from(&cleared));
            return Ok(cleared);
        }
        let previous_reason = task.condition.read().interruption.map(|i| i.reason);
        let updated = TaskRepo::update(
            &*self.db,
            UpdateTask {
                id: task.id.clone(),
                expected_version: task.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: Some(None),
                blocked_json: Some(None),
                failed_json: None,
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        if task.condition.read().interruption_present {
            self.publish(ForgeEvent {
                event_type: "task.unblocked".to_owned(),
                entity_id: updated.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskUnblocked {
                    project_id: updated.project_id.clone(),
                    previous_reason,
                },
            });
        }
        Ok(updated)
    }

    pub(crate) async fn clear_blocking_metadata(&self, task_id: &str) -> Result<Task> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let updated = self.clear_recovery_metadata_at_version(&task).await?;
        crate::task_service::execution::clear_execution_retry_metadata(&self.db, &updated).await?;
        Ok(updated)
    }

    pub(crate) async fn restore_recovery_metadata_after_failed_resume(
        &self,
        cleared_task: &Task,
        original_task: &Task,
        workspace_id: Option<&str>,
        execution_role: &str,
    ) {
        if Self::is_replaying_recovery(&cleared_task.id) {
            return;
        }
        // Restore only if the clear is still the latest Task write and no
        // replacement execution was admitted in the clear-to-launch gap. The
        // DB boundary checks both facts under one write transaction; in
        // particular, an execution insert does not bump Task.version.
        let result = TaskRepo::update_recovery_metadata_if_no_running_execution(
            &*self.db,
            &cleared_task.id,
            cleared_task.version,
            original_task.error_annotation.clone(),
            original_task.blocked_json.clone(),
            original_task.failed_json.clone(),
            &now_rfc3339(),
            workspace_id,
            overlapping_execution_roles(execution_role),
            Vec::new(),
            None,
        )
        .await;
        if let Err(error) = result {
            tracing::warn!(
                task_id = %cleared_task.id,
                %error,
                "failed to restore recovery metadata after resume failure"
            );
        }
    }

    pub async fn fail_task(
        &self,
        task_id: impl Into<String>,
        reason: impl Into<String>,
        kind: Option<api_types::FailureKind>,
        execution_id: Option<String>,
    ) -> Result<Task> {
        let task_id: String = task_id.into();
        let reason: String = reason.into();
        if !db::task_writer::owns_task(&task_id) {
            return self
                .request_task_command(
                    &task_id,
                    "fail_task",
                    serde_json::json!([task_id, reason, kind, execution_id]),
                    false,
                )
                .await;
        }

        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        let failed_meta = json!({
            "reason": reason,
            "created_at": now_rfc3339(),
            "kind": kind,
            "execution_id": execution_id,
        });
        let updated = TaskRepo::update(
            &*self.db,
            UpdateTask {
                id: task.id.clone(),
                expected_version: task.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: Some(None),
                blocked_json: Some(None),
                failed_json: Some(Some(failed_meta.to_string())),
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        self.publish(ForgeEvent {
            event_type: "task.failed".to_owned(),
            entity_id: updated.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskFailed {
                project_id: updated.project_id.clone(),
                reason: reason.clone(),
                kind,
                execution_id: execution_id.clone(),
            },
        });
        tracing::info!(
            task_id = %updated.id,
            status = %updated.status,
            reason = %reason,
            kind = ?kind,
            execution_id = ?execution_id,
            "task marked as failed"
        );
        Ok(updated)
    }
}
