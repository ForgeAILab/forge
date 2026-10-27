use db::{task_condition::channels as channel, Task, TaskRepo, UpdateTask};

use crate::{Result, ServiceError};

use super::TaskDispatcher;

impl TaskDispatcher {
    pub(super) async fn block_task_for_workspace_reset(
        &self,
        task: &Task,
        error: &ServiceError,
    ) -> Result<()> {
        let annotation = channel::ErrorAnnotation::new(channel::AnnotationShape::TimedBlock(
            channel::TimedBlockAnnotation {
                kind: api_types::FailureKind::WorkspaceResetRequired,
                blocking_reason: "workspace_error".into(),
                blocked_by: api_types::Actor::system(api_types::SystemComponent::TaskDispatcher)
                    .display(),
                blocked_at: db::now_rfc3339(),
                message: error.to_string(),
            },
        ));
        TaskRepo::update(
            &*self.db,
            UpdateTask {
                id: task.id.clone(),
                expected_version: task.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: Some(Some(
                    serde_json::to_string(&annotation).expect("workspace annotation serializes"),
                )),
                blocked_json: None,
                failed_json: None,
                task_state_config: None,
                parent_task_id: None,
                updated_at: db::now_rfc3339(),
            },
        )
        .await?;
        self.task_service
            .fail_task(
                task.id.clone(),
                format!("workspace reset required: {error}"),
                Some(api_types::FailureKind::WorkspaceFailed),
                None,
            )
            .await?;
        Ok(())
    }

    pub(super) async fn block_task_on_workspace_error(
        &self,
        task: &Task,
        error: &ServiceError,
    ) -> Result<()> {
        let annotation = channel::ErrorAnnotation::new(channel::AnnotationShape::TimedBlock(
            channel::TimedBlockAnnotation {
                kind: api_types::FailureKind::WorkspaceError,
                blocking_reason: "workspace_error".into(),
                blocked_by: api_types::Actor::system(api_types::SystemComponent::TaskDispatcher)
                    .display(),
                blocked_at: db::now_rfc3339(),
                message: error.to_string(),
            },
        ));
        TaskRepo::update(
            &*self.db,
            UpdateTask {
                id: task.id.clone(),
                expected_version: task.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: Some(Some(
                    serde_json::to_string(&annotation).expect("workspace annotation serializes"),
                )),
                blocked_json: None,
                failed_json: None,
                task_state_config: None,
                parent_task_id: None,
                updated_at: db::now_rfc3339(),
            },
        )
        .await?;
        self.task_service
            .fail_task(
                task.id.clone(),
                format!("workspace error: {error}"),
                Some(api_types::FailureKind::WorkspaceFailed),
                None,
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod condition_tests;
