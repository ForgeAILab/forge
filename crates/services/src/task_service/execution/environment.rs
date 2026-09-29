//! Applying the Project environment immediately before an execution launches.

use super::*;
use executors::environment::{
    mark_task_environment, materialize_assets, run_environment_checks, EnvironmentCheckFailure,
};

/// Characters of check output kept in a blocking message.
const BLOCK_MESSAGE_OUTPUT_CHARS: usize = 1500;

impl TaskService {
    pub(super) async fn project_environment(
        &self,
        project_id: &str,
    ) -> Result<api_types::ProjectEnvironment> {
        let project = ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        let settings =
            serde_json::from_str::<ProjectSettings>(&project.settings).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid project settings: {error}"))
            })?;
        Ok(settings.environment)
    }

    /// Stamp the Project environment onto `agent_config`, copy its assets into
    /// the worktree, and run the checks gating this execution's role.
    ///
    /// Returns the terminal execution when a check or asset copy failed: the
    /// execution is failed before any provider call and the Task is parked as
    /// `environment_not_ready`, so no agent run is spent discovering it.
    pub(super) async fn prepare_execution_environment(
        &self,
        task: &Task,
        execution: &Execution,
        worktree_path: &str,
        agent_config: &mut Value,
    ) -> Result<Option<Execution>> {
        let environment = self.project_environment(&task.project_id).await?;
        mark_task_environment(agent_config, &environment.env);
        if environment.assets.is_empty() && environment.checks.is_empty() {
            return Ok(None);
        }
        let worktree = std::path::Path::new(worktree_path);
        let failure = match materialize_assets(worktree, &environment.assets).await {
            Err(message) => Some(message),
            Ok(()) => run_environment_checks(
                worktree,
                &environment.env,
                &environment.checks,
                &execution.role,
            )
            .await
            .as_ref()
            .map(check_failure_message),
        };
        let Some(message) = failure else {
            return Ok(None);
        };
        let message = format!("environment not ready: {message}");
        tracing::info!(
            execution_id = %execution.id,
            task_id = %task.id,
            %message,
            "execution parked before launch by a Project environment check"
        );
        let failed = self
            .fail_execution_before_dispatch(&execution.id, message.clone())
            .await?;
        self.block_task_for_environment(task, execution, message)
            .await?;
        Ok(Some(failed))
    }

    async fn block_task_for_environment(
        &self,
        task: &Task,
        execution: &Execution,
        message: String,
    ) -> Result<()> {
        let annotation = api_types::TaskBlockingAnnotation {
            annotation_type: api_types::FailureKind::EnvironmentNotReady,
            blocking_reason: "environment_not_ready".to_owned(),
            blocked_by: Some(
                api_types::Actor::system(api_types::SystemComponent::Dispatch).display(),
            ),
            blocked_at: Some(now_rfc3339()),
            blocked_execution_id: Some(execution.id.clone()),
            artifact: Some(api_types::BlockingArtifact {
                kind: "execution".to_owned(),
                id: Some(execution.id.clone()),
                log_path: execution.logs_path.clone(),
            }),
            message: Some(message.clone()),
            hook: None,
            recovery_actions: vec![
                api_types::RecoveryAction::Reexecute,
                api_types::RecoveryAction::CancelTask,
            ],
        };
        let annotation = serde_json::to_string(&annotation).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "failed to serialize environment annotation: {error}"
            ))
        })?;
        let blocked_meta = json!({
            "reason": message,
            "created_at": now_rfc3339(),
            "kind": api_types::FailureKind::EnvironmentNotReady,
            "execution_id": execution.id,
        });
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        let updated = TaskRepo::update_status(
            &*self.db,
            UpdateTaskStatus {
                id: current.id.clone(),
                expected_version: current.version,
                status: current.status.clone(),
                assignee_id: None,
                error_annotation: Some(Some(annotation)),
                blocked_json: Some(Some(blocked_meta.to_string())),
                failed_json: Some(None),
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        self.publish_domain_event_by_dedupe(&format!(
            "task-status-update:{}:{}",
            updated.id, updated.version
        ))
        .await;
        self.publish(ForgeEvent {
            event_type: "task.blocked".to_owned(),
            entity_id: updated.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskBlocked {
                project_id: updated.project_id,
                reason: message,
                kind: Some(api_types::FailureKind::EnvironmentNotReady),
                source: None,
                execution_id: Some(execution.id.clone()),
            },
        });
        Ok(())
    }
}

fn check_failure_message(failure: &EnvironmentCheckFailure) -> String {
    let output = failure.output_tail.trim();
    if output.is_empty() {
        return failure.message();
    }
    let chars = output.chars().count();
    let tail: String = if chars > BLOCK_MESSAGE_OUTPUT_CHARS {
        output
            .chars()
            .skip(chars - BLOCK_MESSAGE_OUTPUT_CHARS)
            .collect()
    } else {
        output.to_owned()
    };
    format!("{}\n{tail}", failure.message())
}
