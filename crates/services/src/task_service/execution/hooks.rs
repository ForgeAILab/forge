use super::*;

impl TaskService {
    pub(super) async fn run_blocking_before_work_preflight(
        &self,
        task: &Task,
        project: &db::Project,
        workspace: &Workspace,
        agent_id: Option<&str>,
        execution_id: Option<&str>,
    ) -> Result<()> {
        let settings =
            serde_json::from_str::<ProjectSettings>(&project.settings).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid project settings: {error}"))
            })?;
        let hooks = settings
            .lifecycle_hooks
            .get(&api_types::LifecycleEvent::BeforeWork)
            .cloned()
            .unwrap_or_default();
        if !hooks.iter().any(|hook| {
            matches!(
                hook,
                api_types::LifecycleHookDef::Script { blocking: true, .. }
            )
        }) {
            return Ok(());
        }

        let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .filter(|repo| repo.project_id == project.id)
            .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
        let repo_path = repo
            .local_path
            .unwrap_or_else(|| workspace.worktree_path.clone());
        let log_dir = std::env::temp_dir()
            .join("forge")
            .join("logs")
            .join(&task.id)
            .join("hooks");
        let ctx = LifecycleHookContext {
            event: api_types::LifecycleEvent::BeforeWork,
            task_id: task.id.clone(),
            task_title: task.title.clone(),
            task_status: task.status.clone(),
            previous_status: task.status.clone(),
            project_id: project.id.clone(),
            project_name: project.name.clone(),
            repo_path,
            worktree_path: Some(workspace.worktree_path.clone()),
            agent_id: agent_id.map(ToOwned::to_owned),
            execution_id: execution_id.map(ToOwned::to_owned),
            log_dir: Some(log_dir),
        };

        if let Some(failure) =
            LifecycleHookRunner::run_blocking_before_work_hooks(ctx, &hooks).await
        {
            self.annotate_before_work_hook_block(task, &failure).await?;
            return Err(ServiceError::invalid_operation(format!(
                "blocking before_work hook failed: {}",
                failure
                    .error
                    .as_deref()
                    .unwrap_or("script did not complete successfully")
            )));
        }

        Ok(())
    }

    async fn annotate_before_work_hook_block(
        &self,
        task: &Task,
        failure: &LifecycleHookRun,
    ) -> Result<()> {
        let annotation_type = if failure.timed_out {
            api_types::FailureKind::BeforeWorkHookTimeout
        } else {
            api_types::FailureKind::BeforeWorkHookFailed
        };
        let command = failure.command.clone().unwrap_or_default();
        let stderr_summary = truncate_annotation_output(&failure.stderr);
        let stdout_summary = truncate_annotation_output(&failure.stdout);
        let annotation = json!({
            "type": annotation_type,
            "blocking_reason": annotation_type,
            "blocked_by": api_types::Actor::system(api_types::SystemComponent::LifecycleHook).display(),
            "blocked_at": now_rfc3339(),
            "blocked_execution_id": null,
            "message": failure.error.clone().unwrap_or_else(|| "blocking before_work hook failed".to_owned()),
            "hook": {
                "event": "before_work",
                "index": failure.index,
                "type": "script",
                "command": command,
                "exit_code": failure.exit_code,
                "timeout": failure.timed_out,
                "duration_ms": failure.duration_ms,
                "working_dir": failure.working_dir,
                "stdout": stdout_summary,
                "stderr": stderr_summary,
                "log_path": failure.log_path,
                "environment": failure.environment_preview,
            },
            "artifact": {
                "kind": "hook",
                "id": format!("before_work:{}", failure.index),
                "log_path": failure.log_path,
            },
            "recovery_actions": ["retry_hook", "update_workspace_and_retry_hook", "skip_hook_once", "cancel_task"],
        });

        TaskRepo::update(
            &*self.db,
            db::UpdateTask {
                id: task.id.clone(),
                expected_version: task.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: Some(Some(annotation.to_string())),
                blocked_json: None,
                failed_json: None,
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        Ok(())
    }
}

fn truncate_annotation_output(output: &str) -> String {
    const LIMIT: usize = 2048;
    if output.len() <= LIMIT {
        return output.to_owned();
    }
    let mut end = LIMIT;
    while !output.is_char_boundary(end) {
        end -= 1;
    }
    output[..end].to_owned()
}
