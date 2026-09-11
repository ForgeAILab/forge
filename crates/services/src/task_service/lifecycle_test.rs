use super::*;

impl TaskService {
    pub async fn test_lifecycle_hook(
        &self,
        project_id: &str,
        task_id: &str,
        event: api_types::LifecycleEvent,
        hook_index: usize,
    ) -> Result<api_types::LifecycleHookTestResponse> {
        let project = ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        if task.project_id != project.id {
            return Err(ServiceError::invalid_operation(format!(
                "task {task_id} does not belong to project {project_id}"
            )));
        }

        let settings =
            serde_json::from_str::<ProjectSettings>(&project.settings).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid project settings: {error}"))
            })?;
        let hooks = settings.lifecycle_hooks.get(&event).ok_or_else(|| {
            ServiceError::invalid_operation(format!(
                "no lifecycle hooks configured for event {}",
                serde_json::to_string(&event).unwrap_or_else(|_| "\"unknown\"".to_owned())
            ))
        })?;
        let hook = hooks.get(hook_index).ok_or_else(|| {
            ServiceError::invalid_operation(format!(
                "hook index {hook_index} is out of range for lifecycle event"
            ))
        })?;
        let (command, timeout_seconds) = match hook {
            api_types::LifecycleHookDef::Script {
                command,
                timeout_seconds,
                ..
            } => (command.as_str(), *timeout_seconds),
            api_types::LifecycleHookDef::Plugin { .. } => {
                return Err(ServiceError::invalid_operation(
                    "plugin lifecycle hooks are not testable through script hook runner",
                ));
            }
        };

        let workspace = prepare_workspace(
            &self.db,
            &self.workspace_root,
            &task,
            &task.id,
            self.repo_cache_locks.clone(),
        )
        .await?;
        let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id).await?;
        let repo_path = repo
            .and_then(|repo| repo.local_path)
            .unwrap_or_else(|| workspace.worktree_path.clone());
        let log_dir = std::env::temp_dir()
            .join("forge")
            .join("logs")
            .join(&task.id)
            .join("hooks");
        let hook_ctx = LifecycleHookContext {
            event,
            task_id: task.id.clone(),
            task_title: task.title.clone(),
            task_status: task.status.clone(),
            previous_status: task.status,
            project_id: project.id,
            project_name: project.name,
            repo_path,
            worktree_path: Some(workspace.worktree_path),
            agent_id: None,
            execution_id: None,
            log_dir: Some(log_dir),
        };
        let run =
            LifecycleHookRunner::test_script_hook(&hook_ctx, hook_index, command, timeout_seconds)
                .await;

        Ok(api_types::LifecycleHookTestResponse {
            status: run.status,
            stdout: run.stdout,
            stderr: run.stderr,
            exit_code: run.exit_code,
            duration_ms: run.duration_ms,
            timeout: run.timed_out,
            working_dir: run.working_dir,
            environment_preview: run.environment_preview,
            hook_log_path: run.log_path,
        })
    }
}
