use super::*;

impl TaskService {
    pub(super) async fn latest_executor_execution(&self, task_id: &str) -> Result<Execution> {
        let page = ExecutionRepo::list_by_task_and_role(
            &*self.db,
            task_id,
            crate::workflow::default_roles::WORKER,
            PageRequest {
                cursor: None,
                limit: 100,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        )
        .await?;
        if let Some(execution) = page.items.into_iter().next() {
            return Ok(execution);
        }

        let page = ExecutionRepo::list_by_task_and_role(
            &*self.db,
            task_id,
            "coder",
            PageRequest {
                cursor: None,
                limit: 100,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        )
        .await?;
        if let Some(execution) = page.items.into_iter().next() {
            return Ok(execution);
        }

        let page = ExecutionRepo::list_by_task_and_role(
            &*self.db,
            task_id,
            "executor",
            PageRequest {
                cursor: None,
                limit: 1,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        )
        .await?;
        page.items.into_iter().next().ok_or_else(|| {
            ServiceError::invalid_operation(format!("task {task_id} has no executor execution"))
        })
    }

    pub(super) async fn latest_review_for_task(&self, task_id: &str) -> Result<Review> {
        let reviews = ReviewRepo::list_by_task(&*self.db, task_id).await?;
        reviews
            .into_iter()
            .max_by_key(|review| review.attempt_number)
            .ok_or_else(|| {
                ServiceError::invalid_operation(format!("task {task_id} has no review records"))
            })
    }

    pub async fn add_user_comment(
        &self,
        task_id: &str,
        author_name: String,
        content: String,
    ) -> Result<TaskComment> {
        let now = now_rfc3339();
        let comment = TaskCommentRepo::create_comment(
            &*self.db,
            CreateTaskComment {
                id: new_uuid_v4(),
                task_id: task_id.to_owned(),
                author_type: CommentAuthorType::User,
                author_id: None,
                author_name,
                content,
                execution_id: None,
                role: None,
                worklog_kind: None,
                idempotency_key: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await?;
        if let Err(error) = self.index_task_comment_memory(task_id, &comment).await {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }
        self.publish(ForgeEvent {
            event_type: "comment.created".to_owned(),
            entity_id: comment.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::CommentCreated {
                task_id: task_id.to_owned(),
                comment_id: comment.id.clone(),
                author_type: "user".to_owned(),
                author_name: comment.author_name.clone(),
            },
        });
        Ok(comment)
    }

    pub(crate) async fn create_system_comment(&self, task_id: &str, content: String) -> Result<()> {
        let now = now_rfc3339();
        let comment = TaskCommentRepo::create_comment(
            &*self.db,
            CreateTaskComment {
                id: new_uuid_v4(),
                task_id: task_id.to_owned(),
                author_type: CommentAuthorType::System,
                author_id: None,
                author_name: "Forge".to_owned(),
                content,
                execution_id: None,
                role: None,
                worklog_kind: None,
                idempotency_key: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await?;
        if let Err(error) = self.index_task_comment_memory(task_id, &comment).await {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }
        self.publish(ForgeEvent {
            event_type: "comment.created".to_owned(),
            entity_id: comment.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::CommentCreated {
                task_id: task_id.to_owned(),
                comment_id: comment.id,
                author_type: "system".to_owned(),
                author_name: "Forge".to_owned(),
            },
        });
        Ok(())
    }

    pub(super) async fn create_agent_comment(
        &self,
        task_id: &str,
        agent_id: &str,
        content: String,
    ) -> Result<()> {
        let agent = match AgentRepo::get_by_id(&*self.db, agent_id).await? {
            Some(agent) => agent,
            None => return self.create_system_comment(task_id, content).await,
        };
        let now = now_rfc3339();
        let comment = TaskCommentRepo::create_comment(
            &*self.db,
            CreateTaskComment {
                id: new_uuid_v4(),
                task_id: task_id.to_owned(),
                author_type: CommentAuthorType::Agent,
                author_id: Some(agent.id.clone()),
                author_name: agent.name.clone(),
                content,
                execution_id: None,
                role: None,
                worklog_kind: None,
                idempotency_key: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await?;
        if let Err(error) = self.index_task_comment_memory(task_id, &comment).await {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }
        self.publish(ForgeEvent {
            event_type: "comment.created".to_owned(),
            entity_id: comment.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::CommentCreated {
                task_id: task_id.to_owned(),
                comment_id: comment.id,
                author_type: "agent".to_owned(),
                author_name: agent.name,
            },
        });
        Ok(())
    }

    async fn index_task_comment_memory(&self, task_id: &str, comment: &TaskComment) -> Result<()> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, true)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        self.memory_service
            .record_task_comment(&task.project_id, comment)
            .await?;
        Ok(())
    }

    pub async fn reset_task_workspace(&self, task_id: &str) -> Result<Workspace> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        reset_workspace(
            &self.db,
            &self.workspace_root,
            &task,
            self.repo_cache_locks.clone(),
        )
        .await
    }

    pub(super) async fn best_effort_git_diff(&self, task_id: &str) -> String {
        match self.git_diff(task_id).await {
            Ok(diff) => diff,
            Err(error) => {
                tracing::warn!(%task_id, %error, "failed to capture follow-up diff");
                String::new()
            }
        }
    }

    pub(super) async fn git_diff(&self, task_id: &str) -> Result<String> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        let repo_id = task
            .repo_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("task has no associated repo"))?;
        let repo = RepoRepo::get_by_id(&*self.db, repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", repo_id.to_owned()))?;
        let execution = self.latest_executor_execution(task_id).await?;
        let workspace_id = execution.workspace_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation("executor execution missing workspace_id")
        })?;
        let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;

        let branch_ref = format!("{}...HEAD", repo.default_branch);
        let output = Command::new("git")
            .arg("diff")
            .arg(branch_ref)
            .current_dir(&workspace.worktree_path)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .await;
        let stdout = match output {
            Ok(output) if output.status.success() => output.stdout,
            _ => {
                Command::new("git")
                    .arg("diff")
                    .current_dir(&workspace.worktree_path)
                    .env_remove("GIT_DIR")
                    .env_remove("GIT_WORK_TREE")
                    .env_remove("GIT_INDEX_FILE")
                    .output()
                    .await
                    .map_err(|error| {
                        ServiceError::invalid_operation(format!("failed to run git diff: {error}"))
                    })?
                    .stdout
            }
        };
        Ok(truncate_utf8_bytes(&stdout, MAX_FOLLOW_UP_DIFF_BYTES))
    }
}
