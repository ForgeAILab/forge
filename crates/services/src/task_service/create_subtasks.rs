use super::*;

pub struct NewSubtaskInput {
    pub title: String,
    pub description: Option<String>,
    pub assignee_id: Option<String>,
}

impl TaskService {
    pub async fn create_subtasks(
        &self,
        parent_task_id: String,
        items: Vec<NewSubtaskInput>,
    ) -> Result<Vec<Task>> {
        validate_required("parent_task_id", &parent_task_id)?;
        let parent = TaskRepo::get_by_id(&*self.db, &parent_task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", parent_task_id.clone()))?;
        if parent.parent_task_id.is_some() {
            return Err(ServiceError::nested_subtask_unsupported());
        }

        for item in &items {
            validate_required("title", &item.title)?;
        }

        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let start_order = sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(subtask_order) + 1, 0) FROM task WHERE parent_task_id = ? AND deleted_at IS NULL",
        )
        .bind(&parent_task_id)
        .fetch_one(&mut *transaction)
        .await?;

        let mut task_ids = Vec::with_capacity(items.len());
        for (offset, item) in items.into_iter().enumerate() {
            let task_id = new_uuid_v4();
            let now = now_rfc3339();
            let metadata = TaskMetadata {
                ..TaskMetadata::default()
            };
            let metadata_json = metadata.to_json();
            sqlx::query("INSERT INTO task (id, project_id, repo_id, parent_task_id, assignee_type, assignee_id, title, description, task_type, status, priority, subtask_order, task_state_config, merge_config, metadata_json, plan, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(&task_id)
                .bind(&parent.project_id)
                .bind(parent.repo_id.as_deref())
                .bind(&parent_task_id)
                .bind(Option::<&str>::None)
                .bind(Option::<&str>::None)
                .bind(&item.title)
                .bind(item.description.as_deref())
                .bind("sub_task")
                .bind("todo")
                .bind(0_i64)
                .bind(start_order + offset as i64)
                .bind(Option::<&str>::None)
                .bind(Option::<&str>::None)
                .bind(metadata_json.as_deref())
                .bind(Option::<&str>::None)
                .bind(&now)
                .bind(&now)
                .execute(&mut *transaction)
                .await?;
            // Subtasks inherit the parent's immutable governance provenance.
            // The replacement link records the adaptive split without making
            // the child a new source of authority.
            sqlx::query(
                "INSERT INTO project_task_governance
                 (task_id, project_id, charter_revision_id, baseline_id,
                  baseline_revision_id, plan_item_id, milestone_id,
                  document_revisions_json, capability_class, risk_class,
                  runnable, replacement_of_task_id, provenance_json,
                  version, created_at, updated_at)
                 SELECT ?, project_id, charter_revision_id, baseline_id,
                        baseline_revision_id, plan_item_id, milestone_id,
                        document_revisions_json, capability_class, risk_class,
                        runnable, ?, provenance_json,
                        1, ?, ?
                 FROM project_task_governance
                 WHERE task_id = ?",
            )
            .bind(&task_id)
            .bind(&parent_task_id)
            .bind(&now)
            .bind(&now)
            .bind(&parent_task_id)
            .execute(&mut *transaction)
            .await?;
            task_ids.push(task_id);
        }
        transaction.commit().await?;

        let mut tasks = Vec::with_capacity(task_ids.len());
        for task_id in task_ids {
            let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
            self.publish(ForgeEvent {
                event_type: "task.created".to_owned(),
                entity_id: task.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskCreated {
                    project_id: task.project_id.clone(),
                    title: task.title.clone(),
                },
            });
            tasks.push(task);
        }

        Ok(tasks)
    }
}
