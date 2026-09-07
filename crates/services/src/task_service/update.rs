use super::*;
use api_types::UpdateTaskRequest;
use db::UpdateTask;

impl TaskService {
    pub async fn update_task(
        &self,
        task_id: impl Into<String>,
        request: UpdateTaskRequest,
    ) -> Result<Task> {
        let task_id = task_id.into();
        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;

        let UpdateTaskRequest {
            title,
            description,
            priority,
            merge_config,
            plan,
            task_state_config,
            review_requirement_ids,
            parent_task_id,
            version,
        } = request;
        let merge_config = serialize_config(merge_config)?;
        let requested_state_config = serialize_config(task_state_config)?;
        let task_state_config = if let Some(requirement_ids) = review_requirement_ids.as_deref() {
            let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
            self.validate_task_review_requirement_ids(&project, requirement_ids)
                .await?;
            super::proposal::replace_review_requirement_ids(
                requested_state_config.or_else(|| task.task_state_config.clone()),
                requirement_ids,
            )?
        } else {
            requested_state_config
        };

        if review_requirement_ids.is_none() {
            if let Some(requirement_ids) =
                review_requirement_ids_from_config(task_state_config.as_deref())?
            {
                let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
                self.validate_task_review_requirement_ids(&project, &requirement_ids)
                    .await?;
            }
        }

        let previous_requirement_ids =
            review_requirement_ids_from_config(task.task_state_config.as_deref())?
                .unwrap_or_default();
        let next_requirement_ids = if task_state_config.is_some() {
            review_requirement_ids_from_config(task_state_config.as_deref())?.unwrap_or_default()
        } else {
            previous_requirement_ids.clone()
        };
        if task.task_type == "discovery" && !next_requirement_ids.is_empty() {
            return Err(ServiceError::invalid_operation(
                "discovery Tasks cannot own Charter review requirements; describe the research deliverable in Task acceptance and record it in the worklog or Task evidence",
            ));
        }
        let review_scope_changed = previous_requirement_ids != next_requirement_ids;

        let mut updated = TaskRepo::update(
            &*self.db,
            UpdateTask {
                id: task_id,
                expected_version: version,
                title,
                description: description.map(Some),
                priority,
                merge_config: merge_config.map(Some),
                plan: plan.map(Some),
                error_annotation: None,
                blocked_json: None,
                failed_json: None,
                task_state_config: task_state_config.map(Some),
                parent_task_id,
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        if review_scope_changed && updated.review_passed_at.is_some() {
            updated = TaskRepo::set_review_passed_at(&*self.db, &updated.id, None, &now_rfc3339())
                .await?;
        }
        Ok(updated)
    }
}

pub(super) fn review_requirement_ids_from_config(
    config: Option<&str>,
) -> Result<Option<Vec<String>>> {
    let Some(config) = config else {
        return Ok(None);
    };
    let config: Value = serde_json::from_str(config).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task_state_config: {error}"))
    })?;
    config
        .pointer("/review/requirement_ids")
        .map(|value| {
            serde_json::from_value::<Vec<String>>(value.clone()).map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "task_state_config.review.requirement_ids must be an array of strings: {error}"
                ))
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::review_requirement_ids_from_config;

    #[test]
    fn reads_explicit_empty_review_scope() {
        assert_eq!(
            review_requirement_ids_from_config(Some(r#"{"review":{"requirement_ids":[]}}"#))
                .expect("configuration parses"),
            Some(Vec::new())
        );
    }

    #[test]
    fn rejects_non_array_review_scope() {
        assert!(review_requirement_ids_from_config(Some(
            r#"{"review":{"requirement_ids":"all"}}"#
        ))
        .is_err());
    }
}
