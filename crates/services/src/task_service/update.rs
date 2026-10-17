use super::*;
use api_types::UpdateTaskRequest;
use db::UpdateTask;

impl TaskService {
    pub async fn update_task(
        &self,
        task_id: impl Into<String>,
        request: UpdateTaskRequest,
    ) -> Result<Task> {
        let task_id: String = task_id.into();
        if !db::task_writer::owns_task(&task_id)
            && (request.review_requirement_ids.is_some()
                || request.parent_task_id.is_some()
                || request.task_state_config.is_some())
        {
            return self
                .request_task_command(
                    &task_id,
                    "update_task",
                    serde_json::json!([task_id, request, request.parent_task_id.is_some()]),
                    false,
                )
                .await;
        }

        validate_required("task_id", &task_id)?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
        super::execution::ensure_plan_publication_transition_authority(&task, None)?;

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
        reject_retired_auditor_config(requested_state_config.as_deref())?;
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
            updated = TaskRepo::set_review_passed_at_cas(
                &*self.db,
                &updated.id,
                updated.version,
                None,
                &now_rfc3339(),
            )
            .await?;
        }
        // The owner removed the retired auditor key: its notice is over.
        if names_retired_auditor(task.task_state_config.as_deref())
            && !names_retired_auditor(updated.task_state_config.as_deref())
        {
            resolve_retired_auditor_notice(&self.db, &updated.id).await?;
        }
        Ok(updated)
    }
}

/// `review.auditor_agent_id` belonged to the inline review re-run, which ran a
/// second, "auditor" pass. There is one review path now and it has no
/// auditor: the review is done by the Agent assigned to the Task's `reviewer`
/// role. A Task configuration that names an auditor would be believed to
/// require a review that nothing runs, so it is not accepted.
pub(crate) const RETIRED_AUDITOR_CONFIG: &str =
    "review.auditor_agent_id is no longer supported: there is no separate auditor pass. \
     Assign the Agent to the Task's `reviewer` role and remove the key";

/// Whether a Task state configuration still names an auditor.
pub(crate) fn names_retired_auditor(config: Option<&str>) -> bool {
    config
        .and_then(|config| serde_json::from_str::<Value>(config).ok())
        .is_some_and(|config| {
            ["/review/auditor_agent_id", "/auditor_agent_id"]
                .into_iter()
                .any(|pointer| {
                    config
                        .pointer(pointer)
                        .is_some_and(|value| !value.is_null())
                })
        })
}

/// The dedupe key of the owner notice a Task carries while its configuration
/// still names an auditor.
pub(crate) fn retired_auditor_notice_key(task_id: &str) -> String {
    format!("retired-auditor:{task_id}")
}

/// State on each open Task that still names an auditor that it does, as an
/// owner notice in the Project's Attention feed (category `review_risk`,
/// which the Task and Project views already render), in status
/// `acknowledged`: listed, and never a wake of the Project Agent. It blocks
/// nothing and changes no Task. Operator status reports the same Tasks for the operator;
/// this is for the Task's owner. Run at start: such a configuration can only
/// predate v0.14 (new ones are refused), so one pass after an upgrade finds
/// them all. Notices of Tasks that no longer name one, or are closed, are
/// resolved. Returns how many notices were opened.
pub(crate) async fn state_retired_auditor_notices(db: &db::SqliteDb) -> Result<usize> {
    let named: Vec<String> = db.tasks_naming_retired_auditor().await?;
    let open: Vec<String> = db.open_retired_auditor_notice_keys().await?;
    let mut opened = 0;
    for task_id in &named {
        let key = retired_auditor_notice_key(task_id);
        if open.contains(&key) {
            continue;
        }
        let Some(task) = TaskRepo::get_by_id(db, task_id, false).await? else {
            continue;
        };
        // One short write transaction per Task.
        let mut tx = db::begin_immediate(db.pool()).await?;
        crate::placement::admission::record_wait_attention_in_tx(
            db,
            &mut tx,
            &task,
            "review_risk",
            &format!("This Task's configuration names a review auditor. {RETIRED_AUDITOR_CONFIG}"),
            &key,
        )
        .await?;
        // A notice, not an incident. An `open` item of this category is a
        // blocker the wake sweep gives to the Project Agent (one turn of its
        // blocker budget per Task, with the instruction to recover a Task
        // that needs no recovery). `acknowledged` items are listed to the
        // owner like open ones and are no wake source.
        db.acknowledge_retired_auditor_notice_in_tx(&mut tx, &key)
            .await?;
        tx.commit().await?;
        opened += 1;
    }
    for key in open {
        let still_named = key
            .strip_prefix("retired-auditor:")
            .is_some_and(|task_id| named.iter().any(|id| id == task_id));
        if !still_named {
            resolve_attention(db, &key).await?;
        }
    }
    Ok(opened)
}

async fn resolve_retired_auditor_notice(db: &db::SqliteDb, task_id: &str) -> Result<()> {
    resolve_attention(db, &retired_auditor_notice_key(task_id)).await
}

async fn resolve_attention(db: &db::SqliteDb, dedupe_key: &str) -> Result<()> {
    let now = now_rfc3339();
    db.resolve_retired_auditor_notice(&now, dedupe_key).await?;
    Ok(())
}

pub(super) fn reject_retired_auditor_config(config: Option<&str>) -> Result<()> {
    if let Some(raw) = config {
        let value: Value = serde_json::from_str(raw).map_err(|error| {
            ServiceError::invalid_operation(format!("task_state_config: {error}"))
        })?;
        api_types::validate_review_verify_config(&value)
            .map_err(ServiceError::invalid_operation)?;
        if let Some(states) = value.as_object() {
            for state in states.values() {
                api_types::validate_review_verify_config(state)
                    .map_err(ServiceError::invalid_operation)?;
            }
        }
    }
    if names_retired_auditor(config) {
        return Err(ServiceError::invalid_operation(format!(
            "task_state_config.{RETIRED_AUDITOR_CONFIG}"
        )));
    }
    Ok(())
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
    use super::{names_retired_auditor, reject_retired_auditor_config};

    #[test]
    fn a_configuration_that_names_an_auditor_is_refused_and_any_other_is_accepted() {
        for named in [
            r#"{"review":{"auditor_agent_id":"a"}}"#,
            r#"{"auditor_agent_id":"a"}"#,
            r#"{"review":{"ci_steps":["true"],"auditor_agent_id":"a"}}"#,
        ] {
            assert!(names_retired_auditor(Some(named)), "{named}");
            let refused = reject_retired_auditor_config(Some(named)).unwrap_err();
            assert!(
                matches!(&refused, crate::ServiceError::InvalidOperation { message }
                    if message.contains("review.auditor_agent_id is no longer supported")
                        && message.contains("`reviewer` role")),
                "{refused:?}"
            );
        }
        for accepted in [
            None,
            Some(r#"{"review":{"ci_steps":["true"]}}"#),
            Some(r#"{"review":{"auditor_agent_id":null}}"#),
            Some("{}"),
        ] {
            assert!(!names_retired_auditor(accepted), "{accepted:?}");
            reject_retired_auditor_config(accepted).expect("accepted");
        }
    }

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
