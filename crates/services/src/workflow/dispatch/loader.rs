use std::sync::Arc;

use api_types::{StateKind, WorkflowDefinition};
use db::{
    ExecutionRepo, ExecutionStatus, PageRequest, ReviewRepo, SortBy, SortOrder, TaskCommentRepo,
    TaskRepo, TransitionLogRepo,
};
use executors::{LogEntry, LogKind};
use serde_json::Value;

use crate::workflow::dispatch::EXECUTION_POLICY_RESUME_LATEST_TARGET_ROLE_THREAD;
use crate::{workflow::dispatch::AgentDispatchContext, Result, ServiceError};

const REVIEW_FEEDBACK_LIMIT: usize = 12_000;

pub async fn load_agent_dispatch_context(
    db: Arc<db::SqliteDb>,
    task_id: &str,
    role: &str,
    state_name: &str,
    state_config: Value,
    execution_policy: Option<&str>,
    workflow: &WorkflowDefinition,
) -> Result<AgentDispatchContext> {
    let task = TaskRepo::get_by_id(&*db, task_id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task_id.to_string()))?;
    let transition_log = TransitionLogRepo::list_by_task(&*db, task_id).await?;
    let comments = TaskCommentRepo::list_comments(
        &*db,
        task_id,
        PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
        },
    )
    .await?
    .items;
    let prior_reviews = ReviewRepo::list_by_task(&*db, task_id).await?;
    let parent_task = match task.parent_task_id.as_deref() {
        Some(parent_task_id) => TaskRepo::get_by_id(&*db, parent_task_id, false).await?,
        None => None,
    };
    let sub_tasks = load_sub_tasks(&db, task_id).await?;
    let last_manual_bounce_reason =
        derive_last_manual_bounce_reason(&transition_log, state_name, workflow);
    let continuation_execution = if should_resume_latest_target_role_thread(execution_policy) {
        latest_terminal_execution_for_role(&db, task_id, role).await?
    } else {
        None
    };
    let continuation_of_execution_id = continuation_execution
        .as_ref()
        .map(|execution| execution.id.clone());
    let continuation_logs_path = continuation_execution
        .as_ref()
        .and_then(|execution| execution.logs_path.clone());
    let latest_review_context = latest_failed_review_context(db.as_ref(), &prior_reviews).await?;
    let plan = task.plan.clone();
    let capability_class = sqlx::query_scalar::<_, Option<String>>(
        "SELECT capability_class FROM project_task_governance WHERE task_id = ?",
    )
    .bind(&task.id)
    .fetch_optional(db.pool())
    .await?
    .flatten();
    let read_only_task = crate::execution_setup::classify_task_execution(
        &task.task_type,
        capability_class.as_deref(),
    )?
    .is_read_only();

    Ok(AgentDispatchContext {
        task,
        role: role.to_string(),
        state_name: state_name.to_string(),
        state_config,
        transition_log,
        comments,
        plan,
        prior_reviews,
        parent_task,
        sub_tasks,
        last_manual_bounce_reason,
        continuation_of_execution_id,
        continuation_logs_path,
        latest_review_feedback: latest_review_context.feedback,
        latest_review_execution_id: latest_review_context.execution_id,
        latest_review_logs_path: latest_review_context.logs_path,
        read_only_task,
    })
}

fn should_resume_latest_target_role_thread(execution_policy: Option<&str>) -> bool {
    execution_policy == Some(EXECUTION_POLICY_RESUME_LATEST_TARGET_ROLE_THREAD)
}

fn derive_last_manual_bounce_reason(
    transition_log: &[db::TransitionLog],
    state_name: &str,
    workflow: &WorkflowDefinition,
) -> Option<String> {
    transition_log
        .iter()
        .rev()
        .find(|entry| {
            entry.to_state == state_name
                && !entry.rejection
                && workflow
                    .states
                    .iter()
                    .any(|state| state.name == entry.from_state && state.kind == StateKind::Gate)
        })
        .map(|entry| entry.trigger_reason.clone())
}

async fn load_sub_tasks(db: &db::SqliteDb, parent_task_id: &str) -> Result<Vec<db::Task>> {
    Ok(TaskRepo::list_subtasks_ordered(db, parent_task_id).await?)
}

async fn latest_terminal_execution_for_role(
    db: &db::SqliteDb,
    task_id: &str,
    role: &str,
) -> Result<Option<db::Execution>> {
    if let Some(execution) = latest_terminal_execution_for_exact_role(db, task_id, role).await? {
        return Ok(Some(execution));
    }
    if role == crate::workflow::default_roles::CODER {
        return latest_terminal_execution_for_exact_role(db, task_id, "executor").await;
    }
    Ok(None)
}

async fn latest_terminal_execution_for_exact_role(
    db: &db::SqliteDb,
    task_id: &str,
    role: &str,
) -> Result<Option<db::Execution>> {
    let page = ExecutionRepo::list_by_task_and_role(
        db,
        task_id,
        role,
        PageRequest {
            cursor: None,
            limit: 1,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await?;

    Ok(page.items.into_iter().next().filter(|execution| {
        matches!(
            execution.status,
            ExecutionStatus::Completed | ExecutionStatus::Failed | ExecutionStatus::Cancelled
        )
    }))
}

#[derive(Debug, Default)]
struct LatestReviewContext {
    feedback: Option<String>,
    execution_id: Option<String>,
    logs_path: Option<String>,
}

async fn latest_failed_review_context(
    db: &db::SqliteDb,
    prior_reviews: &[db::Review],
) -> Result<LatestReviewContext> {
    let Some(review) = prior_reviews
        .iter()
        .filter(|review| review.status == db::ReviewStatus::Failed)
        .max_by_key(|review| review.attempt_number)
    else {
        return Ok(LatestReviewContext::default());
    };

    let execution = ExecutionRepo::get_by_id(db, &review.execution_id).await?;
    let logs_path = execution
        .as_ref()
        .and_then(|execution| execution.logs_path.clone());
    let feedback = if review_has_auditor_feedback(&review.step_results_json) {
        match execution.as_ref() {
            Some(execution) => reviewer_final_message(execution).await?,
            None => None,
        }
    } else {
        None
    };

    Ok(LatestReviewContext {
        feedback,
        execution_id: Some(review.execution_id.clone()),
        logs_path,
    })
}

fn review_has_auditor_feedback(step_results_json: &str) -> bool {
    serde_json::from_str::<Value>(step_results_json)
        .ok()
        .and_then(|value| value.get("auditor").cloned())
        .is_some_and(|auditor| !auditor.is_null())
}

async fn reviewer_final_message(execution: &db::Execution) -> Result<Option<String>> {
    let Some(logs_path) = execution.logs_path.as_deref() else {
        return Ok(execution
            .summary
            .as_deref()
            .map(str::trim)
            .filter(|summary| !summary.is_empty())
            .map(str::to_owned));
    };
    let contents = match tokio::fs::read_to_string(logs_path).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => {
            tracing::warn!(
                execution_id = %execution.id,
                logs_path,
                %error,
                "failed to read reviewer execution log"
            );
            String::new()
        }
    };

    let mut message = String::new();
    let mut stdout_lines = String::new();
    for line in contents.lines() {
        let Ok(entry) = serde_json::from_str::<LogEntry>(line) else {
            continue;
        };
        match entry.kind {
            LogKind::Assistant => {
                let mut candidate = String::new();
                append_log_text(&entry.payload, &mut candidate);
                if !candidate.trim().is_empty() {
                    message = candidate;
                }
            }
            LogKind::SessionInfo
                if entry.payload.get("subtype").and_then(Value::as_str) == Some("success") =>
            {
                if let Some(result) = entry.payload.get("result").and_then(Value::as_str) {
                    message = result.to_owned();
                }
            }
            LogKind::Stdout => {
                if let Some(line) = entry.payload.get("line").and_then(Value::as_str) {
                    stdout_lines.push_str(line);
                    stdout_lines.push('\n');
                }
            }
            _ => {}
        }
    }

    let feedback = if !message.trim().is_empty() {
        message
    } else if !stdout_lines.trim().is_empty() {
        stdout_lines
    } else {
        execution.summary.clone().unwrap_or_default()
    };
    let feedback = feedback.trim();
    if feedback.is_empty() {
        Ok(None)
    } else {
        Ok(Some(tail_chars(feedback, REVIEW_FEEDBACK_LIMIT)))
    }
}

fn append_log_text(payload: &Value, out: &mut String) {
    if let Some(text) = payload
        .get("text")
        .or_else(|| payload.get("content"))
        .and_then(Value::as_str)
    {
        out.push_str(text);
    }

    let Some(content) = payload
        .get("message")
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
    else {
        return;
    };

    for item in content {
        if item.get("type").and_then(Value::as_str) == Some("text") {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                out.push_str(text);
            }
        }
    }
}

fn tail_chars(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        return value.to_owned();
    }

    let mut start = value.len() - limit;
    while !value.is_char_boundary(start) {
        start += 1;
    }
    format!("[truncated]\n{}", &value[start..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{CreateProject, CreateTask, ProjectRepo, TaskRepo};

    #[test]
    fn new_execution_policy_does_not_resume_previous_execution() {
        assert!(!should_resume_latest_target_role_thread(Some(
            "new_execution"
        )));
    }

    #[test]
    fn resume_latest_target_role_thread_policy_resumes_previous_execution() {
        assert!(should_resume_latest_target_role_thread(Some(
            EXECUTION_POLICY_RESUME_LATEST_TARGET_ROLE_THREAD
        )));
    }

    #[tokio::test]
    async fn load_sub_tasks_loads_complete_task_rows() {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations run");
        let db = db::SqliteDb::new(pool);

        let now = db::now_rfc3339();
        let project = ProjectRepo::create(
            &db,
            CreateProject {
                id: db::new_uuid_v4(),
                name: "dispatch context subtasks".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");
        let parent = TaskRepo::create(
            &db,
            CreateTask {
                id: db::new_uuid_v4(),
                project_id: project.id.clone(),
                repo_id: None,
                parent_task_id: None,
                assignee_type: None,
                assignee_id: None,
                title: "parent".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "in_progress".to_owned(),
                is_automation: false,
                priority: 0,
                subtask_order: None,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("parent task creates");
        let child = TaskRepo::create(
            &db,
            CreateTask {
                id: db::new_uuid_v4(),
                project_id: project.id,
                repo_id: None,
                parent_task_id: Some(parent.id.clone()),
                assignee_type: None,
                assignee_id: None,
                title: "child".to_owned(),
                description: None,
                task_type: "sub_task".to_owned(),
                status: "todo".to_owned(),
                is_automation: false,
                priority: 0,
                subtask_order: Some(0),
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("child task creates");

        let loaded = load_sub_tasks(&db, &parent.id)
            .await
            .expect("subtasks load");

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, child.id);
        assert_eq!(loaded[0].task_type, "sub_task");
        assert_eq!(
            loaded[0].parent_task_id.as_deref(),
            Some(parent.id.as_str())
        );
    }
}
