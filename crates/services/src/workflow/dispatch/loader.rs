use std::sync::Arc;

use api_types::{StateKind, WorkflowDefinition};
use db::{
    ExecutionRepo, PageRequest, ReviewRepo, SortBy, SortOrder, TaskCommentRepo, TaskRepo,
    TransitionLogRepo, WorkspaceRepo,
};
use executors::{LogEntry, LogKind};
use serde_json::Value;

use crate::workflow::dispatch::{TaskDelivery, EXECUTION_POLICY_RESUME_LATEST_TARGET_ROLE_THREAD};
use crate::{workflow::dispatch::AgentDispatchContext, Result, ServiceError};

const REVIEW_FEEDBACK_LIMIT: usize = 12_000;

pub struct DispatchContextParams<'a> {
    pub db: Arc<db::SqliteDb>,
    pub router: &'a crate::workspace_backend::WorkspaceBackendRouter,
    pub task_id: &'a str,
    pub role: &'a str,
    pub state_name: &'a str,
    pub state_config: Value,
    pub execution_policy: Option<&'a str>,
    pub workflow: &'a WorkflowDefinition,
}

pub async fn load_agent_dispatch_context(
    params: DispatchContextParams<'_>,
) -> Result<AgentDispatchContext> {
    let DispatchContextParams {
        db,
        router,
        task_id,
        role,
        state_name,
        state_config,
        execution_policy,
        workflow,
    } = params;
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
    .items
    .into_iter()
    .filter(|comment| !is_operator_note(comment))
    .collect::<Vec<_>>();
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
    let plan = if matches!(
        role,
        crate::workflow::default_roles::WORKER | crate::workflow::default_roles::CODER
    ) {
        let workspace_task_id = task.parent_task_id.as_deref().unwrap_or(task_id);
        match WorkspaceRepo::get_by_task_id(&*db, workspace_task_id).await? {
            Some(workspace) => {
                match crate::plan_artifact::read_plan_text_with_router(&db, router, &workspace.id)
                    .await
                {
                    Ok(plan) => plan.or_else(|| task.plan.clone()),
                    // Admission owns owner/protocol refusals. Its hard filters or
                    // workspace preconditions must report these structurally.
                    Err(error) if error.needs_placement_check() => task.plan.clone(),
                    Err(error) => {
                        return Err(
                            error.into_service_error("canonical plan artifact is unreadable")
                        )
                    }
                }
            }
            None => task.plan.clone(),
        }
    } else {
        task.plan.clone()
    };
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
    let delivery = role_delivery(&db, task_id, role).await?;
    let project = db::ProjectRepo::get_by_id(&*db, &task.project_id).await?;
    let review_ci_steps = crate::workflow::engine::review_ci_steps_for_task(
        workflow,
        project.as_ref(),
        task.task_state_config.as_deref(),
    );

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
        delivery,
        review_ci_steps,
    })
}

/// The delivery channel of the Agent assigned to `role`. An unassigned role
/// keeps the native contract; whoever picks it up is resolved at launch.
async fn role_delivery(db: &db::SqliteDb, task_id: &str, role: &str) -> Result<TaskDelivery> {
    let backend_kind = sqlx::query_scalar::<_, String>(
        "SELECT profile.backend_kind
         FROM task_role_assignment assignment
         JOIN agent_identity identity ON identity.id = assignment.assignee_id
         JOIN agent_profile profile ON profile.id = identity.selected_profile_id
         WHERE assignment.task_id = ? AND assignment.role_name = ?
           AND assignment.assignee_type = 'agent'
         LIMIT 1",
    )
    .bind(task_id)
    .bind(role)
    .fetch_optional(db.pool())
    .await?;
    Ok(backend_kind
        .as_deref()
        .map_or(TaskDelivery::NativeTools, TaskDelivery::for_backend_kind))
}

fn should_resume_latest_target_role_thread(execution_policy: Option<&str>) -> bool {
    execution_policy == Some(EXECUTION_POLICY_RESUME_LATEST_TARGET_ROLE_THREAD)
}

fn derive_last_manual_bounce_reason(
    transition_log: &[db::TransitionLog],
    state_name: &str,
    workflow: &WorkflowDefinition,
) -> Option<String> {
    // Only an actor's deliberate move out of a gate carries feedback. Forge's
    // own transitions (a gate skipped for an unassigned role, a cascade) are
    // bookkeeping, and quoting them as "sent back" misleads the agent.
    transition_log
        .iter()
        .rev()
        .find(|entry| {
            entry.to_state == state_name
                && !entry.rejection
                && !is_system_actor(&entry.triggered_by)
                && workflow
                    .states
                    .iter()
                    .any(|state| state.name == entry.from_state && state.kind == StateKind::Gate)
        })
        .map(|entry| entry.trigger_reason.clone())
}

fn is_system_actor(triggered_by: &str) -> bool {
    triggered_by == "system" || triggered_by.starts_with("system:")
}

async fn load_sub_tasks(db: &db::SqliteDb, parent_task_id: &str) -> Result<Vec<db::Task>> {
    crate::task_hierarchy::ordered_children(db, parent_task_id).await
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
    ExecutionRepo::latest_non_running_by_task_and_role(db, task_id, role)
        .await
        .map_err(Into::into)
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

    // `review.execution_id` names the execution that was *reviewed* — the
    // coder's — not the one that produced the verdict. Resolve the reviewer's
    // own execution, or the coder is handed its own log as "review feedback"
    // and can never see the finding it is being asked to fix.
    let reviewer_execution = reviewer_execution_for_review(db, review).await?;
    let logs_path = reviewer_execution
        .as_ref()
        .and_then(|execution| execution.logs_path.clone());
    let feedback = if review_has_auditor_feedback(&review.step_results_json) {
        match reviewer_execution.as_ref() {
            // The frozen assessment states each violated requirement and why,
            // which is what the coder has to act on; the reviewer's closing
            // message is the fallback when no assessment was persisted.
            Some(execution) => match conformance_feedback(db, &execution.id).await? {
                Some(feedback) => Some(feedback),
                None => reviewer_final_message(execution).await?,
            },
            None => None,
        }
    } else {
        None
    };

    Ok(LatestReviewContext {
        feedback,
        execution_id: reviewer_execution.map(|execution| execution.id),
        logs_path,
    })
}

/// The reviewer execution that produced one review attempt's verdict.
///
/// Current attempts carry explicit reviewer/auditor execution identities.
/// Historical direct rows are accepted only when `Review.execution_id` names
/// an actual reviewer/auditor execution. Candidate parentage and timestamps
/// are not sufficient to associate an execution with an attempt.
async fn reviewer_execution_for_review(
    db: &db::SqliteDb,
    review: &db::Review,
) -> Result<Option<db::Execution>> {
    if let Some((execution_id, expected_role)) = exact_review_execution_binding(review) {
        return exact_review_execution(db, &review.task_id, execution_id, expected_role).await;
    }

    // Historical rows may have stored a direct reviewer execution in the
    // candidate-shaped `execution_id` column. Preserve only that exact
    // identity, and reject ordinary candidate executions.
    let Some(execution) = ExecutionRepo::get_by_id(db, &review.execution_id).await? else {
        return Ok(None);
    };
    if execution.task_id == review.task_id
        && matches!(
            execution.role.as_str(),
            crate::workflow::default_roles::REVIEWER | "auditor"
        )
    {
        Ok(Some(execution))
    } else {
        Ok(None)
    }
}

fn exact_review_execution_binding(review: &db::Review) -> Option<(&str, &str)> {
    // An audited Review's actionable feedback is the auditor's verdict, so
    // prefer that exact execution when both role-specific bindings exist.
    if let Some(execution_id) = review.auditor_execution_id.as_deref() {
        return Some((execution_id, "auditor"));
    }
    review
        .reviewer_execution_id
        .as_deref()
        .map(|execution_id| (execution_id, crate::workflow::default_roles::REVIEWER))
}

async fn exact_review_execution(
    db: &db::SqliteDb,
    task_id: &str,
    execution_id: &str,
    expected_role: &str,
) -> Result<Option<db::Execution>> {
    let Some(execution) = ExecutionRepo::get_by_id(db, execution_id).await? else {
        return Ok(None);
    };
    if execution.task_id == task_id && execution.role == expected_role {
        Ok(Some(execution))
    } else {
        Ok(None)
    }
}

/// The coder-actionable feedback from the frozen conformance record: the
/// reviewer's reason followed by its Markdown review.
async fn conformance_feedback(db: &db::SqliteDb, execution_id: &str) -> Result<Option<String>> {
    let Some(conformance) = db::ReviewConformanceRepo::review_conformance(db, execution_id).await?
    else {
        return Ok(None);
    };
    let reason = conformance
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|reason| !reason.is_empty());
    let report = conformance
        .assessment
        .as_ref()
        .map(|assessment| assessment.report.trim())
        .filter(|report| !report.is_empty());
    let mut rendered = match (reason, report) {
        (Some(reason), Some(report)) => format!("{reason}\n\n{report}"),
        (Some(text), None) | (None, Some(text)) => text.to_owned(),
        (None, None) => return Ok(None),
    };
    let mut end = rendered.len().min(REVIEW_FEEDBACK_LIMIT);
    while !rendered.is_char_boundary(end) {
        end -= 1;
    }
    rendered.truncate(end);
    Ok(Some(rendered))
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

/// Forge's own notes on the Task record for the people operating it ("a
/// lifecycle hook was not run", "off-branch commits were kept under a ref").
/// They are not instructions and not something an agent can act on, so they
/// stay out of the prompt.
fn is_operator_note(comment: &db::TaskComment) -> bool {
    comment.author_type == db::CommentAuthorType::System
        && comment.idempotency_key.as_deref().is_some_and(|key| {
            key.starts_with(crate::lifecycle::HOOK_NOT_RUN_COMMENT_KEY)
                || key.starts_with(crate::workspace_manager::RESCUED_COMMENT_KEY)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{CreateProject, CreateTask, ProjectRepo, TaskRepo};

    #[test]
    fn operator_notes_stay_out_of_agent_prompts() {
        let comment = |author_type: db::CommentAuthorType, key: Option<&str>| db::TaskComment {
            id: "c".to_owned(),
            task_id: "task".to_owned(),
            author_type,
            author_id: None,
            author_name: "Forge".to_owned(),
            content: "note".to_owned(),
            execution_id: None,
            role: None,
            worklog_kind: None,
            idempotency_key: key.map(str::to_owned),
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
        };
        let system = || db::CommentAuthorType::System;
        let hook = format!(
            "{}task:before_review",
            crate::lifecycle::HOOK_NOT_RUN_COMMENT_KEY
        );
        let rescued = format!(
            "{}task:abc123",
            crate::workspace_manager::RESCUED_COMMENT_KEY
        );
        assert!(is_operator_note(&comment(system(), Some(&hook))));
        assert!(is_operator_note(&comment(system(), Some(&rescued))));
        // Everything else is still loaded: other Forge comments, and a
        // comment by anyone else whatever key it carries.
        assert!(!is_operator_note(&comment(system(), None)));
        assert!(!is_operator_note(&comment(
            system(),
            Some("review-result:1")
        )));
        assert!(!is_operator_note(&comment(
            db::CommentAuthorType::User,
            Some(&rescued)
        )));
    }

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

    #[test]
    fn manual_bounce_reason_ignores_system_gate_skips() {
        let workflow = crate::workflow::default_workflow::default_workflow();
        let transition = |triggered_by: &str, reason: &str| db::TransitionLog {
            id: db::new_uuid_v4(),
            task_id: "task".to_owned(),
            from_state: "planning".to_owned(),
            to_state: "in_progress".to_owned(),
            trigger_name: Some("accept".to_owned()),
            triggered_by: triggered_by.to_owned(),
            bridge: Default::default(),
            trigger_reason: reason.to_owned(),
            hook_results_json: None,
            rejection: false,
            created_at: "2026-09-25T20:46:22Z".to_owned(),
        };

        let skipped = [transition(
            "system:workflow",
            "gate skipped: no planner role assigned",
        )];
        assert_eq!(
            derive_last_manual_bounce_reason(&skipped, "in_progress", &workflow),
            None
        );

        let bounced = [
            transition("user:api", "add tests for the error path"),
            transition("system:workflow", "gate skipped: no planner role assigned"),
        ];
        assert_eq!(
            derive_last_manual_bounce_reason(&bounced, "in_progress", &workflow).as_deref(),
            Some("add tests for the error path")
        );
    }

    #[test]
    fn audited_review_feedback_prefers_exact_auditor_binding() {
        let review = db::Review {
            id: "review".to_owned(),
            task_id: "task".to_owned(),
            execution_id: "candidate".to_owned(),
            reviewer_execution_id: Some("reviewer".to_owned()),
            auditor_execution_id: Some("auditor".to_owned()),
            attempt_number: 1,
            status: db::ReviewStatus::Failed,
            step_results_json: "[]".to_owned(),
            started_at: "now".to_owned(),
            finished_at: None,
            created_at: "now".to_owned(),
            updated_at: "now".to_owned(),
        };

        assert_eq!(
            exact_review_execution_binding(&review),
            Some(("auditor", "auditor"))
        );
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
