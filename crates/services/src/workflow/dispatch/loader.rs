use std::sync::Arc;

use api_types::{StateKind, WorkflowDefinition};
use db::{
    ExecutionRepo, PageRequest, ReviewRepo, SortBy, SortOrder, TaskCommentRepo, TaskRepo,
    TransitionLogRepo, WorkspaceRepo,
};
use executors::{LogEntry, LogKind};
use serde_json::Value;

use crate::workflow::dispatch::{
    StepDiff, TaskDelivery, EXECUTION_POLICY_RESUME_LATEST_TARGET_ROLE_THREAD, STEP_DIFF_LIMIT,
};
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
    let mut continuation_of_execution_id = continuation_execution
        .as_ref()
        .map(|execution| execution.id.clone());
    let continuation_logs_path = continuation_execution
        .as_ref()
        .and_then(|execution| execution.logs_path.clone());
    let review_window = db.review_window(task_id).await?.reviews;
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
    let capability_class = db
        .task_governance_capability_class(&task.id)
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

    let step_diff = match task.parent_task_id.as_deref() {
        Some(root_id)
            if role == db::inherited_subtask::STEP_CHECK_ROLE
                && state_name == db::inherited_subtask::STEP_CHECK_STATE =>
        {
            load_step_diff(&db, router, &task, root_id).await?
        }
        _ => None,
    };

    let previous_coder = match review_window
        .iter()
        .max_by_key(|review| review.attempt_number)
    {
        Some(review) => ExecutionRepo::get_by_id(&*db, &review.execution_id)
            .await?
            .filter(|execution| {
                execution.task_id == task.id
                    && matches!(execution.role.as_str(), "coder" | "worker" | "executor")
            }),
        None => None,
    };
    let previous_coder = match previous_coder {
        Some(execution) => Some(execution),
        None => latest_terminal_execution_for_role(&db, task_id, role).await?,
    };
    let previous_coder_summary = previous_coder.and_then(|execution| execution.summary);
    let (mut budget_kind, gate_state) = repair_budget_source(workflow, state_name, &transition_log);
    if budget_kind == db::budget::Kind::Review {
        if let Some(review) = review_window.last() {
            if db::budget::receipt_recorded(
                db.pool(),
                task_id,
                db::budget::Kind::ReviewProgress.key(),
                &format!("progress:{}", review.id),
            )
            .await?
            {
                budget_kind = db::budget::Kind::ReviewProgress;
            }
        }
    }
    let budget_config = gate_state.map(|state| {
        crate::workflow::engine::effective_state_config(
            state,
            project.as_ref(),
            task.task_state_config.as_deref(),
        )
    });
    let repair_budget = Some(super::repair_brief::RepairBudget {
        name: budget_kind.key().to_owned(),
        spent: db::budget::spent(db.pool(), task_id, budget_kind.key()).await?,
        limit: i64::from(db::budget::limit(
            &task,
            budget_kind,
            budget_config.as_ref(),
            gate_state.and_then(|state| state.gate_config.as_ref()),
        )?),
    });

    let carried_repair = matches!(
        budget_kind,
        db::budget::Kind::Review | db::budget::Kind::ReviewProgress
    ) && workflow
        .states
        .iter()
        .any(|state| state.name == state_name && state.kind == StateKind::Active)
        && (task.parent_task_id.is_some() || sub_tasks.is_empty())
        && db::review_findings::has_carried_finding(&review_window);
    let reset_model_after_repair = continuation_execution
        .as_ref()
        .and_then(|execution| execution.executor_config_snapshot_json.as_deref())
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .is_some_and(|snapshot| snapshot["repair_model_switched"] == true);
    if carried_repair || reset_model_after_repair {
        continuation_of_execution_id = None;
    }
    let repair_attempts = if matches!(role, "coder" | "worker")
        && workflow
            .states
            .iter()
            .any(|state| state.name == state_name && state.kind == StateKind::Active)
        && !super::repair_brief::repair_rounds(&review_window).is_empty()
    {
        load_repair_attempts(&db, router, &task, &review_window).await?
    } else {
        Vec::new()
    };

    Ok(AgentDispatchContext {
        task,
        role: role.to_string(),
        state_name: state_name.to_string(),
        state_config,
        transition_log,
        comments,
        plan,
        prior_reviews,
        review_window: Some(review_window),
        previous_coder_summary,
        repair_attempts,
        carried_repair,
        reset_model_after_repair,
        repair_budget,
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
        step_diff,
    })
}

/// Evidence is read through the placement owner, including daemon workspaces.
/// A missing historical Git object leaves the memory useful, with an explicit
/// unavailable marker rather than invented file names.
async fn load_repair_attempts(
    db: &db::SqliteDb,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
    task: &db::Task,
    reviews: &[db::Review],
) -> Result<Vec<super::repair_brief::RepairAttempt>> {
    let settled = super::repair_brief::repair_rounds(reviews);
    let workspace =
        WorkspaceRepo::get_by_task_id(db, task.parent_task_id.as_deref().unwrap_or(&task.id))
            .await?;
    let owner = match workspace.as_ref() {
        Some(workspace) => router.resolve(db, workspace).await.ok(),
        None => None,
    };
    let mut memories = Vec::new();
    for (index, review) in settled
        .iter()
        .enumerate()
        .skip(settled.len().saturating_sub(3))
    {
        let execution = ExecutionRepo::get_by_id(db, &review.execution_id)
            .await?
            .filter(|execution| execution.task_id == task.id);
        let commit = |review: &db::Review| {
            serde_json::from_str::<Value>(&review.step_results_json)
                .ok()
                .and_then(|details| {
                    details["conformance"]["contract"]["commit_sha"]
                        .as_str()
                        .map(str::to_owned)
                })
        };
        let mut previous_commit = index
            .checked_sub(1)
            .and_then(|prior| commit(settled[prior]));
        if previous_commit.is_none() {
            if let Some(prior) = index.checked_sub(1) {
                previous_commit = ExecutionRepo::get_by_id(db, &settled[prior].execution_id)
                    .await?
                    .filter(|execution| execution.task_id == task.id)
                    .and_then(|execution| execution.after_sha);
            }
        }
        let from_sha = previous_commit.or_else(|| {
            execution
                .as_ref()
                .and_then(|execution| execution.before_sha.clone())
        });
        let to_sha = commit(review).or_else(|| {
            execution
                .as_ref()
                .and_then(|execution| execution.after_sha.clone())
        });
        let mut files = Vec::new();
        let mut files_unavailable = true;
        if let (Some(owner), Some(from), Some(to)) = (&owner, &from_sha, &to_sha) {
            match owner
                .backend
                .diff(
                    &owner.placement,
                    &crate::workspace_backend::DiffSpec {
                        base_ref: from.clone(),
                        head_ref: Some(to.clone()),
                        max_bytes: 1024,
                    },
                )
                .await
            {
                Ok(diff) => {
                    files = diff
                        .response
                        .files
                        .iter()
                        .take(20)
                        .map(|file| {
                            format!("{} (+{}, -{})", file.path, file.additions, file.deletions)
                        })
                        .collect();
                    if diff.response.files.len() > 20 {
                        files.push(format!("+{} more files", diff.response.files.len() - 20));
                    }
                    files_unavailable = false;
                }
                Err(error) => {
                    tracing::warn!(task_id = %task.id, %error, "repair attempt file history unavailable")
                }
            }
        }
        memories.push(super::repair_brief::RepairAttempt {
            attempt: review.attempt_number,
            from_sha,
            to_sha,
            files,
            files_unavailable,
            summary: execution.and_then(|execution| execution.summary),
        });
    }
    Ok(memories)
}

/// Follow the gate that actually sent this role back, including workflows
/// that renamed their review, integration and repair states.
fn repair_budget_source<'a>(
    workflow: &'a WorkflowDefinition,
    state_name: &str,
    transition_log: &[db::TransitionLog],
) -> (db::budget::Kind, Option<&'a api_types::StateDefinition>) {
    let entry = transition_log
        .iter()
        .rev()
        .find(|entry| entry.to_state == state_name);
    let source = entry.and_then(|entry| {
        workflow
            .states
            .iter()
            .find(|state| state.name == entry.from_state && state.kind == StateKind::Gate)
    });
    let current = workflow
        .states
        .iter()
        .find(|state| state.name == state_name);
    let is_merge = |state: &api_types::StateDefinition| {
        state
            .hooks
            .on_enter
            .iter()
            .any(|hook| hook.action == "run_merge")
    };
    let merge_repair = source.is_some_and(is_merge)
        || current.is_some_and(|state| {
            state.name == crate::workflow::default_states::MERGE_FAILED
                || state
                    .hooks
                    .before_exit
                    .iter()
                    .any(|hook| hook.action == "require_conflict_markers_resolved")
        });
    let kind = if entry.is_some_and(|entry| entry.bridge.is_repair_no_change()) {
        db::budget::Kind::RepairNoChange
    } else if entry.is_some_and(|entry| {
        entry.bridge.bridge_kind == Some(api_types::TransitionBridgeKind::ConflictHandoff)
    }) {
        db::budget::Kind::ConflictHandoff
    } else if merge_repair {
        db::budget::Kind::MergeFix
    } else {
        db::budget::Kind::Review
    };
    let gate = source
        .or_else(|| current.filter(|state| state.kind == StateKind::Gate))
        .or_else(|| {
            workflow.states.iter().find(|state| {
                if merge_repair {
                    is_merge(state)
                } else {
                    db::budget::is_review_state(state)
                }
            })
        });
    (kind, gate)
}

/// The diff of a finished step for its step check: the shared root workspace
/// (uncommitted leftovers included) against the commit the step started from.
///
/// That commit is the head recorded when the last run of an earlier sibling
/// ended (a step check always records it: it is a read-only run; a native
/// worker records it too), and the root workspace's branch point when no
/// earlier run recorded one. It is read through the workspace backend, so an
/// embedded and a daemon-owned workspace answer alike. A diff that cannot be
/// read never fails the dispatch: the prompt says so and the lead reads the
/// workspace.
async fn load_step_diff(
    db: &db::SqliteDb,
    router: &crate::workspace_backend::WorkspaceBackendRouter,
    task: &db::Task,
    root_id: &str,
) -> Result<Option<StepDiff>> {
    let Some(workspace) = WorkspaceRepo::get_by_task_id(db, root_id).await? else {
        return Ok(None);
    };
    let previous_step_head = db
        .previous_sibling_execution_head(root_id, &task.id)
        .await?;
    let since_previous_step = previous_step_head.is_some();
    let Some(base_sha) = previous_step_head.or_else(|| {
        workspace
            .before_sha
            .clone()
            .filter(|sha| !sha.trim().is_empty())
    }) else {
        return Ok(Some(StepDiff::Unavailable {
            reason: "no commit is recorded as the start of this step".to_owned(),
        }));
    };
    let loaded = async {
        let resolved = router.resolve(db, &workspace).await?;
        resolved
            .backend
            .diff(
                &resolved.placement,
                &crate::workspace_backend::DiffSpec {
                    base_ref: base_sha.clone(),
                    head_ref: None,
                    max_bytes: STEP_DIFF_LIMIT,
                },
            )
            .await
    }
    .await;
    Ok(Some(match loaded {
        Ok(diff) => StepDiff::Loaded {
            base_sha,
            since_previous_step,
            files_changed: diff.response.stats.files_changed,
            additions: diff.response.stats.total_additions,
            deletions: diff.response.stats.total_deletions,
            diff: diff.response.diff,
            truncated: diff.truncated,
        },
        Err(error) => {
            let reason = ServiceError::from(error).to_string();
            tracing::warn!(task_id = %task.id, %reason, "step diff could not be loaded");
            StepDiff::Unavailable {
                reason: reason.chars().take(300).collect(),
            }
        }
    }))
}

/// The delivery channel of the Agent assigned to `role`. An unassigned role
/// keeps the native contract; whoever picks it up is resolved at launch.
async fn role_delivery(db: &db::SqliteDb, task_id: &str, role: &str) -> Result<TaskDelivery> {
    let backend_kind = db.task_role_selected_backend_kind(task_id, role).await?;
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
    #[test]
    fn repair_budget_follows_renamed_source_gates_and_reminders() {
        let mut workflow = crate::workflow::default_workflow::default_workflow();
        for state in &mut workflow.states {
            state.name = match state.name.as_str() {
                "review" => "audit",
                "merging" => "integrate",
                "merge_failed" => "repair",
                name => name,
            }
            .to_owned();
        }
        let mut entry = db::TransitionLog {
            id: "entry".into(),
            task_id: "task".into(),
            from_state: "integrate".into(),
            to_state: "repair".into(),
            trigger_name: None,
            triggered_by: "system:workflow".into(),
            bridge: Default::default(),
            trigger_reason: "checks failed".into(),
            hook_results_json: None,
            rejection: true,
            created_at: "now".into(),
        };
        let entries = [entry.clone()];
        let (kind, gate) = repair_budget_source(&workflow, "repair", &entries);
        assert!(matches!(kind, db::budget::Kind::MergeFix));
        assert_eq!(gate.unwrap().name, "integrate");
        entry.from_state = "audit".into();
        entry.to_state = "in_progress".into();
        let entries = [entry.clone()];
        let (kind, gate) = repair_budget_source(&workflow, "in_progress", &entries);
        assert!(matches!(kind, db::budget::Kind::Review));
        assert_eq!(gate.unwrap().name, "audit");
        entry.bridge = api_types::TransitionBridge::recovery("repair_no_change", false);
        assert!(matches!(
            repair_budget_source(&workflow, "in_progress", &[entry.clone()]).0,
            db::budget::Kind::RepairNoChange
        ));
        entry.bridge =
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::ConflictHandoff);
        assert!(matches!(
            repair_budget_source(&workflow, "in_progress", &[entry]).0,
            db::budget::Kind::ConflictHandoff
        ));
    }
}
