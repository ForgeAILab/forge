use crate::auditor;
use chrono::{Duration as ChronoDuration, Utc};
use db::{
    new_uuid_v4, now_rfc3339, Agent, AgentRepo, AgentStatus, AssigneeKind, ClaimExecutionLease,
    CreateExecution, CreateReview, Execution, ExecutionAdmission, ExecutionLeaseDisposition,
    ExecutionLeaseMutation, ExecutionRepo, ExecutionStatus, ExecutionTerminalOutcome, Project,
    ProjectRepo, RenewExecutionLease, RepoRepo, Review, ReviewConformanceRepo, ReviewRepo,
    ReviewStatus, SqliteDb, Task, TaskRepo, TaskRoleAssignment, TaskRoleAssignmentRepo,
    TerminalizeExecution, WorkspaceRepo,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use executors::{
    resolve_config_value, AdapterExecutor, AdapterRegistry, ExecutionContext, ExecutionOutcome,
    ExecutionOverrides, LogEntry, LogKind, LogStream, LogWriter, TaskExecutor,
};
use serde_json::{json, Value};
use std::{future::Future, path::PathBuf, process::ExitStatus, sync::Arc};
use thiserror::Error;
use tokio::{process::Command, sync::oneshot, task::JoinHandle};
use uuid::Uuid;

const MAX_LOG_BYTES: u64 = 10 * 1024 * 1024;
const MAX_DIFF_BYTES: usize = 64 * 1024;
const STDERR_TAIL_BYTES: usize = 4096;
const RESUME_THREAD_ID_CONFIG_KEY: &str = "resume_thread_id";

pub struct ReviewRunner {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    executor: Arc<dyn TaskExecutor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewOutcome {
    Passed,
    PassedCiOnly,
    AwaitingHuman,
    AuditorFailed {
        reason: String,
    },
    CiFailed {
        failing_steps: Vec<StepResult>,
    },
    MergeConflict {
        conflict_paths: Vec<PathBuf>,
        conflict_summary: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepResult {
    pub index: usize,
    pub command: String,
    pub exit_code: i32,
    pub stderr_tail: String,
    pub output_tail: String,
    pub started_at: String,
    pub finished_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewRequest {
    pub task_id: Uuid,
    pub executor_execution_id: Uuid,
    pub workspace_path: PathBuf,
    pub ci_steps: Vec<String>,
    pub logs_path: String,
    pub auditor_agent_id: Option<String>,
    pub review_prompt: Option<String>,
    pub executor_thread_id: Option<String>,
    pub requires_user_approval: bool,
}

#[derive(Debug, Error)]
pub enum ReviewError {
    #[error("review assessment unavailable: {reason}")]
    Conformance {
        execution_id: String,
        reason: String,
    },

    #[error(transparent)]
    Db(#[from] db::DbError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    #[error(transparent)]
    Executor(#[from] executors::ExecutorError),

    #[error(transparent)]
    Git(#[from] git::GitError),

    #[error("executor execution not found: {0}")]
    ExecutorExecutionNotFound(Uuid),

    #[error("executor execution has no workspace: {0}")]
    ExecutorExecutionMissingWorkspace(Uuid),

    #[error("review execution lease was lost: {execution_id}")]
    ExecutionLeaseLost { execution_id: String },

    #[error("review execution hard deadline reached: {execution_id}")]
    ExecutionHardDeadline { execution_id: String },

    #[error("review execution lease could not be claimed: {execution_id}")]
    ExecutionLeaseUnavailable { execution_id: String },
}

impl ReviewRunner {
    pub fn new(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        adapter_registry: Arc<AdapterRegistry>,
    ) -> Self {
        Self {
            db,
            event_bus,
            executor: Arc::new(AdapterExecutor::new(adapter_registry)),
        }
    }

    /// Return a runner that dispatches through `executor` instead of the raw
    /// CLI adapter registry.
    ///
    /// The embedded runtime is not an adapter: it is routed separately from
    /// the CLI adapters, so an `AdapterExecutor` answers an embedded reviewer
    /// with "No adapter registered for executor type: embedded". The review
    /// path therefore has to share the same routed executor the Task path
    /// uses, which only the composition root can build.
    #[must_use]
    pub fn with_task_executor(&self, executor: Arc<dyn TaskExecutor>) -> Self {
        Self {
            db: Arc::clone(&self.db),
            event_bus: Arc::clone(&self.event_bus),
            executor,
        }
    }

    #[cfg(test)]
    #[allow(dead_code)] // pre-existing warning, out of scope for this change
    fn new_for_tests(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        executor: Arc<dyn TaskExecutor>,
    ) -> Self {
        Self {
            db,
            event_bus,
            executor,
        }
    }

    pub async fn run(&self, req: ReviewRequest) -> Result<(Review, ReviewOutcome), ReviewError> {
        let task_id = req.task_id.to_string();
        let executor_execution_id = req.executor_execution_id.to_string();
        let executor_execution = ExecutionRepo::get_by_id(&*self.db, &executor_execution_id)
            .await?
            .ok_or(ReviewError::ExecutorExecutionNotFound(
                req.executor_execution_id,
            ))?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let candidate_belongs_to_task = if executor_execution.task_id == task_id {
            true
        } else {
            TaskRepo::get_by_id(&*self.db, &executor_execution.task_id, false)
                .await?
                .is_some_and(|candidate_task| {
                    candidate_task.parent_task_id.as_deref() == Some(task_id.as_str())
                })
        };
        if !candidate_belongs_to_task {
            return Err(ReviewError::Db(db::DbError::Check(
                "review candidate execution belongs to another Task or coordination child"
                    .to_owned(),
            )));
        }
        let workspace_id = executor_execution.workspace_id.clone().ok_or(
            ReviewError::ExecutorExecutionMissingWorkspace(req.executor_execution_id),
        )?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or(db::DbError::NotFound)?;
        // A configured workflow role is authoritative.  Built-in `no-review`
        // and `human-required` workflows deliberately have no reviewer role;
        // their rerun still executes bounded CI as a server-owned check, but
        // must not accidentally borrow a stale Agent assignment.
        let effective_review_role = effective_review_role(&task, &project);
        let reviewer_assignment = if effective_review_role.as_deref() == Some("reviewer") {
            TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, &task_id, "reviewer").await?
        } else {
            None
        };
        let reviewer_agent = self
            .load_assigned_agent(reviewer_assignment.as_ref())
            .await?;
        if req.auditor_agent_id.is_some() && reviewer_agent.is_none() {
            return Err(ReviewError::Db(db::DbError::VersionConflict));
        }
        let auditor_agent = match req.auditor_agent_id.as_deref() {
            Some(agent_id) => self.load_auditor_agent(agent_id).await?,
            None => None,
        };
        let latest_review = ReviewRepo::list_by_task(&*self.db, &task_id)
            .await?
            .into_iter()
            .max_by_key(|review| (review.attempt_number, review.id.clone()));
        // Review rows point at the candidate execution being reviewed.  Each
        // reviewer/auditor child carries that same candidate as its durable
        // parent, allowing the admission transaction to reject a stale
        // rerun without relying on a racy MAX(attempt_number) preflight.
        let candidate_execution_id = executor_execution_id.clone();
        let reviewer_admission = review_execution_admission(
            &task,
            &project,
            reviewer_assignment.as_ref(),
            reviewer_agent.as_ref(),
            latest_review.as_ref(),
            &candidate_execution_id,
        );
        let ci_only_review = task.review_passed_at.is_some() && req.auditor_agent_id.is_none();
        let state_config = read_review_state_config(task.task_state_config.as_deref())?;
        let review_source = self
            .db
            .review_source(&task_id, Some(&executor_execution_id))
            .await?;
        let ci_steps = if crate::contract::task_scope_is_read_only(&review_source) {
            Vec::new()
        } else {
            read_ci_steps(&state_config)?
        };
        let review_prompt = read_review_prompt(&state_config);

        let (mut review, reviewer_execution) = self
            .create_reviewer_attempt(
                &task_id,
                &candidate_execution_id,
                workspace_id.clone(),
                &req,
                reviewer_agent.as_ref(),
                reviewer_admission,
            )
            .await?;
        let reviewer_owner = match reviewer_execution.lease_owner.clone() {
            Some(owner) => owner,
            None => {
                let error = ReviewError::ExecutionLeaseUnavailable {
                    execution_id: reviewer_execution.id.clone(),
                };
                cancel_review_if_unchanged(&self.db, &review, &error.to_string()).await;
                return Err(error);
            }
        };

        let reviewer_lease = match ReviewExecutionLease::start(
            Arc::clone(&self.db),
            reviewer_execution.clone(),
            reviewer_owner.clone(),
        ) {
            Ok(lease) => lease,
            Err(error) => {
                cancel_review_if_unchanged(&self.db, &review, &error.to_string()).await;
                return Err(error);
            }
        };
        let reviewer_steps = if ci_steps.is_empty() {
            reviewer_lease
                .run(async {
                    Ok::<_, ReviewError>((
                        ReviewStatus::Passed,
                        ReviewOutcome::Passed,
                        Vec::new(),
                        None,
                    ))
                })
                .await
                .and_then(|result| result)
        } else {
            reviewer_lease
                .run(self.run_steps(&req, &reviewer_execution, &ci_steps))
                .await
                .and_then(|result| result)
        };
        let (mut status, mut outcome, step_results, mut failed_step_index) = match reviewer_steps {
            Ok(result) => {
                let terminalized = match terminalize_review_execution(
                    &self.db,
                    &reviewer_execution,
                    &reviewer_owner,
                    ExecutionStatus::Completed,
                    Some(format!("review:{}", review.id)),
                    None,
                    ReviewTerminalPolicy::default(),
                )
                .await
                {
                    Ok(terminalized) => terminalized,
                    Err(error) => {
                        cancel_review_if_unchanged(&self.db, &review, &error.to_string()).await;
                        return Err(error);
                    }
                };
                if !terminalized {
                    let error = ReviewError::ExecutionLeaseLost {
                        execution_id: reviewer_execution.id.clone(),
                    };
                    cancel_review_if_unchanged(&self.db, &review, &error.to_string()).await;
                    return Err(error);
                }
                result
            }
            Err(error) => {
                let terminalized = terminalize_review_execution(
                    &self.db,
                    &reviewer_execution,
                    &reviewer_owner,
                    ExecutionStatus::Failed,
                    None,
                    Some(error.to_string()),
                    review_terminal_policy(&error),
                )
                .await
                .unwrap_or_else(|terminalize_error| {
                    tracing::warn!(
                        review_id = %review.id,
                        %terminalize_error,
                        "failed to terminalize reviewer execution after execution failure"
                    );
                    false
                });
                let cleanup_now = now_rfc3339();
                let cleanup = update_review_with_authority(
                    &self.db,
                    &task,
                    &project,
                    &review,
                    ReviewStatus::Failed,
                    json!({"error": error.to_string()}).to_string(),
                    Some(cleanup_now.clone()),
                    &cleanup_now,
                    if task.review_passed_at.is_some() {
                        Some(None)
                    } else {
                        None
                    },
                )
                .await;
                if let Err(cleanup_error) = cleanup {
                    tracing::warn!(
                        review_id = %review.id,
                        %cleanup_error,
                        "failed to settle Review after reviewer execution failure"
                    );
                    cancel_review_if_unchanged(&self.db, &review, &cleanup_error.to_string()).await;
                }
                if !terminalized {
                    let error = ReviewError::ExecutionLeaseLost {
                        execution_id: reviewer_execution.id.clone(),
                    };
                    cancel_review_if_unchanged(&self.db, &review, &error.to_string()).await;
                    return Err(error);
                }
                return Err(error);
            }
        };

        let mut auditor_details = None;
        if status == ReviewStatus::Passed && ci_only_review {
            outcome = ReviewOutcome::PassedCiOnly;
            auditor_details = Some(AuditorDetails::pass_ci_only());
        } else if status == ReviewStatus::Passed {
            if req.auditor_agent_id.is_some() {
                let now = now_rfc3339();
                review = match update_review_with_authority(
                    &self.db,
                    &task,
                    &project,
                    &review,
                    ReviewStatus::Running,
                    json!({"ci_steps": step_results_value(&step_results)}).to_string(),
                    None,
                    &now,
                    None,
                )
                .await
                {
                    Ok(review) => review,
                    Err(error) => {
                        cancel_review_if_unchanged(&self.db, &review, &error.to_string()).await;
                        return Err(error);
                    }
                };
            }
            let audit = self
                .run_auditor(
                    &req,
                    &executor_execution,
                    &task,
                    &project,
                    &review,
                    reviewer_assignment.as_ref(),
                    auditor_agent.as_ref(),
                    workspace_id,
                    review_prompt.as_deref(),
                )
                .await;
            let audit = match audit {
                Ok(result) => result,
                Err(error) => {
                    let conformance = if let ReviewError::Conformance { execution_id, .. } = &error
                    {
                        match db::ReviewConformanceRepo::review_conformance(&*self.db, execution_id)
                            .await
                        {
                            Ok(conformance) => conformance,
                            Err(conformance_error) => {
                                cancel_review_if_unchanged(
                                    &self.db,
                                    &review,
                                    &conformance_error.to_string(),
                                )
                                .await;
                                return Err(ReviewError::Db(conformance_error));
                            }
                        }
                    } else {
                        None
                    }
                    .unwrap_or_else(|| api_types::ReviewConformance {
                        status: api_types::ConformanceStatus::Unverified,
                        reason: Some(error.to_string()),
                        ..Default::default()
                    });
                    let details = json!({"ci_steps": step_results_value(&step_results), "conformance": conformance});
                    let now = now_rfc3339();
                    if let Err(update_error) = update_review_with_authority(
                        &self.db,
                        &task,
                        &project,
                        &review,
                        ReviewStatus::Failed,
                        details.to_string(),
                        Some(now.clone()),
                        &now,
                        if task.review_passed_at.is_some() {
                            Some(None)
                        } else {
                            None
                        },
                    )
                    .await
                    {
                        cancel_review_if_unchanged(&self.db, &review, &update_error.to_string())
                            .await;
                    }
                    return Err(error);
                }
            };
            if let Some(result) = audit {
                status = result.status;
                outcome = result.outcome;
                failed_step_index = None;
                auditor_details = Some(result.details);
            }
        }

        if status == ReviewStatus::Passed && req.requires_user_approval {
            status = ReviewStatus::AwaitingHuman;
            outcome = ReviewOutcome::AwaitingHuman;
        }

        let finished_at = now_rfc3339();
        let step_results_json = match review_details_json(&step_results, auditor_details.as_ref()) {
            Ok(details) => details,
            Err(error) => {
                cancel_review_if_unchanged(&self.db, &review, &error.to_string()).await;
                return Err(error.into());
            }
        };
        let review_finished_at =
            (status != ReviewStatus::AwaitingHuman).then_some(finished_at.clone());
        let task_projection = match &status {
            ReviewStatus::Passed if !ci_only_review => Some(Some(finished_at.clone())),
            ReviewStatus::Failed if task.review_passed_at.is_some() => Some(None),
            _ => None,
        };
        let review = match update_review_with_authority(
            &self.db,
            &task,
            &project,
            &review,
            status,
            step_results_json,
            review_finished_at,
            &finished_at,
            task_projection,
        )
        .await
        {
            Ok(review) => review,
            Err(error) => {
                cancel_review_if_unchanged(&self.db, &review, &error.to_string()).await;
                return Err(error);
            }
        };

        self.publish_review_event(&task_id, &review, outcome.clone(), failed_step_index);

        Ok((review, outcome))
    }

    async fn create_reviewer_attempt(
        &self,
        task_id: &str,
        candidate_execution_id: &str,
        workspace_id: String,
        req: &ReviewRequest,
        reviewer_agent: Option<&Agent>,
        admission: ExecutionAdmission,
    ) -> Result<(Review, Execution), ReviewError> {
        let execution_id = new_uuid_v4().to_string();
        let lease_claim = ReviewExecutionLease::new_claim(&execution_id);
        let now = lease_claim.claim.now.clone();
        let execution = CreateExecution {
            id: execution_id,
            task_id: task_id.to_owned(),
            agent_id: reviewer_agent.map(|agent| agent.id.clone()),
            role: "reviewer".to_string(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: Some(candidate_execution_id.to_owned()),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: Some(req.logs_path.clone()),
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now,
        };
        let review_input = CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            // The Review is bound to the candidate, not to the synthetic
            // reviewer execution. The child execution's parent is validated
            // against this value by the same transaction.
            execution_id: candidate_execution_id.to_owned(),
            // The repository allocates the next attempt while holding the
            // writer lock; this placeholder is intentionally ignored.
            attempt_number: 0,
            status: ReviewStatus::Running,
            step_results_json: "[]".to_owned(),
            started_at: lease_claim.claim.now.clone(),
            created_at: lease_claim.claim.now.clone(),
            updated_at: lease_claim.claim.now.clone(),
        };
        let (review, execution) = ReviewRepo::create_attempt_with_execution_and_lease(
            &*self.db,
            review_input,
            execution,
            lease_claim.claim,
            Some(admission),
        )
        .await
        .map_err(ReviewError::from)?;
        Ok((review, execution))
    }

    #[cfg(test)]
    async fn create_reviewer_execution(
        &self,
        task_id: &str,
        candidate_execution_id: &str,
        workspace_id: String,
        req: &ReviewRequest,
    ) -> Result<(Execution, String), ReviewError> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let assignment =
            TaskRoleAssignmentRepo::get_by_task_and_role(&*self.db, task_id, "reviewer").await?;
        let agent = self.load_assigned_agent(assignment.as_ref()).await?;
        let latest_review = ReviewRepo::list_by_task(&*self.db, task_id)
            .await?
            .into_iter()
            .max_by_key(|review| (review.attempt_number, review.id.clone()));
        let admission = review_execution_admission(
            &task,
            &project,
            assignment.as_ref(),
            agent.as_ref(),
            latest_review.as_ref(),
            candidate_execution_id,
        );
        let (_, execution) = self
            .create_reviewer_attempt(
                task_id,
                candidate_execution_id,
                workspace_id,
                req,
                agent.as_ref(),
                admission,
            )
            .await?;
        let owner = execution.lease_owner.clone().ok_or_else(|| {
            ReviewError::ExecutionLeaseUnavailable {
                execution_id: execution.id.clone(),
            }
        })?;
        Ok((execution, owner))
    }

    async fn run_steps(
        &self,
        req: &ReviewRequest,
        reviewer_execution: &Execution,
        ci_steps: &[String],
    ) -> Result<(ReviewStatus, ReviewOutcome, Vec<StepResult>, Option<usize>), ReviewError> {
        let mut writer =
            LogWriter::new(&req.logs_path, reviewer_execution.id.clone(), MAX_LOG_BYTES);
        let mut step_results = Vec::new();

        for (index, step) in ci_steps.iter().enumerate() {
            let started_at = now_rfc3339();
            let output = Command::new("bash")
                .arg("-lc")
                .arg(step)
                .current_dir(&req.workspace_path)
                .output()
                .await?;
            let finished_at = now_rfc3339();

            let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            let combined_output = combined_output(&stdout, &stderr);
            let exit_code = exit_code(output.status);
            let result = StepResult {
                index,
                command: step.clone(),
                exit_code,
                stderr_tail: tail_bytes(&stderr, STDERR_TAIL_BYTES),
                output_tail: tail_bytes(&combined_output, STDERR_TAIL_BYTES),
                started_at,
                finished_at,
            };

            writer
                .write(
                    LogKind::ShellCommand,
                    LogStream::Main,
                    serde_json::json!({
                        "index": index,
                        "command": step,
                        "exit_code": exit_code,
                        "stdout": stdout,
                        "stderr": stderr,
                        "output": combined_output,
                    }),
                )
                .await?;

            step_results.push(result.clone());
            if exit_code != 0 {
                let failing_steps = vec![result];
                return Ok((
                    ReviewStatus::Failed,
                    ReviewOutcome::CiFailed { failing_steps },
                    step_results,
                    Some(index),
                ));
            }
        }

        Ok((
            ReviewStatus::Passed,
            ReviewOutcome::Passed,
            step_results,
            None,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_auditor(
        &self,
        req: &ReviewRequest,
        executor_execution: &Execution,
        task: &Task,
        project: &Project,
        review: &Review,
        reviewer_assignment: Option<&TaskRoleAssignment>,
        auditor_agent: Option<&Agent>,
        workspace_id: String,
        review_prompt: Option<&str>,
    ) -> Result<Option<AuditorRunResult>, ReviewError> {
        if req.auditor_agent_id.is_none() {
            return Ok(None);
        }

        let task_id = req.task_id.to_string();
        let workspace = WorkspaceRepo::get_by_id(&*self.db, &workspace_id)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .filter(|repo| repo.project_id == task.project_id)
            .ok_or(db::DbError::NotFound)?;
        let Some(auditor_agent) = auditor_agent else {
            return Ok(Some(AuditorRunResult::failed("auditor_agent_unavailable")));
        };

        let diff_text = read_git_diff(&req.workspace_path, &repo.default_branch).await?;
        let mut prompt = auditor::render_auditor_prompt(
            &task.title,
            task.description.as_deref(),
            &diff_text,
            review_prompt,
        );
        let auditor_execution_id = new_uuid_v4();
        let auditor_before_sha = git::get_current_sha(&req.workspace_path).await?;
        let auditor_logs_path = auditor_logs_path(&req.logs_path, &auditor_execution_id);
        let executor_type = executor_type_for_execution(&self.db, executor_execution).await?;
        let extra_config = auditor_resume_thread_extra_config(
            executor_execution,
            executor_type.as_deref(),
            auditor_agent,
        );
        let snapshot = build_auditor_config_snapshot(auditor_agent, extra_config).await?;
        let lease_claim = ReviewExecutionLease::new_claim(&auditor_execution_id.to_string());
        let now = lease_claim.claim.now.clone();
        let admission = review_execution_admission(
            task,
            project,
            reviewer_assignment,
            Some(auditor_agent),
            Some(review),
            &review.execution_id,
        );
        let auditor_execution = ExecutionRepo::create_with_lease_and_admission(
            &*self.db,
            CreateExecution {
                id: auditor_execution_id.to_string(),
                task_id: task_id.clone(),
                agent_id: Some(auditor_agent.id.clone()),
                role: "auditor".to_string(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                // Keep the auditor bound to the same candidate as the
                // reviewer/Review row. The admission transaction checks this
                // lineage together with the Task/Project/assignment snapshot.
                parent_execution_id: Some(review.execution_id.clone()),
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: Some(auditor_logs_path.clone()),
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: Some(snapshot.clone()),
                workspace_id: Some(workspace_id),
                created_at: now.clone(),
                updated_at: now,
            },
            lease_claim.claim,
            Some(admission),
        )
        .await?;

        prompt = match crate::contract::prepare_prompt(
            &self.db,
            &auditor_execution.id,
            &task_id,
            &req.workspace_path,
            true,
            auditor_agent.executor_type == "shell",
            prompt,
        )
        .await
        {
            Ok(prompt) => prompt,
            Err(reason) => {
                terminalize_review_execution(
                    &self.db,
                    &auditor_execution,
                    &lease_claim.owner,
                    ExecutionStatus::Failed,
                    None,
                    Some(reason.clone()),
                    ReviewTerminalPolicy::default(),
                )
                .await?;
                return Err(ReviewError::Conformance {
                    execution_id: auditor_execution.id.clone(),
                    reason,
                });
            }
        };

        let auditor_owner = lease_claim.owner;
        let auditor_lease = ReviewExecutionLease::start(
            Arc::clone(&self.db),
            auditor_execution.clone(),
            auditor_owner.clone(),
        )?;
        let (execution_result, lease_error) = match auditor_lease
            .run(self.executor.execute(ExecutionContext {
                task_id,
                execution_id: auditor_execution.id.clone(),
                worktree_path: req.workspace_path.display().to_string(),
                description: prompt,
                agent_config: serde_json::from_str(&snapshot)?,
                logs_path: auditor_logs_path.clone(),
                heartbeat_interval_seconds: heartbeat_interval(auditor_agent),
                max_turns: None,
                log_sender: None,
            }))
            .await
        {
            Ok(result) => (result, None),
            Err(error) => (
                Err(executors::ExecutorError::Other(error.to_string())),
                Some(error),
            ),
        };
        let restore_result = git::restore_worktree(&req.workspace_path, &auditor_before_sha)
            .await
            .map_err(|error| {
                executors::ExecutorError::Other(format!(
                    "failed to restore auditor worktree state: {error}"
                ))
            });
        let execution_result = match (execution_result, restore_result) {
            (_, Err(error)) => Err(error),
            (Ok(mut result), Ok(())) => {
                result.after_sha = Some(auditor_before_sha);
                Ok(result)
            }
            (Err(error), Ok(())) => Err(error),
        };

        if let Some(error) = lease_error {
            let terminalized = terminalize_review_execution(
                &self.db,
                &auditor_execution,
                &auditor_owner,
                ExecutionStatus::Failed,
                None,
                Some(error.to_string()),
                review_terminal_policy(&error),
            )
            .await?;
            if !terminalized {
                return Err(ReviewError::ExecutionLeaseLost {
                    execution_id: auditor_execution.id.clone(),
                });
            }
            return Err(error);
        }

        let result = match execution_result {
            Ok(result) => result,
            Err(error) => {
                let terminalized = terminalize_review_execution(
                    &self.db,
                    &auditor_execution,
                    &auditor_owner,
                    ExecutionStatus::Failed,
                    None,
                    Some(error.to_string()),
                    ReviewTerminalPolicy::default(),
                )
                .await?;
                if !terminalized {
                    return Err(ReviewError::ExecutionLeaseLost {
                        execution_id: auditor_execution.id.clone(),
                    });
                }
                return Ok(Some(AuditorRunResult::failed("auditor_execution_failed")));
            }
        };

        let execution_status = match result.status {
            ExecutionOutcome::Completed => ExecutionStatus::Completed,
            ExecutionOutcome::Failed => ExecutionStatus::Failed,
            ExecutionOutcome::Cancelled => ExecutionStatus::Cancelled,
        };
        let terminalized = terminalize_review_execution_with_result(
            &self.db,
            &auditor_execution,
            &auditor_owner,
            ReviewTerminalUpdate {
                status: execution_status,
                summary: result.summary,
                after_sha: result.after_sha,
                error: result.error.clone(),
                agent_session_id: result.agent_session_id,
                policy: ReviewTerminalPolicy::default(),
            },
        )
        .await?;
        if !terminalized {
            return Err(ReviewError::ExecutionLeaseLost {
                execution_id: auditor_execution.id.clone(),
            });
        }

        if result.status != ExecutionOutcome::Completed {
            return Ok(Some(AuditorRunResult::failed(
                result
                    .error
                    .as_deref()
                    .unwrap_or("auditor_execution_failed"),
            )));
        }

        let final_message = last_assistant_message(&auditor_logs_path).await?;
        let conformance = crate::contract::evaluate(
            &self.db,
            &auditor_execution.id,
            &req.workspace_path,
            &final_message,
        )
        .await
        .map_err(|reason| ReviewError::Conformance {
            execution_id: auditor_execution.id.clone(),
            reason,
        })?;
        if conformance.status == api_types::ConformanceStatus::Unverified {
            return Err(ReviewError::Conformance {
                execution_id: auditor_execution.id.clone(),
                reason: conformance
                    .reason
                    .unwrap_or_else(|| "unverified review".into()),
            });
        }
        let mut result = if conformance.status == api_types::ConformanceStatus::Passed {
            AuditorRunResult {
                status: ReviewStatus::Passed,
                outcome: ReviewOutcome::Passed,
                details: AuditorDetails::passed(),
            }
        } else {
            AuditorRunResult::failed(
                conformance
                    .reason
                    .clone()
                    .unwrap_or_else(|| "review conformance failed".into()),
            )
        };
        result.details.conformance = Some(conformance);
        Ok(Some(result))
    }

    async fn load_auditor_agent(
        &self,
        auditor_agent_id: &str,
    ) -> Result<Option<Agent>, ReviewError> {
        let Some(agent) = AgentRepo::get_by_id(&*self.db, auditor_agent_id).await? else {
            return Ok(None);
        };
        if !matches!(agent.status, AgentStatus::Idle | AgentStatus::Busy) {
            return Ok(None);
        }
        Ok(Some(agent))
    }

    async fn load_assigned_agent(
        &self,
        assignment: Option<&TaskRoleAssignment>,
    ) -> Result<Option<Agent>, ReviewError> {
        let Some(assignment) = assignment else {
            return Ok(None);
        };
        if assignment.assignee_type != Some(AssigneeKind::Agent) {
            return Ok(None);
        }
        let agent_id = assignment.assignee_id.as_deref().ok_or_else(|| {
            ReviewError::Db(db::DbError::Check(
                "reviewer assignment has no agent identity".to_owned(),
            ))
        })?;
        self.load_auditor_agent(agent_id)
            .await?
            .ok_or_else(|| {
                ReviewError::Db(db::DbError::Check(
                    "assigned reviewer agent is unavailable".to_owned(),
                ))
            })
            .map(Some)
    }

    fn publish_review_event(
        &self,
        task_id: &str,
        review: &Review,
        outcome: ReviewOutcome,
        failed_step_index: Option<usize>,
    ) {
        let (event_type, entity_id, context) = match outcome {
            ReviewOutcome::Passed | ReviewOutcome::PassedCiOnly => (
                "review.passed",
                review.id.clone(),
                EventContext::ReviewPassed {
                    task_id: task_id.to_owned(),
                    review_id: review.id.clone(),
                    attempt_number: review.attempt_number,
                },
            ),
            ReviewOutcome::AwaitingHuman => (
                "task.awaiting_human",
                task_id.to_owned(),
                EventContext::TaskAwaitingHuman {
                    task_id: task_id.to_owned(),
                    role: "reviewer".to_owned(),
                    assignee_id: "human".to_owned(),
                    state: "review".to_owned(),
                },
            ),
            ReviewOutcome::AuditorFailed { .. }
            | ReviewOutcome::CiFailed { .. }
            | ReviewOutcome::MergeConflict { .. } => (
                "review.failed",
                review.id.clone(),
                EventContext::ReviewFailed {
                    task_id: task_id.to_owned(),
                    review_id: review.id.clone(),
                    attempt_number: review.attempt_number,
                    failed_step_index: failed_step_index.unwrap_or(0),
                },
            ),
        };

        self.event_bus.publish(ForgeEvent {
            event_type: event_type.to_owned(),
            entity_id,
            timestamp: event_timestamp(),
            context,
        });
    }
}

fn review_execution_admission(
    task: &Task,
    project: &Project,
    assignment: Option<&TaskRoleAssignment>,
    agent: Option<&Agent>,
    latest_review: Option<&Review>,
    candidate_execution_id: &str,
) -> ExecutionAdmission {
    let inherited_workflow = task.parent_task_id.is_some()
        && matches!(
            task.status.as_str(),
            "todo" | "in_progress" | "done" | "cancelled"
        );
    let expected_effective_role = effective_review_role(task, project);
    ExecutionAdmission {
        expected_project_version: Some(project.version),
        expected_task_version: task.version,
        expected_task_status: task.status.clone(),
        expected_effective_role,
        expected_agent_version: agent.map(|agent| agent.version),
        expected_agent_max_concurrent_tasks: agent.map(|agent| agent.max_concurrent_tasks),
        // This is always the candidate that the new reviewer/auditor child
        // will parent to. A rerun may use a fresh candidate while the latest
        // Review snapshot still names an older one; that prior candidate is
        // carried separately below.
        expected_reviewer_parent_execution_id: Some(candidate_execution_id.to_owned()),
        expected_latest_review_candidate_execution_id: latest_review
            .map(|review| review.execution_id.clone()),
        expected_reviewer_id: latest_review.map(|review| review.id.clone()),
        expected_reviewer_attempt_number: latest_review.map(|review| review.attempt_number),
        expected_reviewer_status: latest_review.map(|review| review.status.to_string()),
        expected_reviewer_updated_at: latest_review.map(|review| review.updated_at.clone()),
        expected_reviewer_execution_id: latest_review
            .and_then(|review| review.reviewer_execution_id.clone()),
        expected_auditor_execution_id: latest_review
            .and_then(|review| review.auditor_execution_id.clone()),
        expected_assignment_id: assignment.map(|assignment| assignment.id.clone()),
        expected_assignment_updated_at: assignment.map(|assignment| assignment.updated_at.clone()),
        expected_workflow_definition: (!inherited_workflow)
            .then(|| project.workflow_definition.clone()),
    }
}

/// Resolve the effective role for a Task's current workflow state without
/// depending on the services crate (which would create a dependency cycle).
/// The database admission layer repeats this calculation under its writer
/// lock; this snapshot only selects the expected value for that CAS.
fn effective_review_role(task: &Task, project: &Project) -> Option<String> {
    if task.parent_task_id.is_some()
        && matches!(
            task.status.as_str(),
            "todo" | "in_progress" | "done" | "cancelled"
        )
    {
        return match task.status.as_str() {
            "in_progress" => Some("coder".to_owned()),
            _ => None,
        };
    }

    let raw = project.workflow_definition.trim();
    if raw.is_empty() || raw == "{}" {
        return match task.status.as_str() {
            "planning" => Some("planner".to_owned()),
            "in_progress" | "merge_failed" => Some("coder".to_owned()),
            "review" => Some("reviewer".to_owned()),
            _ => None,
        };
    }
    let Ok(workflow) = serde_json::from_str::<api_types::WorkflowDefinition>(raw) else {
        // The database admission will fail closed for malformed workflow
        // definitions.  Returning no role here avoids manufacturing an Agent
        // assignment during the preflight phase.
        return None;
    };
    workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
        .and_then(|state| {
            state.role.clone().or_else(|| {
                (state.kind == api_types::StateKind::Active).then_some("assignee".to_owned())
            })
        })
}

fn review_workflow_definition<'a>(task: &Task, project: &'a Project) -> Option<&'a str> {
    let inherited_workflow = task.parent_task_id.is_some()
        && matches!(
            task.status.as_str(),
            "todo" | "in_progress" | "done" | "cancelled"
        );
    (!inherited_workflow).then_some(project.workflow_definition.as_str())
}

#[allow(clippy::too_many_arguments)]
async fn update_review_with_authority(
    db: &SqliteDb,
    task: &Task,
    project: &Project,
    review: &Review,
    status: ReviewStatus,
    details: String,
    finished_at: Option<String>,
    updated_at: &str,
    task_projection: Option<Option<String>>,
) -> Result<Review, ReviewError> {
    let review = ReviewRepo::update_status_with_review_authority_and_task_projection(
        db,
        &review.id,
        status,
        details,
        finished_at,
        updated_at,
        task.version,
        &task.status,
        Some(project.version),
        review_workflow_definition(task, project),
        review.status.clone(),
        &review.updated_at,
        &review.execution_id,
        task_projection,
    )
    .await?;
    Ok(review)
}

/// Once the reviewer execution/lease has been reserved, every later failure
/// must revoke the still-running Review unless another writer has already
/// changed that exact Review row.  This is deliberately a Review-only CAS:
/// authority invalidation must not guess at or overwrite the Task projection.
async fn cancel_review_if_unchanged(db: &SqliteDb, review: &Review, reason: &str) {
    let now = now_rfc3339();
    match ReviewRepo::cancel_if_unchanged(
        db,
        &review.id,
        review.status.clone(),
        &review.updated_at,
        json!({"error": reason}).to_string(),
        &now,
        &now,
    )
    .await
    {
        Ok(Some(_)) => {}
        Ok(None) => {
            tracing::debug!(
                review_id = %review.id,
                "Review compensation skipped because its snapshot is stale"
            );
        }
        Err(error) => {
            tracing::warn!(
                review_id = %review.id,
                %error,
                "failed to cancel Review after post-reservation failure"
            );
        }
    }
}

const REVIEW_LEASE_SECONDS: i64 = 30;
const REVIEW_HARD_DEADLINE_SECONDS: i64 = 30 * 60;
const REVIEW_HEARTBEAT_SECONDS: u64 = 10;

/// Review and auditor executions are ordinary running executions.  Keep an
/// authenticated owner lease alive independently of CI/model output so a
/// quiet provider/tool call is not mistaken for a dead reviewer.
#[derive(Debug, Clone, Copy)]
enum ReviewLeaseSignal {
    OwnerLost,
    HardDeadline,
}

struct ReviewLeaseClaim {
    owner: String,
    claim: ClaimExecutionLease,
}

struct ReviewExecutionLease {
    execution_id: String,
    owner: String,
    stop_tx: Option<oneshot::Sender<()>>,
    heartbeat: JoinHandle<()>,
    signal_rx: oneshot::Receiver<ReviewLeaseSignal>,
}

impl ReviewExecutionLease {
    fn new_claim(execution_id: &str) -> ReviewLeaseClaim {
        let owner = format!("review-owner:{}", new_uuid_v4());
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let lease_expires_at = (now + ChronoDuration::seconds(REVIEW_LEASE_SECONDS)).to_rfc3339();
        let hard_deadline_at =
            (now + ChronoDuration::seconds(REVIEW_HARD_DEADLINE_SECONDS)).to_rfc3339();
        ReviewLeaseClaim {
            owner: owner.clone(),
            claim: ClaimExecutionLease {
                execution_id: execution_id.to_owned(),
                expected_version: 1,
                owner,
                lease_expires_at,
                hard_deadline_at,
                now: now_text,
            },
        }
    }

    fn start(db: Arc<SqliteDb>, claimed: Execution, owner: String) -> Result<Self, ReviewError> {
        if claimed.status != ExecutionStatus::Running
            || claimed.lease_owner.as_deref() != Some(owner.as_str())
        {
            return Err(ReviewError::ExecutionLeaseUnavailable {
                execution_id: claimed.id,
            });
        }

        let (stop_tx, mut stop_rx) = oneshot::channel();
        let (signal_tx, signal_rx) = oneshot::channel();
        let db_for_heartbeat = Arc::clone(&db);
        let execution_id = claimed.id.clone();
        let execution_id_for_heartbeat = execution_id.clone();
        let owner_for_heartbeat = owner.clone();
        let heartbeat = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(REVIEW_HEARTBEAT_SECONDS));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    _ = interval.tick() => {
                        let now = Utc::now();
                        let now_text = now.to_rfc3339();
                        let proposed_expiry = now + ChronoDuration::seconds(REVIEW_LEASE_SECONDS);
                        let lease_expires_at = claimed
                            .hard_deadline_at
                            .as_deref()
                            .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                            .map(|hard_deadline| {
                                std::cmp::min(
                                    proposed_expiry,
                                    hard_deadline.with_timezone(&Utc),
                                )
                                .to_rfc3339()
                            })
                            .unwrap_or_else(|| proposed_expiry.to_rfc3339());

                        // The heartbeat loop owns the latest execution
                        // version by re-reading after each CAS.  This keeps
                        // terminalization after a long future from using a
                        // stale version after a renewal.
                        let current = match ExecutionRepo::get_by_id(&*db_for_heartbeat, &execution_id_for_heartbeat).await {
                            Ok(Some(current)) => current,
                            Ok(None) => {
                                let _ = signal_tx.send(ReviewLeaseSignal::OwnerLost);
                                break;
                            }
                            Err(error) => {
                                tracing::debug!(
                                    execution_id = %execution_id_for_heartbeat,
                                    %error,
                                    "review execution lease read failed; expiry monitor remains authoritative"
                                );
                                continue;
                            }
                        };
                        if current.status != ExecutionStatus::Running
                            || current.lease_owner.as_deref() != Some(owner_for_heartbeat.as_str())
                        {
                            let _ = signal_tx.send(ReviewLeaseSignal::OwnerLost);
                            break;
                        }
                        match ExecutionRepo::renew_lease(
                            &*db_for_heartbeat,
                            RenewExecutionLease {
                                execution_id: execution_id_for_heartbeat.clone(),
                                expected_version: current.execution_version,
                                owner: owner_for_heartbeat.clone(),
                                lease_expires_at,
                                now: now_text,
                            },
                        ).await {
                            Ok(ExecutionLeaseMutation::Updated(_)) => {}
                            Ok(ExecutionLeaseMutation::Concurrent { current: Some(current) })
                                if current.status == ExecutionStatus::Running
                                    && current.lease_owner.as_deref() == Some(owner_for_heartbeat.as_str()) => {}
                            Ok(ExecutionLeaseMutation::Concurrent { .. }) => {
                                let _ = signal_tx.send(ReviewLeaseSignal::OwnerLost);
                                break;
                            }
                            Ok(ExecutionLeaseMutation::HardDeadline { .. }) => {
                                let _ = signal_tx.send(ReviewLeaseSignal::HardDeadline);
                                break;
                            }
                            Err(error) => {
                                tracing::debug!(
                                    execution_id = %execution_id_for_heartbeat,
                                    %error,
                                    "review execution lease renewal failed; expiry monitor remains authoritative"
                                );
                                continue;
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            execution_id,
            owner,
            stop_tx: Some(stop_tx),
            heartbeat,
            signal_rx,
        })
    }

    async fn run<F, T, E>(self, future: F) -> Result<Result<T, E>, ReviewError>
    where
        F: Future<Output = Result<T, E>>,
    {
        let Self {
            execution_id,
            owner: _owner,
            mut stop_tx,
            heartbeat,
            mut signal_rx,
        } = self;
        tokio::pin!(future);
        let result = tokio::select! {
            result = &mut future => Ok(result),
            signal = &mut signal_rx => Err(match signal {
                Ok(ReviewLeaseSignal::OwnerLost) | Err(_) => {
                    ReviewError::ExecutionLeaseLost { execution_id }
                }
                Ok(ReviewLeaseSignal::HardDeadline) => {
                    ReviewError::ExecutionHardDeadline { execution_id }
                }
            }),
        };
        if let Some(stop_tx) = stop_tx.take() {
            let _ = stop_tx.send(());
        }
        let _ = heartbeat.await;
        result
    }
}

async fn terminalize_review_execution(
    db: &SqliteDb,
    execution: &Execution,
    owner: &str,
    status: ExecutionStatus,
    summary: Option<String>,
    error: Option<String>,
    policy: ReviewTerminalPolicy,
) -> Result<bool, ReviewError> {
    terminalize_review_execution_with_result(
        db,
        execution,
        owner,
        ReviewTerminalUpdate {
            status,
            summary,
            after_sha: None,
            error,
            agent_session_id: None,
            policy,
        },
    )
    .await
}

#[derive(Debug, Clone, Default)]
struct ReviewTerminalPolicy {
    stop_reason: Option<db::StopReason>,
    stopped_by: Option<String>,
    resume_policy: Option<db::ResumePolicy>,
}

fn review_terminal_policy(error: &ReviewError) -> ReviewTerminalPolicy {
    if matches!(error, ReviewError::ExecutionHardDeadline { .. }) {
        ReviewTerminalPolicy {
            stop_reason: Some(db::StopReason::AgentTimeout),
            stopped_by: Some("system:heartbeat_monitor".to_owned()),
            resume_policy: Some(db::ResumePolicy::Manual),
        }
    } else {
        ReviewTerminalPolicy::default()
    }
}

#[derive(Debug, Clone)]
struct ReviewTerminalUpdate {
    status: ExecutionStatus,
    summary: Option<String>,
    after_sha: Option<String>,
    error: Option<String>,
    agent_session_id: Option<String>,
    policy: ReviewTerminalPolicy,
}

async fn terminalize_review_execution_with_result(
    db: &SqliteDb,
    execution: &Execution,
    owner: &str,
    update: ReviewTerminalUpdate,
) -> Result<bool, ReviewError> {
    let mut candidate = execution.clone();
    for _ in 0..3 {
        let updated_at = now_rfc3339();
        let outcome = ExecutionRepo::terminalize(
            db,
            TerminalizeExecution {
                execution_id: candidate.id.clone(),
                expected_version: candidate.execution_version,
                lease_owner: Some(owner.to_owned()),
                status: update.status.clone(),
                stop_reason: update.policy.stop_reason.clone().map(Some),
                stopped_by: update.policy.stopped_by.clone().map(Some),
                stopped_at: Some(Some(updated_at.clone())),
                resume_policy: update.policy.resume_policy.clone().map(Some),
                agent_session_id: Some(update.agent_session_id.clone()),
                agent_message_id: None,
                last_activity_at: None,
                last_progress_at: None,
                summary: Some(update.summary.clone()),
                logs_path: None,
                before_sha: None,
                after_sha: Some(update.after_sha.clone()),
                error: Some(update.error.clone()),
                executor_config_snapshot_json: None,
                updated_at,
                actor_type: "system".to_owned(),
                actor_id: Some("review".to_owned()),
                correlation_id: Some(candidate.id.clone()),
                causation_id: None,
                causation_depth: 0,
                lease_disposition: ExecutionLeaseDisposition::Revoke,
            },
        )
        .await?;
        match outcome {
            ExecutionTerminalOutcome::Committed { .. } => return Ok(true),
            ExecutionTerminalOutcome::Concurrent {
                current: Some(current),
            } if current.status == ExecutionStatus::Running
                && current.lease_owner.as_deref() == Some(owner) =>
            {
                candidate = current;
            }
            ExecutionTerminalOutcome::Concurrent { .. } => return Ok(false),
        }
    }
    Ok(false)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuditorDetails {
    conformance: Option<api_types::ReviewConformance>,
    verdict: &'static str,
    reason: Option<String>,
}

impl AuditorDetails {
    fn passed() -> Self {
        Self {
            conformance: None,
            verdict: "pass",
            reason: None,
        }
    }

    fn pass_ci_only() -> Self {
        Self {
            conformance: None,
            verdict: "pass_ci_only",
            reason: Some("CI-only re-review".to_owned()),
        }
    }

    fn failed(reason: impl Into<String>) -> Self {
        Self {
            conformance: None,
            verdict: "fail",
            reason: Some(reason.into()),
        }
    }

    fn to_json(&self) -> Value {
        match &self.reason {
            Some(reason) => json!({
                "verdict": self.verdict,
                "reason": reason,
            }),
            None => json!({
                "verdict": self.verdict,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuditorRunResult {
    status: ReviewStatus,
    outcome: ReviewOutcome,
    details: AuditorDetails,
}

impl AuditorRunResult {
    fn failed(reason: impl Into<String>) -> Self {
        let reason = reason.into();
        Self {
            status: ReviewStatus::Failed,
            outcome: ReviewOutcome::AuditorFailed {
                reason: reason.clone(),
            },
            details: AuditorDetails::failed(reason),
        }
    }
}

async fn read_git_diff(
    workspace_path: &std::path::Path,
    default_branch: &str,
) -> Result<String, ReviewError> {
    let branch_ref = format!("{default_branch}...HEAD");
    let output = Command::new("git")
        .arg("diff")
        .arg(branch_ref)
        .current_dir(workspace_path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await?;
    let stdout = if output.status.success() {
        output.stdout
    } else {
        Command::new("git")
            .arg("diff")
            .current_dir(workspace_path)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .await?
            .stdout
    };
    Ok(truncate_utf8_bytes(&stdout, MAX_DIFF_BYTES))
}

fn truncate_utf8_bytes(bytes: &[u8], max_bytes: usize) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.len() <= max_bytes {
        return text.into_owned();
    }

    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let mut truncated = text[..end].to_owned();
    truncated.push_str("[truncated]");
    truncated
}

async fn build_auditor_config_snapshot(
    agent: &Agent,
    extra_config: Option<Value>,
) -> Result<String, ReviewError> {
    let mut base_config = parse_json_value("agent config_json", &agent.config_json)?;
    apply_agent_fields_to_config(agent, &mut base_config)?;
    let capabilities = parse_json_value("agent capabilities_json", &agent.capabilities_json)?;
    let kind = agent
        .executor_type
        .parse()
        .map_err(executors::ExecutorError::Other)?;
    let execution_overrides = extra_config.unwrap_or_else(|| json!({}));
    let (merged_config, overrides_applied) =
        merge_config_layers(&base_config, &execution_overrides);
    let normalized_config =
        resolve_config_value(kind, &merged_config, &ExecutionOverrides::default())?;
    let overrides_applied = overrides_applied.retain_config_keys(&normalized_config);
    // The auditor execution is a Task execution like any other, so its
    // snapshot must carry the same identity fields the runtime resolves a
    // session from. A native backend reads the profile — and through it the
    // provider credential — from `profile_id`, and refuses a snapshot whose
    // claimed role does not match the execution's own role.
    let mut snapshot = json!({
        "agent_id": agent.id,
        "profile_id": agent.profile_id,
        "provider": agent.provider,
        "executor_type": agent.executor_type,
        "model": agent.model,
        "prompt_template": agent.prompt_template,
        "reasoning_effort": agent.reasoning_effort,
        "permission_policy": agent.permission_policy,
        "config": normalized_config,
        "capabilities": capabilities,
        "overrides_applied": overrides_applied.to_json(),
        "snapshotted_at": now_rfc3339(),
    });
    snapshot[executors::TASK_ROLE_CONFIG_KEY] = json!("reviewer");
    // An auditor reads the delivered worktree and never writes to it.
    executors::mark_worktree_read_only(&mut snapshot);
    serde_json::to_string(&snapshot).map_err(Into::into)
}

fn auditor_resume_thread_extra_config(
    executor_execution: &Execution,
    executor_type: Option<&str>,
    auditor_agent: &Agent,
) -> Option<Value> {
    let thread_id = executor_execution.agent_session_id.as_deref()?;
    if executor_type == Some("codex") && auditor_agent.executor_type == "codex" {
        Some(json!({ RESUME_THREAD_ID_CONFIG_KEY: thread_id }))
    } else {
        None
    }
}

async fn executor_type_for_execution(
    db: &SqliteDb,
    executor_execution: &Execution,
) -> Result<Option<String>, ReviewError> {
    if let Some(snapshot) = executor_execution
        .executor_config_snapshot_json
        .as_deref()
        .and_then(|snapshot| serde_json::from_str::<Value>(snapshot).ok())
    {
        if let Some(executor_type) = snapshot.get("executor_type").and_then(Value::as_str) {
            return Ok(Some(executor_type.to_owned()));
        }
    }

    let Some(agent_id) = executor_execution.agent_id.as_deref() else {
        return Ok(None);
    };
    let Some(agent) = AgentRepo::get_by_id(db, agent_id).await? else {
        return Ok(None);
    };
    Ok(Some(agent.executor_type))
}

fn apply_agent_fields_to_config(agent: &Agent, config: &mut Value) -> Result<(), ReviewError> {
    let Some(config_object) = config.as_object_mut() else {
        return Err(ReviewError::Executor(executors::ExecutorError::Other(
            "agent config_json must be a JSON object".to_owned(),
        )));
    };
    if let Some(model) = &agent.model {
        config_object.insert("model".to_owned(), Value::String(model.clone()));
    }
    if let Some(reasoning_effort) = &agent.reasoning_effort {
        config_object.insert(
            "model_reasoning_effort".to_owned(),
            Value::String(reasoning_effort.clone()),
        );
        config_object.insert("effort".to_owned(), Value::String(reasoning_effort.clone()));
    }
    if let Some(permission_policy) = &agent.permission_policy {
        config_object.insert(
            "permission_policy".to_owned(),
            Value::String(permission_policy.clone()),
        );
    }
    Ok(())
}

fn heartbeat_interval(agent: &Agent) -> u64 {
    u64::try_from(agent.heartbeat_interval_seconds)
        .ok()
        .filter(|value| *value > 0)
        .unwrap_or(30)
}

fn auditor_logs_path(reviewer_logs_path: &str, auditor_execution_id: &str) -> String {
    let path = std::path::Path::new(reviewer_logs_path);
    path.parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join(format!("{auditor_execution_id}.jsonl"))
        .display()
        .to_string()
}

async fn last_assistant_message(logs_path: &str) -> Result<String, ReviewError> {
    let contents = match tokio::fs::read_to_string(logs_path).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(String::new()),
        Err(error) => return Err(error.into()),
    };
    let mut message = String::new();
    let mut stdout = String::new();
    for line in contents.lines() {
        let Ok(entry) = serde_json::from_str::<LogEntry>(line) else {
            continue;
        };
        if entry.kind == LogKind::Assistant {
            let mut candidate = String::new();
            append_assistant_log_text(&entry.payload, &mut candidate);
            if !candidate.trim().is_empty() {
                message = candidate;
            }
        } else if entry.kind == LogKind::Stdout {
            append_stdout_log_text(&entry.payload, &mut stdout);
        } else if entry.kind == LogKind::SessionInfo
            && entry.payload.get("subtype").and_then(Value::as_str) == Some("success")
        {
            if let Some(result) = entry.payload.get("result").and_then(Value::as_str) {
                message = result.to_owned();
            }
        }
    }
    Ok(if message.is_empty() { stdout } else { message })
}

fn append_stdout_log_text(payload: &Value, message: &mut String) {
    for key in ["line", "text", "content"] {
        if let Some(text) = payload.get(key).and_then(Value::as_str) {
            message.push_str(text);
            message.push('\n');
            return;
        }
    }
}

fn append_assistant_log_text(payload: &Value, message: &mut String) {
    if let Some(text) = payload.get("text").and_then(Value::as_str) {
        message.push_str(text);
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
                message.push_str(text);
            }
        }
    }
}

fn combined_output(stdout: &str, stderr: &str) -> String {
    let mut output = String::with_capacity(stdout.len() + stderr.len());
    output.push_str(stdout);
    output.push_str(stderr);
    output
}

fn exit_code(status: ExitStatus) -> i32 {
    status.code().unwrap_or(1)
}

fn review_details_json(
    results: &[StepResult],
    auditor: Option<&AuditorDetails>,
) -> Result<String, serde_json::Error> {
    let ci_steps = step_results_value(results);
    match auditor {
        Some(auditor) => serde_json::to_string(&json!({
            "ci_steps": ci_steps,
            "auditor": auditor.to_json(),
            "conformance": auditor.conformance.clone().unwrap_or_default(),
        })),
        None => serde_json::to_string(&ci_steps),
    }
}

fn step_results_value(results: &[StepResult]) -> Value {
    Value::Array(
        results
            .iter()
            .map(|result| {
                json!({
                    "index": result.index,
                    "command": result.command,
                    "exit_code": result.exit_code,
                    "stderr_tail": result.stderr_tail,
                    "output_tail": result.output_tail,
                    "started_at": result.started_at,
                    "finished_at": result.finished_at,
                })
            })
            .collect(),
    )
}

fn read_review_state_config(task_state_config: Option<&str>) -> Result<Value, ReviewError> {
    let Some(raw_config) = task_state_config else {
        return Ok(json!({}));
    };
    if raw_config.trim().is_empty() {
        return Ok(json!({}));
    }

    let value: Value = serde_json::from_str(raw_config)?;
    let value = value.get("review").cloned().unwrap_or(value);
    if !value.is_object() {
        return Err(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "review configuration must be an object",
        ))
        .into());
    }
    Ok(value)
}

fn read_ci_steps(state_config: &Value) -> Result<Vec<String>, ReviewError> {
    let Some(value) = state_config.get("ci_steps") else {
        return Ok(Vec::new());
    };
    let Some(steps) = value.as_array() else {
        return Err(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "review ci_steps must be an array",
        ))
        .into());
    };
    steps
        .iter()
        .enumerate()
        .map(|(index, step)| {
            let command = step
                .as_str()
                .filter(|command| !command.trim().is_empty())
                .ok_or_else(|| {
                    serde_json::Error::io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("review ci_steps[{index}] must be a nonblank string"),
                    ))
                })?;
            Ok(command.to_owned())
        })
        .collect()
}

fn read_review_prompt(state_config: &Value) -> Option<String> {
    state_config
        .get("review_prompt")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn parse_json_value(field: &str, value: &str) -> Result<Value, ReviewError> {
    serde_json::from_str(value).map_err(|error| {
        serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid {field}: {error}"),
        ))
        .into()
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OverridesApplied {
    agent: Vec<String>,
    execution: Vec<String>,
}

impl OverridesApplied {
    fn to_json(&self) -> Value {
        json!({
            "agent": self.agent,
            "execution": self.execution,
        })
    }

    fn retain_config_keys(mut self, config: &Value) -> Self {
        let Some(config_object) = config.as_object() else {
            self.agent.clear();
            self.execution.clear();
            return self;
        };

        self.agent
            .retain(|key| config_object.contains_key(key.as_str()));
        self.execution
            .retain(|key| config_object.contains_key(key.as_str()));
        self
    }
}

fn merge_config_layers(agent: &Value, execution: &Value) -> (Value, OverridesApplied) {
    let mut merged = agent.clone();
    let mut overrides_applied = OverridesApplied {
        agent: object_keys(agent),
        execution: Vec::new(),
    };

    merge_override_layer(&mut merged, execution, &mut overrides_applied.execution);

    (merged, overrides_applied)
}

fn object_keys(value: &Value) -> Vec<String> {
    value
        .as_object()
        .map(|object| object.keys().cloned().collect())
        .unwrap_or_default()
}

fn merge_override_layer(merged: &mut Value, layer: &Value, applied_keys: &mut Vec<String>) {
    let Some(layer_object) = layer.as_object() else {
        return;
    };
    let Some(merged_object) = merged.as_object_mut() else {
        return;
    };
    for (key, value) in layer_object {
        merged_object.insert(key.clone(), value.clone());
        applied_keys.push(key.clone());
    }
}

fn tail_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }

    let mut start = value.len() - max_bytes;
    while !value.is_char_boundary(start) {
        start += 1;
    }
    value[start..].to_owned()
}

#[cfg(test)]
mod tests;
