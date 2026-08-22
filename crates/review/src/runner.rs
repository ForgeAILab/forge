use crate::auditor::{self, AuditorVerdict};
use chrono::{Duration as ChronoDuration, Utc};
use db::{
    new_uuid_v4, now_rfc3339, Agent, AgentRepo, AgentStatus, ClaimExecutionLease, CreateExecution,
    CreateReview, Execution, ExecutionLeaseDisposition, ExecutionLeaseMutation, ExecutionRepo,
    ExecutionStatus, ExecutionTerminalOutcome, RenewExecutionLease, RepoRepo, Review, ReviewRepo,
    ReviewStatus, SqliteDb, TaskRepo, TerminalizeExecution,
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
}

#[derive(Debug, Error)]
pub enum ReviewError {
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
        let attempt_number = ReviewRepo::next_attempt_number(&*self.db, &task_id).await?;
        let executor_execution = ExecutionRepo::get_by_id(&*self.db, &executor_execution_id)
            .await?
            .ok_or(ReviewError::ExecutorExecutionNotFound(
                req.executor_execution_id,
            ))?;
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let ci_only_review = task.review_passed_at.is_some();
        let state_config = read_review_state_config(task.task_state_config.as_deref())?;
        let ci_steps = read_ci_steps(&state_config);
        let review_prompt = read_review_prompt(&state_config);
        let workspace_id = executor_execution.workspace_id.clone().ok_or(
            ReviewError::ExecutorExecutionMissingWorkspace(req.executor_execution_id),
        )?;

        let (reviewer_execution, reviewer_owner) = self
            .create_reviewer_execution(&task_id, &executor_execution_id, workspace_id.clone(), &req)
            .await?;
        let review = self
            .create_review(&task_id, &reviewer_execution.id, attempt_number)
            .await?;

        let reviewer_lease = ReviewExecutionLease::start(
            Arc::clone(&self.db),
            reviewer_execution.clone(),
            reviewer_owner.clone(),
        )?;
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
                let terminalized = terminalize_review_execution(
                    &self.db,
                    &reviewer_execution,
                    &reviewer_owner,
                    ExecutionStatus::Completed,
                    Some(format!("review:{}", review.id)),
                    None,
                    ReviewTerminalPolicy::default(),
                )
                .await?;
                if !terminalized {
                    return Err(ReviewError::ExecutionLeaseLost {
                        execution_id: reviewer_execution.id.clone(),
                    });
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
                .await?;
                if !terminalized {
                    return Err(ReviewError::ExecutionLeaseLost {
                        execution_id: reviewer_execution.id.clone(),
                    });
                }
                return Err(error);
            }
        };

        let mut auditor_details = None;
        if status == ReviewStatus::Passed && ci_only_review {
            outcome = ReviewOutcome::PassedCiOnly;
            auditor_details = Some(AuditorDetails::pass_ci_only());
        } else if status == ReviewStatus::Passed {
            if let Some(result) = self
                .run_auditor(
                    &req,
                    &executor_execution,
                    workspace_id,
                    review_prompt.as_deref(),
                )
                .await?
            {
                status = result.status;
                outcome = result.outcome;
                failed_step_index = None;
                auditor_details = Some(result.details);
            }
        }

        let finished_at = now_rfc3339();
        let step_results_json = review_details_json(&step_results, auditor_details.as_ref())?;
        let review = ReviewRepo::update_status(
            &*self.db,
            &review.id,
            status,
            step_results_json,
            Some(finished_at.clone()),
            &finished_at,
        )
        .await?;

        match (&outcome, ci_only_review) {
            (ReviewOutcome::Passed, false) => {
                TaskRepo::set_review_passed_at(
                    &*self.db,
                    &task_id,
                    Some(finished_at.clone()),
                    &finished_at,
                )
                .await?;
            }
            (ReviewOutcome::CiFailed { .. }, true) => {
                TaskRepo::set_review_passed_at(&*self.db, &task_id, None, &finished_at).await?;
            }
            _ => {}
        }

        self.publish_review_event(&task_id, &review, outcome.clone(), failed_step_index);

        Ok((review, outcome))
    }

    async fn create_reviewer_execution(
        &self,
        task_id: &str,
        executor_execution_id: &str,
        workspace_id: String,
        req: &ReviewRequest,
    ) -> Result<(Execution, String), ReviewError> {
        let execution_id = new_uuid_v4().to_string();
        let lease_claim = ReviewExecutionLease::new_claim(&execution_id);
        let now = lease_claim.claim.now.clone();
        let execution = ExecutionRepo::create_with_lease(
            &*self.db,
            CreateExecution {
                id: execution_id,
                task_id: task_id.to_owned(),
                agent_id: None,
                role: "reviewer".to_string(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: Some(executor_execution_id.to_owned()),
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
            },
            lease_claim.claim,
        )
        .await
        .map_err(ReviewError::from)?;
        Ok((execution, lease_claim.owner))
    }

    async fn create_review(
        &self,
        task_id: &str,
        execution_id: &str,
        attempt_number: i64,
    ) -> Result<Review, ReviewError> {
        let now = now_rfc3339();
        ReviewRepo::create(
            &*self.db,
            CreateReview {
                id: new_uuid_v4(),
                task_id: task_id.to_owned(),
                execution_id: execution_id.to_owned(),
                attempt_number,
                status: ReviewStatus::Running,
                step_results_json: "[]".to_owned(),
                started_at: now.clone(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .map_err(Into::into)
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

    async fn run_auditor(
        &self,
        req: &ReviewRequest,
        executor_execution: &Execution,
        workspace_id: String,
        review_prompt: Option<&str>,
    ) -> Result<Option<AuditorRunResult>, ReviewError> {
        let Some(auditor_agent_id) = req.auditor_agent_id.as_deref() else {
            return Ok(None);
        };

        let task_id = req.task_id.to_string();
        let task = TaskRepo::get_by_id(&*self.db, &task_id, false)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let repo_id = task.repo_id.as_deref().ok_or(db::DbError::NotFound)?;
        let repo = RepoRepo::get_by_id(&*self.db, repo_id)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let Some(auditor_agent) = self.load_auditor_agent(auditor_agent_id).await? else {
            return Ok(Some(AuditorRunResult::failed("auditor_agent_unavailable")));
        };

        let diff_text = read_git_diff(&req.workspace_path, &repo.default_branch).await?;
        let prompt = auditor::render_auditor_prompt(
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
            &auditor_agent,
        );
        let snapshot = build_auditor_config_snapshot(&auditor_agent, extra_config).await?;
        let lease_claim = ReviewExecutionLease::new_claim(&auditor_execution_id.to_string());
        let now = lease_claim.claim.now.clone();
        let auditor_execution = ExecutionRepo::create_with_lease(
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
                parent_execution_id: Some(executor_execution.id.clone()),
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
        )
        .await?;

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
                heartbeat_interval_seconds: heartbeat_interval(&auditor_agent),
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
        Ok(Some(match auditor::parse_verdict(&final_message) {
            AuditorVerdict::Passed => AuditorRunResult {
                status: ReviewStatus::Passed,
                outcome: ReviewOutcome::Passed,
                details: AuditorDetails::passed(),
            },
            AuditorVerdict::Failed { reason } => AuditorRunResult::failed(reason),
        }))
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

    fn publish_review_event(
        &self,
        task_id: &str,
        review: &Review,
        outcome: ReviewOutcome,
        failed_step_index: Option<usize>,
    ) {
        let (event_type, context) = match outcome {
            ReviewOutcome::Passed | ReviewOutcome::PassedCiOnly => (
                "review.passed",
                EventContext::ReviewPassed {
                    task_id: task_id.to_owned(),
                    review_id: review.id.clone(),
                    attempt_number: review.attempt_number,
                },
            ),
            ReviewOutcome::AuditorFailed { .. }
            | ReviewOutcome::CiFailed { .. }
            | ReviewOutcome::MergeConflict { .. } => (
                "review.failed",
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
            entity_id: review.id.clone(),
            timestamp: event_timestamp(),
            context,
        });
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
    verdict: &'static str,
    reason: Option<String>,
}

impl AuditorDetails {
    fn passed() -> Self {
        Self {
            verdict: "pass",
            reason: None,
        }
    }

    fn pass_ci_only() -> Self {
        Self {
            verdict: "pass_ci_only",
            reason: Some("CI-only re-review".to_owned()),
        }
    }

    fn failed(reason: impl Into<String>) -> Self {
        Self {
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
    serde_json::to_string(&json!({
        "agent_id": agent.id,
        "executor_type": agent.executor_type,
        "model": agent.model,
        "reasoning_effort": agent.reasoning_effort,
        "permission_policy": agent.permission_policy,
        "config": normalized_config,
        "capabilities": capabilities,
        "overrides_applied": overrides_applied.to_json(),
        "snapshotted_at": now_rfc3339(),
    }))
    .map_err(Into::into)
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
    for line in contents.lines() {
        let Ok(entry) = serde_json::from_str::<LogEntry>(line) else {
            continue;
        };
        if entry.kind == LogKind::Assistant {
            append_assistant_log_text(&entry.payload, &mut message);
        } else if entry.kind == LogKind::Stdout {
            // A shell auditor has no assistant channel: its verdict marker is
            // ordinary stdout. Reading only assistant text made every shell
            // auditor fail as "verdict marker missing" even when it printed
            // the marker. This is the auditor's own log, so its stdout is as
            // authoritative here as an agent's final message.
            append_stdout_log_text(&entry.payload, &mut message);
        } else if entry.kind == LogKind::SessionInfo
            && entry.payload.get("subtype").and_then(Value::as_str) == Some("success")
        {
            if let Some(result) = entry.payload.get("result").and_then(Value::as_str) {
                message.push_str(result);
            }
        }
    }
    Ok(message)
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
    Ok(value.get("review").cloned().unwrap_or(value))
}

fn read_ci_steps(state_config: &Value) -> Vec<String> {
    state_config
        .get("ci_steps")
        .and_then(Value::as_array)
        .map(|steps| {
            steps
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
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
