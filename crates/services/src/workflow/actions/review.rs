use std::sync::Arc;

use async_trait::async_trait;
use db::{now_rfc3339, ProjectRepo, ReviewRepo, ReviewStatus, TaskRepo};
use serde_json::{json, Value};

use crate::workflow::{
    default_states, effective_role, engine::WorkflowEngine, HookAction, HookContext, HookResult,
};

use super::common::{
    block_task, cancel_review_after_authority_loss, create_review_attempt_with_authority,
    get_role_assignment, latest_executor_execution, latest_review, publish_review_failed,
    publish_review_passed, review_ci_steps, review_has_auditor_verdict, review_is_ci_only, task,
    task_execution_is_read_only, update_review_status_with_authority_checks, workspace_id,
};

pub(super) async fn resolve_workspace(
    ctx: &HookContext,
    workspace: &db::Workspace,
) -> crate::workspace_backend::Result<crate::workspace_backend::ResolvedWorkspace> {
    ctx.workspace_backend_router
        .resolve(&ctx.db, workspace)
        .await
}

pub struct RunCiSteps;

#[async_trait]
impl HookAction for RunCiSteps {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let task = match task(ctx).await {
            Ok(task) => task,
            Err(reason) => return HookResult::Failed { reason },
        };
        let read_only = match task_execution_is_read_only(ctx, &task).await {
            Ok(read_only) => read_only,
            Err(reason) => return HookResult::Failed { reason },
        };
        if read_only {
            return HookResult::Skipped {
                reason: "read-only Task does not run implementation ci steps".to_owned(),
            };
        }
        let ci_steps = match review_ci_steps(&ctx.state_config) {
            Ok(ci_steps) => ci_steps,
            Err(reason) => return HookResult::Failed { reason },
        };
        if ci_steps.is_empty() {
            if let Err(error) =
                crate::placement::admission::resolve_review_ci_attention(&ctx.db, &ctx.task_id)
                    .await
            {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
            return HookResult::Skipped {
                reason: "no ci steps".to_string(),
            };
        }

        let Some(_) = workspace_id(ctx).await else {
            return HookResult::Skipped {
                reason: "no workspace".to_string(),
            };
        };
        // A coordination root has no implementation execution of its own.
        // Review its shared workspace against the latest relevant child
        // execution instead of accidentally binding CI to a reviewer or
        // other non-implementation execution from the root context.
        let execution_id = latest_executor_execution(ctx)
            .await
            .map(|execution| execution.id);
        let Some(execution_id) = execution_id else {
            return HookResult::Skipped {
                reason: "no executor execution".to_string(),
            };
        };

        let consumers = match ctx.task_service.check_consumers() {
            Some(consumers) => consumers,
            None => {
                return HookResult::Failed {
                    reason: "check consumers are not composed".to_owned(),
                }
            }
        };
        // The review attempt this hook opened, when it already ran: a woken
        // or redelivered step continues that attempt and never opens a second.
        let asked =
            match crate::workflow::engine::durable::hook_effect(&ctx.task_id, CI_REVIEW).await {
                // An effect is stored as JSON text.
                Ok(asked) => asked.and_then(|raw| serde_json::from_str::<AskedReview>(&raw).ok()),
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            };
        let had_review_passed = task.review_passed_at.is_some();
        let reviewer_assignment =
            match get_role_assignment(ctx, crate::workflow::default_roles::REVIEWER).await {
                Ok(assignment) => assignment,
                Err(reason) => return HookResult::Failed { reason },
            };
        let reviewer_assigned = reviewer_assignment
            .as_ref()
            .is_some_and(|assignment| assignment.assignee_id.is_some());
        let (review, asked) = match asked {
            Some(asked) => match ReviewRepo::get_by_id(&*ctx.db, &asked.review_id).await {
                Ok(Some(review)) => (review, asked),
                Ok(None) => {
                    return HookResult::Failed {
                        reason: "the review attempt of this check no longer exists".to_owned(),
                    }
                }
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            },
            None => {
                let review = match create_review_attempt_with_authority(
                    ctx,
                    &execution_id,
                    task.version,
                    &execution_id,
                )
                .await
                {
                    Ok(review) => review,
                    Err(reason) => return HookResult::Failed { reason },
                };
                let asked = AskedReview {
                    review_id: review.id.clone(),
                    task_version: task.version,
                    review_status: review.status.to_string(),
                    review_updated_at: review.updated_at.clone(),
                };
                if let Err(error) = crate::workflow::engine::durable::record_hook_effect(
                    &ctx.task_id,
                    CI_REVIEW,
                    &serde_json::to_value(&asked).expect("asking-time authority serializes"),
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                (review, asked)
            }
        };
        // An earlier delivery of this step already wrote the CI result.
        if let Some(settled) = settled_ci_result(&review) {
            return settled;
        }
        // The settlement below is fenced on the authority this hook asked
        // under, not on what the woken step re-reads: a Task version bump or
        // a Review row change while the check ran refuses the write exactly
        // where the inline run refused it, and cancels the attempt.
        let authority_version = asked.task_version;
        let asked_status = match asked.review_status.parse::<ReviewStatus>() {
            Ok(status) => status,
            Err(_) => {
                return HookResult::Failed {
                    reason: "the review attempt's asking-time authority is unreadable".to_owned(),
                }
            }
        };
        let review = db::Review {
            status: asked_status,
            updated_at: asked.review_updated_at.clone(),
            ..review
        };
        let consumer_id =
            match crate::workflow::engine::durable::hook_effect(&ctx.task_id, CI_CONSUMER).await {
                Ok(Some(consumer_id)) => {
                    serde_json::from_str::<String>(&consumer_id).unwrap_or_default()
                }
                Ok(None) => match request_entry_check(ctx, &task, &review, &consumers).await {
                    Ok(EntryCheck::NothingToRun) => String::new(),
                    Ok(EntryCheck::Asked(consumer_id)) => {
                        if let Err(error) = crate::workflow::engine::durable::record_hook_effect(
                            &ctx.task_id,
                            CI_CONSUMER,
                            &json!(consumer_id),
                        )
                        .await
                        {
                            return HookResult::Failed {
                                reason: error.to_string(),
                            };
                        }
                        consumer_id
                    }
                    Err(result) => return result,
                },
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            };
        // The verdict is the result's outcome. Whether that result may be
        // reused by a later request is not read here: a pass is a pass.
        let result = if consumer_id.is_empty() {
            None
        } else {
            use crate::check_runner::consumer::CheckWaitState;
            match consumers.wait_state(&consumer_id).await {
                Ok(CheckWaitState::Result(result)) => Some(result),
                // No result yet, or no verdict after the automatic retries
                // (the Task is parked on the typed check condition and the
                // owner's `retry` asks again): the step waits. The deadline
                // is the re-arm for a delivery that never arrives.
                Ok(CheckWaitState::Pending | CheckWaitState::Exhausted) => {
                    let until = (chrono::Utc::now()
                        + chrono::Duration::seconds(entry_check_wake_seconds()))
                    .to_rfc3339();
                    if !crate::workflow::engine::durable::suspend_hook(
                        &ctx.task_id,
                        &consumer_id,
                        until,
                    ) {
                        return HookResult::Failed {
                            reason: "review-entry CI runs from the Task's hooks step".to_owned(),
                        };
                    }
                    return HookResult::Ok;
                }
                // Nothing will ever answer: the consumer was cancelled or its
                // run row is gone. Waiting again would wait forever, so the
                // attempt is cancelled and the hook fails as an unfinished
                // check does.
                Ok(CheckWaitState::Lost) => {
                    let reason = "review check was lost before it produced a result";
                    cancel_review_after_authority_loss(ctx, &review, reason).await;
                    return HookResult::Failed {
                        reason: reason.to_owned(),
                    };
                }
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            }
        };
        let (ci_results, failed_step_index) = match result {
            None => (Vec::new(), None),
            Some(result) => {
                let failed = result
                    .commands
                    .iter()
                    .position(|command| command.exit_code != 0);
                let unfinished = match result.outcome {
                    db::CheckResultOutcome::Pass => None,
                    db::CheckResultOutcome::Fail if failed.is_some() => None,
                    db::CheckResultOutcome::TimedOut => Some("review command timed out"),
                    _ => Some("review check did not finish"),
                };
                if let Some(reason) = unfinished {
                    cancel_review_after_authority_loss(ctx, &review, reason).await;
                    return HookResult::Failed {
                        reason: reason.to_owned(),
                    };
                }
                (
                    result
                        .commands
                        .into_iter()
                        .map(|command| {
                            serde_json::to_value(command).expect("CI command facts serialize")
                        })
                        .collect::<Vec<_>>(),
                    failed,
                )
            }
        };
        if let Err(error) =
            crate::placement::admission::resolve_review_ci_attention(&ctx.db, &ctx.task_id).await
        {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
        let mut review_details = json!({ "ci_steps": ci_results });
        if let Some(attempt) = crate::workflow::engine::durable::current_hook(&ctx.task_id) {
            for entry in review_details["ci_steps"]
                .as_array_mut()
                .expect("CI results array")
            {
                entry["step_id"] = json!(attempt.step.id);
            }
        }
        let now = now_rfc3339();

        let user_approval_required =
            gate_requires_user_approval(ctx) || human_review_requested(ctx, reviewer_assigned);

        let (status, finished_at) = if let Some(failed_step_index) = failed_step_index {
            let review = match ReviewRepo::update_status_with_review_authority_and_task_projection(
                &*ctx.db,
                &review.id,
                ReviewStatus::Failed,
                review_details.to_string(),
                Some(now.clone()),
                &now,
                authority_version,
                &ctx.to_state,
                ctx.project_version,
                ctx.project_workflow_definition.as_deref(),
                review.status.clone(),
                &review.updated_at,
                &execution_id,
                Some(None),
            )
            .await
            {
                Ok(review) => review,
                Err(error) => {
                    cancel_review_after_authority_loss(ctx, &review, &error.to_string()).await;
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            };
            let settled_task = match TaskRepo::get_by_id(&*ctx.db, &ctx.task_id, false).await {
                Ok(Some(task)) => task,
                Ok(None) => {
                    return HookResult::Failed {
                        reason: "task disappeared after review settlement".to_owned(),
                    };
                }
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            };

            let memory_service = crate::MemoryService::new(Arc::clone(&ctx.db));
            if let Err(error) = memory_service
                .record_review_result_if_final(&ctx.project_id, &review)
                .await
            {
                tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
            }
            publish_review_failed(ctx, &review, failed_step_index);
            if had_review_passed && ctx.triggered_by.is_user() {
                let reason = "merge-fix follow-up failed: ci";
                if let Err(error) = block_task(
                    ctx,
                    &settled_task,
                    reason,
                    api_types::FailureKind::CiFailed,
                    None,
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                return HookResult::Failed {
                    reason: reason.to_string(),
                };
            }
            return HookResult::Failed {
                reason: ci_failure_summary(&review_details["ci_steps"], failed_step_index),
            };
        } else if had_review_passed && !reviewer_assigned && !user_approval_required {
            review_details["auditor"] = json!({
                "verdict": "pass_ci_only",
                "reason": "CI-only re-review",
            });
            (ReviewStatus::Passed, Some(now.clone()))
        } else if reviewer_assigned {
            (ReviewStatus::Running, None)
        } else if user_approval_required {
            let reason = if gate_requires_user_approval(ctx) {
                "gate requires user approval"
            } else {
                "manual review requested"
            };
            review_details["user_approval"] = json!({
                "status": "awaiting_human",
                "reason": reason,
            });
            (ReviewStatus::AwaitingHuman, None)
        } else {
            (ReviewStatus::Passed, Some(now.clone()))
        };

        let review = if matches!(status, ReviewStatus::Passed | ReviewStatus::Failed) {
            match ReviewRepo::update_status_with_review_authority_and_task_projection(
                &*ctx.db,
                &review.id,
                status.clone(),
                review_details.to_string(),
                finished_at.clone(),
                &now,
                authority_version,
                &ctx.to_state,
                ctx.project_version,
                ctx.project_workflow_definition.as_deref(),
                review.status.clone(),
                &review.updated_at,
                &execution_id,
                Some((status == ReviewStatus::Passed).then_some(now.clone())),
            )
            .await
            {
                Ok(review) => review,
                Err(error) => {
                    cancel_review_after_authority_loss(ctx, &review, &error.to_string()).await;
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            }
        } else {
            match update_review_status_with_authority_checks(
                ctx,
                &review,
                status.clone(),
                review_details.to_string(),
                finished_at.clone(),
                &now,
                authority_version,
                &execution_id,
            )
            .await
            {
                Ok(review) => review,
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
            }
        };

        let memory_service = crate::MemoryService::new(Arc::clone(&ctx.db));
        if let Err(error) = memory_service
            .record_review_result_if_final(&ctx.project_id, &review)
            .await
        {
            tracing::warn!(error = %error, "memory indexing failed (non-fatal)");
        }

        if status == ReviewStatus::Passed {
            publish_review_passed(ctx, &review);
        }

        HookResult::Ok
    }
}

/// Hook effects of `run_ci_steps`: the review attempt it opened and the check
/// consumer it asked with. They make a woken or redelivered step continue.
const CI_REVIEW: &str = "ci_review";
const CI_CONSUMER: &str = "ci_consumer";
/// The wall limit of one review-entry CI run.
pub const ENTRY_CHECK_WALL_SECONDS: u64 = 3600;
static ENTRY_CHECK_WALL_OVERRIDE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// The wall limit in force. Tests shorten it to drive a timeout through the
/// runner; production never sets the override.
fn entry_check_wall_seconds() -> u64 {
    match ENTRY_CHECK_WALL_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed) {
        0 => ENTRY_CHECK_WALL_SECONDS,
        seconds => seconds,
    }
}
#[doc(hidden)]
pub fn set_entry_check_wall_seconds_for_test(seconds: u64) {
    ENTRY_CHECK_WALL_OVERRIDE.store(seconds, std::sync::atomic::Ordering::Relaxed);
}
/// When a suspended step looks again on its own: past the run's wall limit
/// and the worker's settlement grace.
fn entry_check_wake_seconds() -> i64 {
    entry_check_wall_seconds() as i64 + 120
}

/// The authority `run_ci_steps` asked under, carried across the suspension:
/// the review attempt it opened and the versions the settlement is fenced on.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AskedReview {
    review_id: String,
    task_version: i64,
    review_status: String,
    review_updated_at: String,
}

enum EntryCheck {
    /// Every configured step is blank: nothing runs and the entry passes.
    NothingToRun,
    Asked(String),
}

/// What an earlier delivery of this hook already settled on the Review.
fn settled_ci_result(review: &db::Review) -> Option<HookResult> {
    let details: Value = serde_json::from_str(&review.step_results_json).unwrap_or(Value::Null);
    let steps = details.get("ci_steps");
    match review.status {
        ReviewStatus::Failed => {
            let failed = steps
                .and_then(Value::as_array)
                .and_then(|steps| steps.iter().position(|step| step["exit_code"] != json!(0)))
                .unwrap_or(0);
            Some(HookResult::Failed {
                reason: ci_failure_summary(steps.unwrap_or(&Value::Null), failed),
            })
        }
        ReviewStatus::Cancelled => Some(HookResult::Failed {
            reason: "review check did not finish".to_owned(),
        }),
        // A fresh attempt is created with an empty `ci_steps` list; the
        // settlement writes one entry per command that ran.
        ReviewStatus::Running
            if steps
                .and_then(Value::as_array)
                .is_none_or(|steps| steps.is_empty()) =>
        {
            None
        }
        _ => Some(HookResult::Ok),
    }
}

/// Ask the durable check runner for this entry's CI. The request is keyed by
/// the Task status entry and the review attempt, so asking again (a
/// redelivered step) returns the same consumer and never a second run.
async fn request_entry_check(
    ctx: &HookContext,
    task: &db::Task,
    review: &db::Review,
    consumers: &crate::check_runner::consumer::TaskCheckConsumers,
) -> Result<EntryCheck, HookResult> {
    let failed = |reason: String| HookResult::Failed { reason };
    let workspace = match crate::task_service::workspace::prepare_workspace_for(
        &ctx.db,
        &ctx.workspace_root,
        task,
        &task.id,
        ctx.repo_cache_locks.clone(),
        &ctx.workspace_backend_router,
        crate::workspace_manager::Purpose::Check,
    )
    .await
    {
        Ok(workspace) => workspace,
        Err(error) => {
            let reset = matches!(error, crate::ServiceError::WorkspaceResetRequired { .. });
            let retry = matches!(
                error,
                crate::ServiceError::DaemonUnavailable { .. }
                    | crate::ServiceError::DaemonTimeout { .. }
            );
            // No command ran; this is not a Review attempt.
            let _ = ctx.db.discard_unstarted_review(&review.id).await;
            if reset || retry {
                return Err(interrupt_ci(ctx, task, &error.to_string(), retry, reset).await);
            }
            return Err(failed(error.to_string()));
        }
    };
    let environment = ::review::contract::project_environment(&ctx.db, &ctx.task_id)
        .await
        .map_err(failed)?;
    let resolved = resolve_workspace(ctx, &workspace)
        .await
        .map_err(|error| failed(error.to_string()))?;
    let daemon = resolved.placement.owner_kind == db::PlacementOwnerKind::Daemon;
    let state = match resolved.backend.describe(&resolved.placement).await {
        Ok(state) => state,
        Err(error) => {
            let retry = matches!(
                &error,
                crate::workspace_backend::WorkspaceBackendError::OwnerUnreachable { .. }
                    | crate::workspace_backend::WorkspaceBackendError::RpcTimeoutBeforeStart { .. }
            );
            let _ = ctx.db.discard_unstarted_review(&review.id).await;
            if retry || daemon {
                return Err(interrupt_ci(ctx, task, &error.to_string(), retry, false).await);
            }
            return Err(failed(error.to_string()));
        }
    };
    let commit_sha = state
        .head_sha
        .ok_or_else(|| failed("review workspace has no HEAD".to_owned()))?;
    let source = json!({
        "task_id": task.id, "project_id": task.project_id, "repo_id": workspace.repo_id,
    });
    // A daemon-owned checkout gets its owner's frozen policy from the
    // builder: it always runs and its result is never reused.
    let spec = ::review::check_spec::build_check_spec(
        api_types::CheckPurpose::EntryCi,
        &::review::check_spec::CheckSpecConfiguration {
            source: &source,
            entry_state_config: Some(&ctx.state_config),
            environment: &environment,
            hooks: &[],
            hook_test_index: None,
            single_environment_check: None,
            event: api_types::LifecycleEvent::BeforeWork,
            role: "",
            owner_is_daemon: daemon,
            canonical_policy: true,
            workspace: Some(::review::check_spec::CheckWorkspaceIdentity {
                workspace_id: &workspace.id,
                generation: u64::try_from(resolved.placement.generation)
                    .map_err(|_| failed("invalid workspace generation".to_owned()))?,
            }),
            queue_head_bundle: None,
        },
    )
    .map_err(failed)?;
    if spec.commands.is_empty() {
        return Ok(EntryCheck::NothingToRun);
    }
    let inputs = crate::check_runner::policy::digest_input(&ctx.db, spec, &environment.env)
        .await
        .map_err(|error| failed(error.to_string()))?;
    let status_epoch = db::CheckDeliveryRepo::live_task_epoch(&*ctx.db, &task.id)
        .await
        .map_err(|error| failed(error.to_string()))?
        .ok_or_else(|| failed("task disappeared before its review check".to_owned()))?;
    let requested = consumers
        .request(crate::check_runner::consumer::TaskCheckRequest {
            task_id: task.id.clone(),
            status_epoch,
            authority: review.id.clone(),
            origin: db::CheckConsumerOrigin::Entry,
            purpose: api_types::CheckPurpose::EntryCi,
            identity: db::CheckRunIdentity {
                project_id: task.project_id.clone(),
                repo_id: workspace.repo_id.clone(),
                commit_sha,
                inputs,
            },
            workspace_id: Some(workspace.id.clone()),
            machine_id: resolved.placement.daemon_id.clone().filter(|_| daemon),
            wall_timeout_seconds: entry_check_wall_seconds(),
        })
        .await
        .map_err(|error| failed(error.to_string()))?;
    Ok(EntryCheck::Asked(requested.consumer.id))
}

/// Run `run_ci_steps` once the way the step queue runs it, for tests that
/// exercise the hook without the engine: as a claimed hooks step of the Task,
/// with the check it asks for executed by `worker` and the hook run again
/// when the result exists. The result's delivery step is not executed; the
/// step and the delivery are removed afterwards.
#[doc(hidden)]
pub async fn run_ci_steps_in_step(
    ctx: &HookContext,
    worker: &Arc<crate::check_runner::worker::CheckRunWorker>,
) -> HookResult {
    use crate::check_runner::consumer::CheckVerdict;
    let db = Arc::clone(&ctx.db);
    let id = db::new_uuid_v4();
    let entered: Option<(String, i64)> =
        sqlx::query_as("SELECT status,version FROM task WHERE id=?")
            .bind(&ctx.task_id)
            .fetch_optional(db.pool())
            .await
            .expect("Task reads");
    let (status, version) = entered.unwrap_or_else(|| (ctx.to_state.clone(), 1));
    db::TaskStepRepo::enqueue_step(
        &*db,
        &db::EnqueueTaskStep {
            kind: "hooks".to_owned(),
            id: id.clone(),
            task_id: ctx.task_id.clone(),
            payload_json: "{}".to_owned(),
            causation_step_id: None,
            causation_key: id.clone(),
            chain_id: id.clone(),
            chain_position: 1,
            expected_status: status,
            expected_version: version,
            expected_epoch: None,
            lane: "long".to_owned(),
            available_at: now_rfc3339(),
        },
    )
    .await
    .expect("hooks step enqueues");
    sqlx::query(
        "UPDATE task_step SET status='claimed',claimed_by='run-ci-steps',lease_until=? WHERE id=?",
    )
    .bind(db::task_writer::lease_deadline())
    .bind(&id)
    .execute(db.pool())
    .await
    .expect("hooks step claims");
    let step = db::TaskStepRepo::task_steps(&*db, &ctx.task_id)
        .await
        .expect("steps list")
        .into_iter()
        .find(|step| step.id == id)
        .expect("the hooks step exists");
    let run = async {
        db.start_hook(&step, 0).await.expect("hook starts");
        loop {
            let (result, wait) = crate::workflow::engine::durable::in_hook(
                Arc::clone(&db),
                step.clone(),
                0,
                RunCiSteps.execute(ctx),
            )
            .await;
            let Some(wait) = wait else {
                break result;
            };
            let consumers = ctx
                .task_service
                .check_consumers()
                .expect("check consumers are composed");
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
            while !matches!(
                consumers
                    .verdict(&wait.consumer_id)
                    .await
                    .expect("verdict reads"),
                Some(CheckVerdict::Result(_))
            ) {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the review-entry check produced no result"
                );
                let mut jobs = tokio::task::JoinSet::new();
                worker.sweep(&mut jobs).await.expect("check sweep");
                while let Some(job) = jobs.join_next().await {
                    job.expect("check job joins").expect("check job");
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    };
    let result = db::task_writer::in_task_step(step.clone(), run).await;
    sqlx::query("DELETE FROM task_step WHERE task_id=? AND (id=? OR (kind='command' AND json_extract(payload_json,'$.operation') IN ('apply_check_result','apply_check_progress')))")
        .bind(&ctx.task_id)
        .bind(&id)
        .execute(db.pool())
        .await
        .expect("helper steps are removed");
    result
}

pub struct AutoCascadeOnReviewPass;

#[async_trait]
impl HookAction for AutoCascadeOnReviewPass {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let user_approval_required = gate_requires_user_approval(ctx);
        let task = match task(ctx).await {
            Ok(task) => task,
            Err(reason) => return HookResult::Failed { reason },
        };

        let reviewer_assigned =
            match get_role_assignment(ctx, crate::workflow::default_roles::REVIEWER).await {
                Ok(assignment) => {
                    assignment.is_some_and(|assignment| assignment.assignee_id.is_some())
                }
                Err(reason) => return HookResult::Failed { reason },
            };
        let latest_review = match latest_review(ctx).await {
            Ok(review) => review,
            Err(reason) => return HookResult::Failed { reason },
        };
        let review_id = latest_review.as_ref().map(|r| r.id.clone());
        let input = match crate::workflow::engine::durable::hook_effect(
            &ctx.task_id,
            "review_pass_input",
        )
        .await
        {
            Ok(Some(value)) => match serde_json::from_str::<Value>(&value) {
                Ok(value) => value,
                Err(error) => {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    }
                }
            },
            Ok(None) => {
                let value = json!({"review_id":review_id,"had_review_passed":task.review_passed_at.is_some()});
                if let Err(error) = crate::workflow::engine::durable::record_hook_effect(
                    &ctx.task_id,
                    "review_pass_input",
                    &value,
                )
                .await
                {
                    return HookResult::Failed {
                        reason: error.to_string(),
                    };
                }
                value
            }
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                }
            }
        };
        let had_review_passed = if input["review_id"] == json!(review_id) {
            input["had_review_passed"].as_bool().unwrap_or(false)
        } else {
            task.review_passed_at.is_some()
        };
        match latest_review {
            Some(review)
                if review.status == ReviewStatus::Passed
                    && had_review_passed
                    && !user_approval_required
                    && (!reviewer_assigned || review_has_auditor_verdict(&review)) =>
            {
                let project = match ProjectRepo::get_by_id(&*ctx.db, &ctx.project_id).await {
                    Ok(Some(project)) => project,
                    Ok(None) => {
                        return HookResult::Failed {
                            reason: format!("project {} not found", ctx.project_id),
                        };
                    }
                    Err(error) => {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                };
                if project.paused_at.is_some() {
                    if let Err(error) =
                        crate::deferred_dispatch::defer_integration_for_pause(&ctx.db, &task).await
                    {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                    return HookResult::Skipped {
                        reason: "project paused; integration deferred".to_owned(),
                    };
                }
                HookResult::Cascade {
                    to: default_states::MERGING.to_string(),
                    reason: if review_is_ci_only(&review) {
                        "CI-only re-review passed".to_string()
                    } else {
                        "review passed".to_string()
                    },

                    bridge: if review_is_ci_only(&review) {
                        api_types::TransitionBridge::new(
                            api_types::TransitionBridgeKind::CiOnlyReviewPassed,
                        )
                    } else {
                        Default::default()
                    },
                }
            }
            Some(review)
                if review.status == ReviewStatus::Failed
                    && Some(review.execution_id.as_str()) == ctx.execution_id.as_deref() =>
            {
                if had_review_passed && !review_has_auditor_verdict(&review) {
                    if let Err(error) = TaskRepo::set_review_passed_at_cas(
                        &*ctx.db,
                        &ctx.task_id,
                        task.version,
                        None,
                        &now_rfc3339(),
                    )
                    .await
                    {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                    let reason = "merge-fix follow-up failed: ci";
                    if let Err(error) =
                        block_task(ctx, &task, reason, api_types::FailureKind::CiFailed, None).await
                    {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                    return HookResult::Ok;
                }
                let budget = match db::budget::limit(
                    &task,
                    db::budget::Kind::Review,
                    Some(&ctx.state_config),
                    ctx.gate_config.as_ref(),
                ) {
                    Ok(budget) => budget,
                    Err(error) => {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                };
                let existing_count = match db::budget::spent(
                    ctx.db.pool(),
                    &ctx.task_id,
                    db::budget::Kind::Review.key(),
                )
                .await
                {
                    Ok(n) => n,
                    Err(error) => {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        }
                    }
                };
                if !db::budget::allows_retry(i64::from(budget), existing_count) {
                    let reason = "review retry budget exhausted";
                    if let Err(error) = block_task(
                        ctx,
                        &task,
                        reason,
                        api_types::FailureKind::ReviewGateFailed,
                        None,
                    )
                    .await
                    {
                        return HookResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                    HookResult::Ok
                } else {
                    HookResult::Cascade {
                        to: default_states::IN_PROGRESS.to_string(),
                        reason: "review failed".to_string(),

                        bridge: Default::default(),
                    }
                }
            }
            Some(_) | None => HookResult::Ok,
        }
    }
}

pub struct AutoCascadeOnUnconfiguredReview;

#[async_trait]
impl HookAction for AutoCascadeOnUnconfiguredReview {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        let Some(state) = ctx
            .workflow
            .states
            .iter()
            .find(|state| state.name == ctx.to_state)
        else {
            return HookResult::Failed {
                reason: WorkflowEngine::undefined_state_message(&ctx.to_state, &ctx.workflow),
            };
        };
        let Some(role_name) = effective_role(state) else {
            return HookResult::Skipped {
                reason: "review state has no role".to_string(),
            };
        };
        let assignment = match get_role_assignment(ctx, role_name).await {
            Ok(assignment) => assignment,
            Err(reason) => return HookResult::Failed { reason },
        };
        if assignment
            .as_ref()
            .is_some_and(|assignment| assignment.assignee_id.is_some())
        {
            return HookResult::Skipped {
                reason: format!("{role_name} role assigned"),
            };
        }

        let task = match task(ctx).await {
            Ok(task) => task,
            Err(reason) => return HookResult::Failed { reason },
        };
        let ci_steps = match task_execution_is_read_only(ctx, &task).await {
            Ok(true) => Vec::new(),
            Ok(false) => match review_ci_steps(&ctx.state_config) {
                Ok(ci_steps) => ci_steps,
                Err(reason) => return HookResult::Failed { reason },
            },
            Err(reason) => return HookResult::Failed { reason },
        };
        if !ci_steps.is_empty() {
            return HookResult::Skipped {
                reason: "review checks configured".to_string(),
            };
        }

        if gate_requires_user_approval(ctx) || human_review_requested(ctx, false) {
            if let Err(reason) = super::common::ensure_review_awaiting_human(ctx).await {
                return HookResult::Failed { reason };
            }
            return HookResult::Ok;
        }

        let project = match ProjectRepo::get_by_id(&*ctx.db, &ctx.project_id).await {
            Ok(Some(project)) => project,
            Ok(None) => {
                return HookResult::Failed {
                    reason: format!("project {} not found", ctx.project_id),
                };
            }
            Err(error) => {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
        };
        if project.paused_at.is_some() {
            if let Err(error) =
                crate::deferred_dispatch::defer_integration_for_pause(&ctx.db, &task).await
            {
                return HookResult::Failed {
                    reason: error.to_string(),
                };
            }
            return HookResult::Skipped {
                reason: "project paused; integration deferred".to_owned(),
            };
        }

        HookResult::Cascade {
            to: default_states::MERGING.to_string(),
            reason: "review skipped: no checks or reviewer assigned".to_string(),

            bridge: Default::default(),
        }
    }
}

fn gate_requires_user_approval(ctx: &HookContext) -> bool {
    ctx.gate_config
        .as_ref()
        .is_some_and(|gate_config| gate_config.requires_user_approval())
}

fn human_review_requested(ctx: &HookContext, reviewer_assigned: bool) -> bool {
    ctx.triggered_by.is_user() && ctx.to_state == default_states::REVIEW && !reviewer_assigned
}

/// One line naming what failed, for the transition log and Task history. The
/// full output stays on the review; this line only has to tell a reader which
/// command broke and how, without opening it.
fn ci_failure_summary(ci_results: &Value, failed_step_index: usize) -> String {
    const MAX_DETAIL_CHARS: usize = 240;
    let step = ci_results.get(failed_step_index);
    let command = step
        .and_then(|step| step.get("command"))
        .and_then(Value::as_str)
        .unwrap_or("unknown command");
    let exit_code = step
        .and_then(|step| step.get("exit_code"))
        .and_then(Value::as_i64)
        .map_or_else(|| "?".to_owned(), |code| code.to_string());
    let mut summary = format!("CI step {failed_step_index} failed (exit {exit_code}): `{command}`");
    let detail = step
        .and_then(|step| step.get("output_tail"))
        .and_then(Value::as_str)
        .and_then(|output| {
            output
                .lines()
                .rev()
                .map(str::trim)
                .find(|line| !line.is_empty())
        });
    if let Some(detail) = detail {
        let detail: String = detail.chars().take(MAX_DETAIL_CHARS).collect();
        summary.push_str(" -- ");
        summary.push_str(&detail);
    }
    summary
}

async fn interrupt_ci(
    ctx: &HookContext,
    task: &db::Task,
    reason: &str,
    retry: bool,
    reset: bool,
) -> HookResult {
    if !ctx.triggered_by.is_user() {
        if let Err(error) = ctx
            .task_service
            .annotate_review_ci_interruption(task, ctx, reason, retry, reset)
            .await
        {
            return HookResult::Failed {
                reason: error.to_string(),
            };
        }
    }
    HookResult::Failed {
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
mod ci_failure_summary_tests {
    use super::*;

    #[test]
    fn names_the_command_exit_code_and_last_output_line() {
        let results = json!([
            {"command": "cargo fmt --check", "exit_code": 0, "output_tail": ""},
            {
                "command": "python3 -m unittest discover -s tests",
                "exit_code": 1,
                "output_tail": "Traceback...\nTypeError: unsupported operand\n\nFAILED (errors=3)\n",
            },
        ]);
        assert_eq!(
            ci_failure_summary(&results, 1),
            "CI step 1 failed (exit 1): `python3 -m unittest discover -s tests` -- FAILED (errors=3)"
        );
    }

    #[test]
    fn tolerates_missing_output() {
        let results = json!([{"command": "make check", "exit_code": 2}]);
        assert_eq!(
            ci_failure_summary(&results, 0),
            "CI step 0 failed (exit 2): `make check`"
        );
    }
}
