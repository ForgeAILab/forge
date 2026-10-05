//! Durable service command ingress. The worker uses these same handlers inline.
use super::*;
use api_types::{MoveTaskRequest, TaskAction, UpdateTaskRequest};
use db::TaskStepRepo;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{future::Future, pin::Pin};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TaskCommand {
    pub operation: String,
    pub arguments: Value,
    pub preempt: bool,
}
impl TaskCommand {
    pub(crate) fn payload_json(&self) -> Result<String> {
        let mut payload = serde_json::to_value(self)
            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
        let agent = match self.operation.as_str() {
            "dispatch_initial_role_execution_with_metadata_and_admission"
            | "dispatch_initial_role_execution_with_optional_admission" => {
                self.arguments[1].as_str()
            }
            "dispatch_recovery_role" => self.arguments[3].as_str(),
            "claim_task" | "claim_and_start_task" => self.arguments[1]["Agent"].as_str(),
            _ => None,
        };
        if let Some(agent) = agent {
            payload["admission_agent_id"] = serde_json::json!(agent);
        }
        if let Some(replay) = TaskService::recovery_context() {
            payload["replaying_recovery"] = serde_json::json!(replay);
        }
        if let Ok(actor) = super::actions::TASK_ACTION_ACTOR.try_with(Clone::clone) {
            payload["action_actor"] = serde_json::json!(actor);
        }
        if super::actions::TASK_ACTION_COMMAND.try_with(|_| ()).is_ok() {
            payload["action_command"] = serde_json::json!(true);
        }
        Ok(payload.to_string())
    }
}
type ManualStopArguments = (
    db::Execution,
    Task,
    String,
    Option<String>,
    Option<String>,
    String,
    Option<db::TaskRoleAssignment>,
    String,
    String,
);
fn encode<T: serde::Serialize>(value: T) -> Result<Value> {
    serde_json::to_value(value).map_err(|e| ServiceError::invalid_operation(e.to_string()))
}
impl TaskService {
    async fn command_task(&self, id: &str) -> Result<Task> {
        TaskRepo::get_by_id(&*self.db, id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", id.to_owned()))
    }
    pub(crate) async fn request_task_command<T: serde::de::DeserializeOwned>(
        &self,
        task_id: &str,
        operation: &str,
        arguments: Value,
        preempt: bool,
    ) -> Result<T> {
        self.task_step_worker()
            .request_command(
                task_id,
                TaskCommand {
                    operation: operation.to_owned(),
                    arguments,
                    preempt,
                },
            )
            .await
    }
    pub(crate) async fn enqueue_task_command(
        &self,
        id: &str,
        operation: &str,
        arguments: Value,
        preempt: bool,
    ) -> Result<String> {
        let task = self.command_task(id).await?;
        let step_id = db::new_uuid_v4();
        let queued = self
            .db
            .enqueue_step(&db::EnqueueTaskStep {
                id: step_id.clone(),
                task_id: id.to_owned(),
                kind: "command".to_owned(),
                payload_json: TaskCommand {
                    operation: operation.to_owned(),
                    arguments,
                    preempt,
                }
                .payload_json()?,
                causation_step_id: db::task_writer::current_task_step().map(|step| step.id),
                causation_key: step_id.clone(),
                chain_id: step_id,
                chain_position: 1,
                expected_status: task.status,
                expected_version: task.version,
                expected_epoch: None,
                lane: "fast".to_owned(),
                available_at: db::now_rfc3339(),
            })
            .await?;
        if preempt {
            self.db.request_task_preemption(id).await?;
        }
        Ok(queued)
    }
    pub(crate) fn execute_task_command<'a>(
        &'a self,
        command: &'a TaskCommand,
    ) -> Pin<Box<dyn Future<Output = Result<Value>> + Send + 'a>> {
        Box::pin(async move {
            match command.operation.as_str() {
                "dispatch_role_follow_up_with_admission" => {
                    let (id, role, parent, prompt, trigger, admission): (
                        String,
                        String,
                        String,
                        String,
                        String,
                        db::ExecutionAdmission,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    encode(
                        Box::pin(self.dispatch_role_follow_up_with_admission(
                            &id, &role, parent, prompt, &trigger, admission,
                        ))
                        .await?,
                    )
                }
                "dispatch_role_follow_up_with_agent" => {
                    let (id, role, parent, agent, prompt, trigger): (
                        String,
                        String,
                        String,
                        String,
                        String,
                        String,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    encode(
                        Box::pin(self.dispatch_role_follow_up_with_agent(
                            &id, &role, parent, agent, prompt, &trigger,
                        ))
                        .await?,
                    )
                }
                "create_running_execution" => {
                    let (input, created): (db::CreateExecution, bool) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    encode(Box::pin(self.create_running_execution(input, created)).await?)
                }
                "follow_up_execution" => {
                    let (id, message, agent, overrides): (
                        String,
                        String,
                        Option<String>,
                        Option<ExecutionOverrides>,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    encode(Box::pin(self.follow_up_execution(id, message, agent, overrides)).await?)
                }
                "start_execution" => {
                    let (id,): (String,) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    encode(Box::pin(self.start_execution(id)).await?)
                }
                "dispatch_queued_recovery" => {
                    let (id,): (String,) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    let task = self.command_task(&id).await?;
                    encode(Box::pin(self.dispatch_queued_recovery(&task)).await?)
                }
                "claim_and_start_task" => {
                    let (id, assignee, overrides): (String, Assignee, Option<ExecutionOverrides>) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    encode(Box::pin(self.claim_and_start_task(id, assignee, overrides)).await?)
                }
                "expire_owner_wait" => {
                    let (id,): (String,) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    let task = self.command_task(&id).await?;
                    encode(Box::pin(self.expire_owner_wait(&task)).await?)
                }
                "add_task_dependency" | "remove_task_dependency" => {
                    let (id, dependency): (String, String) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    if command.operation == "add_task_dependency" {
                        encode(Box::pin(self.add_task_dependency(&id, &dependency)).await?)
                    } else {
                        encode(Box::pin(self.remove_task_dependency(&id, &dependency)).await?)
                    }
                }
                "cancel_task_with_options" => {
                    let (task_id, version, reason, actor): (String, Option<i64>, String, Actor) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    let version = if version.is_some() {
                        Some(self.command_task(&task_id).await?.version)
                    } else {
                        None
                    };
                    encode(
                        Box::pin(self.cancel_task_with_options(task_id, version, reason, actor))
                            .await?,
                    )
                }
                "dispatch_initial_role_execution_with_optional_admission" => {
                    let (task_id, agent_id, role, prompt, metadata, admission): (
                        String,
                        String,
                        String,
                        String,
                        Option<Value>,
                        Option<db::ExecutionAdmission>,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    encode(
                        Box::pin(
                            self.dispatch_initial_role_execution_with_optional_admission(
                                &task_id, &agent_id, &role, prompt, metadata, admission,
                            ),
                        )
                        .await?,
                    )
                }
                "advance_task_condition" => {
                    let (task_id, workflow, target, reason, actor): (
                        String,
                        api_types::WorkflowDefinition,
                        String,
                        String,
                        Actor,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    let task = self.command_task(&task_id).await?;
                    encode(
                        Box::pin(
                            self.advance_task_condition(&task, &workflow, target, reason, actor),
                        )
                        .await?,
                    )
                }
                "perform_task_action_as" => {
                    let (task_id, action, _version, actor): (String, TaskAction, i64, Actor) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    let version = self.command_task(&task_id).await?.version;
                    encode(
                        Box::pin(self.perform_task_action_as(task_id, action, version, actor))
                            .await?,
                    )
                }
                "claim_task" => {
                    let (task_id, assignee, overrides): (
                        String,
                        Assignee,
                        Option<ExecutionOverrides>,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(Box::pin(self.claim_task(task_id, assignee, overrides)).await?)
                }
                "maybe_cascade_executor_completion" => {
                    let (execution_id,): (String,) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(Box::pin(self.maybe_cascade_executor_completion(&execution_id)).await?)
                }
                "dispatch_role_follow_up" => {
                    let (task_id, role, parent_execution_id, prompt, trigger): (
                        String,
                        String,
                        String,
                        String,
                        String,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(
                        Box::pin(self.dispatch_role_follow_up(
                            &task_id,
                            &role,
                            parent_execution_id,
                            prompt,
                            &trigger,
                        ))
                        .await?,
                    )
                }
                "dispatch_recovery_role" => {
                    let (task, project, workflow, agent_id, role, action): (
                        Task,
                        db::Project,
                        api_types::WorkflowDefinition,
                        String,
                        String,
                        api_types::TaskAction,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(
                        Box::pin(self.dispatch_recovery_role(
                            &task, &project, &workflow, &agent_id, &role, &action,
                        ))
                        .await?,
                    )
                }
                "dispatch_initial_role_execution_with_metadata_and_admission" => {
                    let (task_id, agent_id, role, prompt, dispatch_metadata, admission): (
                        String,
                        String,
                        String,
                        String,
                        Option<Value>,
                        db::ExecutionAdmission,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(
                        Box::pin(
                            self.dispatch_initial_role_execution_with_metadata_and_admission(
                                &task_id,
                                &agent_id,
                                &role,
                                prompt,
                                dispatch_metadata,
                                admission,
                            ),
                        )
                        .await?,
                    )
                }
                "launch_execution" => {
                    let (task_id, agent_id, summary, overrides): (
                        String,
                        String,
                        Option<String>,
                        Option<ExecutionOverrides>,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(
                        Box::pin(self.launch_execution(task_id, agent_id, summary, overrides))
                            .await?,
                    )
                }
                "follow_up_interactive_execution" => {
                    let (parent_execution_id, message, agent_id, overrides): (
                        String,
                        String,
                        Option<String>,
                        Option<ExecutionOverrides>,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(
                        Box::pin(self.follow_up_interactive_execution(
                            parent_execution_id,
                            message,
                            agent_id,
                            overrides,
                        ))
                        .await?,
                    )
                }
                "re_execute_execution_with_context" => {
                    let (parent_execution_id, context): (String, Option<String>) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(
                        Box::pin(
                            self.re_execute_execution_with_context(parent_execution_id, context),
                        )
                        .await?,
                    )
                }
                "stop_execution" => {
                    let (execution_id, reason): (String, String) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(Box::pin(self.stop_execution(execution_id, reason)).await?)
                }
                "pause_execution" => {
                    let (execution_id, reason): (String, String) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(Box::pin(self.pause_execution(execution_id, reason)).await?)
                }
                "fail_task" => {
                    let (task_id, reason, kind, execution_id): (
                        String,
                        String,
                        Option<api_types::FailureKind>,
                        Option<String>,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(Box::pin(self.fail_task(task_id, reason, kind, execution_id)).await?)
                }
                "move_task" => {
                    let (task_id, request): (String, MoveTaskRequest) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(Box::pin(self.move_task(task_id, request)).await?)
                }
                "reassign_role" => {
                    let (input, reset_workspace, reset_worktree): (
                        CreateTaskRoleAssignment,
                        bool,
                        bool,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(
                        Box::pin(self.reassign_role(input, reset_workspace, reset_worktree))
                            .await?,
                    )
                }
                "remove_role" => {
                    let (task_id, role_name, reset_workspace, reset_worktree): (
                        String,
                        String,
                        bool,
                        bool,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(
                        Box::pin(self.remove_role(
                            &task_id,
                            &role_name,
                            reset_workspace,
                            reset_worktree,
                        ))
                        .await?,
                    )
                }
                "advance_coordination_root" => {
                    let (parent_task_id,): (String,) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;

                    encode(Box::pin(self.advance_coordination_root(&parent_task_id)).await?)
                }
                "transition" => {
                    let (task_id, new_status, mut options): (
                        String,
                        TaskStatus,
                        TransitionOptions,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    options.version = self.command_task(&task_id).await?.version;
                    encode(Box::pin(self.transition(task_id, new_status, options)).await?)
                }
                "transition_with_plan_publication" => {
                    let (task_id, new_status, mut options, execution_id): (
                        String,
                        TaskStatus,
                        TransitionOptions,
                        String,
                    ) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    options.version = self.command_task(&task_id).await?.version;
                    encode(
                        Box::pin(self.transition_with_plan_publication(
                            task_id,
                            new_status,
                            options,
                            &execution_id,
                        ))
                        .await?,
                    )
                }
                "update_task" => {
                    let (task_id, mut request, parent_present): (String, UpdateTaskRequest, bool) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    if parent_present && request.parent_task_id.is_none() {
                        request.parent_task_id = Some(None);
                    }
                    encode(Box::pin(self.update_task(task_id, request)).await?)
                }
                "rerun_review" => {
                    let (id,): (String,) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    encode(
                        Box::pin(
                            self.rerun_review(
                                Uuid::parse_str(&id)
                                    .map_err(|e| ServiceError::invalid_operation(e.to_string()))?,
                            ),
                        )
                        .await?,
                    )
                }
                "approve_review_as" => {
                    let (id, actor): (String, Actor) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    encode(Box::pin(self.approve_review_as(id, actor)).await?)
                }
                "reject_review_as" => {
                    let (id, reason, actor): (String, Option<String>, Actor) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    encode(Box::pin(self.reject_review_as(id, reason, actor)).await?)
                }
                "create_subtasks" => {
                    let (id, items): (String, Vec<NewSubtaskInput>) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    encode(Box::pin(self.create_subtasks(id, items)).await?)
                }
                "persist_manual_stop_annotation" => {
                    let (
                        execution,
                        task,
                        status,
                        token,
                        role,
                        definition,
                        assignment,
                        annotation,
                        updated_at,
                    ): ManualStopArguments = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    encode(
                        Box::pin(self.persist_manual_stop_annotation(
                            &execution,
                            &task,
                            &status,
                            token,
                            role.as_deref(),
                            &definition,
                            assignment,
                            annotation,
                            updated_at,
                        ))
                        .await?,
                    )
                }
                "recover_task_after_restart" => {
                    let (id,): (String,) = serde_json::from_value(command.arguments.clone())
                        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    let task = self.command_task(&id).await?;
                    let _ = crate::recovery::recover_task(
                        &self.db,
                        task,
                        db::StopReason::CrashRecovery,
                        &Actor::system(api_types::SystemComponent::CrashRecovery),
                    )
                    .await?;
                    encode(())
                }
                "block_cancelled_dependencies" => {
                    let (task_id, cancelled): (String, Vec<String>) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    let task = self.command_task(&task_id).await?;
                    encode(Box::pin(self.block_cancelled_dependencies(&task, &cancelled)).await?)
                }
                "assign_agent_to_task" => {
                    let (task_id, agent_id): (String, String) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    encode(Box::pin(self.assign_agent_to_task(&task_id, &agent_id)).await?)
                }
                "execute_adaptive_task_command" => {
                    let (input,): (super::adaptive::AdaptiveTaskCommand,) =
                        serde_json::from_value(command.arguments.clone())
                            .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
                    encode(Box::pin(self.execute_adaptive_task_command(input)).await?)
                }
                _ => Err(ServiceError::invalid_operation("unknown Task command")),
            }
        })
    }
}
