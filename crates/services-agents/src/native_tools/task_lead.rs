//! Native bindings of the lead operations. One adapter (`lead_run`) derives
//! the lead's run from the admitted principal and the running execution;
//! every handler takes it from there and hands the target to the one command
//! boundary (`TaskService::bind_lead`). No handler reads a root, a parent or
//! an actor from its input.
use super::*;
use operation_registry::task_lead::{
    Handoff, Read, Recover, RequestInput, Subtask, TaskLeadContext,
};

impl registered_reads::Context<'_> {
    /// The run this lead operation is issued by: the session's identity and
    /// Task, its one running execution, and the root the server says that
    /// execution leads now.
    async fn lead_run(
        &self,
    ) -> Result<(Arc<crate::TaskService>, crate::LeadRun, String), AgentHostError> {
        let runtime = self.runtime_session_id.expect("proposal session");
        let authority = match self.admitted_authority {
            Some(authority) => authority.clone(),
            None => self
                .provider
                .db
                .resolve_task_effective_authority(
                    self.actor_identity_id,
                    None,
                    &self.scope.scope_id,
                    workspace_access_name(self.scope.workspace_access),
                    runtime,
                )
                .await
                .map_err(|_| AgentHostError::ProtectedPersistence)?,
        };
        let operation_registry::authority::Principal::TaskAgent {
            execution_id,
            lead_root_id: Some(root_id),
            ..
        } = authority.principal
        else {
            return Err(AgentHostError::Authority(
                "this run does not lead a coordination root".to_owned(),
            ));
        };
        let service = self.provider.task_service_handle().ok_or_else(|| {
            AgentHostError::Configuration("lead operations are not wired".to_owned())
        })?;
        let key = required_argument(
            self.proposal_arguments.expect("proposal envelope"),
            "dedupe_key",
        )?;
        Ok((
            service,
            crate::LeadRun {
                identity_id: self.actor_identity_id.to_owned(),
                execution_id,
                task_id: self.scope.scope_id.clone(),
                root_id,
            },
            key,
        ))
    }
}

/// A refusal is an ordinary tool result the lead reads: the typed reason is
/// in `details.refusal`, and the call is not to be repeated unchanged.
fn lead_error(operation: &str, error: crate::ServiceError) -> AgentHostError {
    match error {
        crate::ServiceError::LeadOperationRefused { refusal, message } => {
            let mut outcome = OrchestrationOutcome::failed(
                OutcomeCode::ValidationError,
                operation,
                OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
                "",
                message,
            );
            outcome.details = Some(json!({ "refusal": refusal.code() }));
            AgentHostError::StructuredOutcome(Box::new(outcome))
        }
        error => service_error(error),
    }
}

#[async_trait]
impl TaskLeadContext<AgentHostError> for registered_reads::Context<'_> {
    async fn lead_read(&self, input: Read) -> Result<Value, AgentHostError> {
        let (service, run, _) = self.lead_run().await?;
        service
            .lead_read(&run, input.task_id.as_deref())
            .await
            .map_err(|error| lead_error("lead.read", error))
    }
    async fn lead_subtask(&self, input: Subtask) -> Result<Value, AgentHostError> {
        let (service, run, key) = self.lead_run().await?;
        match input {
            Subtask::Create {
                title,
                description,
                assignee_id,
            } => {
                service
                    .lead_create_subtask(&run, title, description, assignee_id, &key)
                    .await
            }
            Subtask::Update {
                task_id,
                version,
                title,
                description,
            } => {
                service
                    .lead_update_subtask(&run, &task_id, version, title, description, &key)
                    .await
            }
            Subtask::Cancel {
                task_id,
                version,
                reason,
            } => {
                service
                    .lead_cancel_subtask(&run, &task_id, version, reason)
                    .await
            }
            Subtask::Reorder { ordered_task_ids } => {
                service
                    .lead_reorder_subtasks(&run, ordered_task_ids, &key)
                    .await
            }
            Subtask::Assign { task_id, agent_id } => {
                service
                    .lead_assign_worker(&run, task_id.as_deref(), &agent_id)
                    .await
            }
        }
        .map_err(|error| lead_error("lead.subtask", error))
    }
    async fn lead_recover(&self, input: Recover) -> Result<Value, AgentHostError> {
        let (service, run, key) = self.lead_run().await?;
        service
            .lead_recover_subtask(&run, &input.task_id, input.action, input.version, &key)
            .await
            .map_err(|error| lead_error("lead.recover", error))
    }
    async fn lead_handoff(&self, input: Handoff) -> Result<Value, AgentHostError> {
        let (service, run, key) = self.lead_run().await?;
        service
            .lead_handoff(&run, &input.task_id, &input.note, &key)
            .await
            .map_err(|error| lead_error("lead.handoff", error))
    }
    async fn lead_request_input(&self, input: RequestInput) -> Result<Value, AgentHostError> {
        let (service, run, key) = self.lead_run().await?;
        service
            .lead_request_input(&run, &input.question, &key)
            .await
            .map_err(|error| lead_error("lead.request_input", error))
    }
}
