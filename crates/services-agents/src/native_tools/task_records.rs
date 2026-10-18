//! Typed Task record bindings; the current execution supplies durable provenance.
use super::*;
use operation_registry::task_records::{Evidence, Plan, TaskRecordContext, Worklog};

#[async_trait]
impl TaskRecordContext<AgentHostError> for registered_reads::Context<'_> {
    async fn task_plan(&self, input: Plan) -> Result<Value, AgentHostError> {
        self.provider
            .execute_task_plan_write(
                self.actor_identity_id,
                self.runtime_session_id.expect("proposal session"),
                self.scope,
                &input,
            )
            .await
    }
    async fn task_worklog(&self, input: Worklog) -> Result<Value, AgentHostError> {
        self.provider
            .execute_task_worklog_append(
                self.actor_identity_id,
                self.runtime_session_id.expect("proposal session"),
                self.scope,
                self.proposal_arguments.expect("proposal envelope"),
                &input,
            )
            .await
    }
    async fn task_evidence(&self, input: Evidence) -> Result<Value, AgentHostError> {
        self.provider
            .execute_task_evidence_capture(
                self.actor_identity_id,
                self.runtime_session_id.expect("proposal session"),
                self.scope,
                &input,
            )
            .await
    }
}
