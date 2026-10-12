//! `Step::ReplanRoot`: one attempt to send a coordination root back to its
//! planning state for a parked subtask, quiescing on a deterministic refusal
//! exactly as the root advance does.
use db::{DbError, Task};

use super::{helpers, TaskDispatcher};
use crate::{deferred_dispatch, Result, ServiceError};

/// The disposition capability of a refused re-plan.
pub(super) const COORDINATION_ROOT_REPLAN_CAPABILITY: &str = "coordination_root_replan";

impl TaskDispatcher {
    pub(super) async fn replan_coordination_root_once(
        &self,
        task: &Task,
        child_id: &str,
        park: &str,
    ) -> Result<u64> {
        if deferred_dispatch::dispatch_disposition_is_current(
            task,
            COORDINATION_ROOT_REPLAN_CAPABILITY,
        ) {
            return Ok(0);
        }
        match self
            .task_service
            .replan_coordination_root(&task.id, child_id, park)
            .await
        {
            Ok(written) => Ok(u64::from(written)),
            Err(ServiceError::Db(DbError::VersionConflict)) => {
                tracing::debug!(task_id = %task.id, "coordination-root re-plan lost version race");
                Ok(0)
            }
            Err(error) if helpers::is_deterministic_dispatch_refusal(&error) => {
                deferred_dispatch::record_dispatch_disposition(
                    &self.db,
                    task,
                    COORDINATION_ROOT_REPLAN_CAPABILITY,
                    &error.to_string(),
                )
                .await?;
                tracing::warn!(
                    task_id = %task.id,
                    %error,
                    "coordination-root re-plan parked until Task state changes or an explicit wake"
                );
                Ok(0)
            }
            Err(error) => {
                tracing::warn!(task_id = %task.id, %error, "coordination-root re-plan remains pending");
                Ok(0)
            }
        }
    }
}
