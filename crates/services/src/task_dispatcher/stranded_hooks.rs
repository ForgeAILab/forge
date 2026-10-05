//! Startup sweep for integration entries whose post-commit hooks were lost.
//!
//! Before durable hook steps, `merging` ran `run_merge` inline and a
//! two-minute re-drive retried an entry whose merge was interrupted. A Task
//! left in `merging` by such a binary has no hooks row after the upgrade and
//! nothing else re-drives it. The sweep is limited to states whose `on_enter`
//! runs `run_merge`: the merge proves completion durably (the target already
//! contains the candidate), so re-running it is safe. Other states have no
//! such witness, and a missing hooks row there is the normal shape of an
//! upgraded Task whose inline hooks finished, or of a settled row that
//! storage maintenance pruned; re-running their hooks would re-dispatch agents
//! and re-run checks.

use super::{helpers, TaskDispatcher};
use crate::{workflow::engine::WorkflowEngine, Result};

/// Root Tasks of one Project in one of its merge states, in their current
/// entry with no unfinished step and no hooks row for that entry (matched on
/// status and status epoch, as the step fence is). Subtasks never merge.
const STRANDED_ENTRIES: &str = "SELECT t.id, t.status_epoch FROM task t
    WHERE t.project_id = ? AND t.status IN (SELECT value FROM json_each(?))
      AND t.parent_task_id IS NULL AND t.deleted_at IS NULL AND t.archived_at IS NULL
      AND NOT EXISTS (SELECT 1 FROM task_step s WHERE s.task_id = t.id AND s.status IN ('pending','claimed'))
      AND NOT EXISTS (SELECT 1 FROM task_step s WHERE s.task_id = t.id AND s.kind = 'hooks'
                      AND s.expected_status = t.status AND s.expected_epoch = t.status_epoch)
    ORDER BY t.created_at, t.id";

impl TaskDispatcher {
    /// Runs once on every start, before the step worker and the dispatcher
    /// loop. Idempotent: a recovered row is itself the current entry's hooks
    /// row, and its causation key is unique per entry.
    pub async fn recover_stranded_hook_entries(&self) -> Result<u64> {
        let mut recovered = 0;
        let engine = self.task_service.workflow_execution();
        for project in self.list_projects().await? {
            let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
            let merge_states = workflow
                .states
                .iter()
                .filter(|state| state.hooks.on_enter.iter().any(|h| h.action == "run_merge"))
                .map(|state| state.name.clone())
                .collect::<Vec<_>>();
            if merge_states.is_empty() {
                continue;
            }
            let candidates: Vec<(String, i64)> = sqlx::query_as(STRANDED_ENTRIES)
                .bind(&project.id)
                .bind(serde_json::to_string(&merge_states).expect("state names serialize"))
                .fetch_all(self.db.pool())
                .await?;
            for (task_id, status_epoch) in candidates {
                let Some(task) = db::TaskRepo::get_by_id(&*self.db, &task_id, false).await? else {
                    continue;
                };
                // Paused, held, or waiting on a human: the owner's recovery
                // action owns these. A paused-integration marker has its own
                // resume in active recovery.
                if helpers::has_blocking_annotation(&task)
                    || helpers::awaiting_human(&task)
                    || crate::deferred_dispatch::paused_integration(&task).is_some()
                    || task.entry_barrier_json.is_some()
                {
                    continue;
                }
                match engine
                    .enqueue_recovered_entry_hooks(&task, status_epoch, &project, &workflow)
                    .await
                {
                    Ok(Some(step_id)) => {
                        tracing::info!(
                            task_id = %task.id,
                            status = %task.status,
                            %step_id,
                            "re-enqueued post-commit hooks for an entry that had none"
                        );
                        recovered += 1;
                    }
                    Ok(None) => tracing::warn!(
                        task_id = %task.id,
                        status = %task.status,
                        "stranded entry has no identifiable transition; owner retry required"
                    ),
                    Err(error) => tracing::warn!(
                        task_id = %task.id,
                        status = %task.status,
                        %error,
                        "failed to re-enqueue post-commit hooks for a stranded entry"
                    ),
                }
            }
        }
        Ok(recovered)
    }
}
