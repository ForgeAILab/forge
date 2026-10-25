//! Startup execution and session recovery.

use super::*;

impl CrashRecovery {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self { db, event_bus }
    }

    /// Remove the per-run temp directories dead runs left in server-owned
    /// Task roots of the managed shape. Hooks, checks and tool commands are
    /// in no table, and a run the previous process left detached (a
    /// protected push, a check still settling) can still be alive after the
    /// restart, so being older than this process condemns nothing: the sweep
    /// ([`executors::sandbox::sweep_stale_runs`]) takes only a directory
    /// older than the longest run. An execution still recorded as running
    /// keeps its directory and loses it when it settles.
    async fn sweep_dead_run_temp_dirs(&self) -> Result<()> {
        let live: Vec<String> = self.db.running_execution_ids_for_temp_sweep().await?;
        let mut task_root_dirs = HashSet::new();
        for placement in
            WorkspacePlacementRepo::list_by_state(&*self.db, PlacementState::Ready).await?
        {
            // Only the managed shape `<root>/<task_id>/<name>`: nothing is
            // ever swept beside a directory Forge does not own.
            if let Some(dir) = crate::workspace_manager::task_root_anchor_of(&placement)
                .ok()
                .and_then(|worktree| {
                    let task_root = worktree.parent()?;
                    (task_root.file_name()?.to_str()? == placement.task_id)
                        .then(|| task_root.parent().map(std::path::Path::to_path_buf))?
                })
            {
                task_root_dirs.insert(dir);
            }
        }
        let swept = tokio::task::spawn_blocking(move || {
            task_root_dirs
                .iter()
                .map(|dir| {
                    executors::sandbox::sweep_stale_runs(
                        dir,
                        live.iter().map(String::as_str),
                        std::time::SystemTime::now(),
                        executors::gc::MAX_RUN_AGE,
                    )
                })
                .sum::<usize>()
        })
        .await
        .unwrap_or(0);
        if swept > 0 {
            tracing::info!(
                swept,
                "removed temp directories of runs that did not settle"
            );
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn run(&self) -> Result<u64> {
        self.run_recovery().await
    }

    #[tracing::instrument(skip(self))]
    pub async fn run_recovery(&self) -> Result<u64> {
        // Attempt receipts are bookkeeping; a failure to settle them must not
        // stop crash recovery of executions, grants and Tasks. An intent left
        // here is settled by the next admission on its checkout.
        if let Err(error) = crate::integration_owner::ServerIntegrationOwner::new(self.db.clone())
            .reconcile_outstanding()
            .await
        {
            tracing::warn!(target: "services::recovery", %error, "integration attempt reconciliation failed at startup");
        }
        // A server restart loses its sockets, not the CLI running on an owner.
        // Persist that suspension before either execution or grant recovery.
        for placement in
            WorkspacePlacementRepo::list_by_state(&*self.db, PlacementState::Ready).await?
        {
            if let Some(daemon_id) = placement_execution_daemon_id(&placement) {
                if placement.owner_kind == PlacementOwnerKind::Daemon
                    || DaemonRepo::get_by_id(&*self.db, daemon_id)
                        .await?
                        .is_some_and(|daemon| !is_embedded_daemon_machine(&daemon.machine_id))
                {
                    disconnect_daemon_placements(&self.db, &self.event_bus, daemon_id).await?;
                }
            }
        }
        // Housekeeping only: a failure here must not stop recovery.
        if let Err(error) = self.sweep_dead_run_temp_dirs().await {
            tracing::warn!(%error, "could not sweep temp directories of dead runs");
        }
        // Expire stale grants before recovering active Tasks. The recovery
        // pass below then sees the still-running attempt and requeues/blocks
        // it through the normal crash-recovery state machine.
        expire_workspace_leases(&self.db, &self.event_bus, None, None, false).await?;

        // A user cancellation can commit the execution terminal CAS and move
        // still-running local provider calls to pending settlement immediately
        // before the process exits. There is no daemon to replay those calls
        // after restart, so repair that durable ambiguity before recovering
        // Tasks. Daemon-owned invocations remain pending: their terminal
        // report is replayable across a reconnect.
        let unsettled = reconcile_unreplayable_local_usage_invocations(&self.db).await?;
        if unsettled > 0 {
            tracing::info!(
                unsettled,
                "marked unreplayable local usage invocations unsettled during crash recovery"
            );
        }

        // Native (in-process) runtime sessions cannot survive a restart, so
        // any 'starting'/'ready'/'running'/'degraded' native session left by
        // a previous process lies about liveness. Suspend them: the reuse
        // path (get_active_agent_session) then re-establishes a fresh session
        // on next use. That session rebuilds its history from the chat
        // transcript and retires the old runtime's LCM timeline on first bind.
        let suspended_sessions =
            AgentSessionRepo::suspend_stale_native_sessions(&*self.db, &now_rfc3339()).await?;
        if suspended_sessions > 0 {
            tracing::info!(
                suspended_sessions,
                "suspended stale native agent sessions from previous process"
            );
        }
        // Driven by the running executions, not by the Tasks: a start with
        // nothing to recover reads one empty list and writes nothing,
        // however many Tasks the database holds.
        let tasks = tasks_with_running_executions(&self.db).await?;
        let mut recovered = 0;

        for (task, executions) in tasks {
            // One live recovery command per Task and running-execution set:
            // repeated restarts before the queue drains do not stack copies.
            // The retired inline review owner has its own startup settlement
            // below, which explains the stop and re-enters review. Do not let
            // a generic gate recovery consume that owner's executions first.
            if executions
                .iter()
                .any(crate::task_service::retired_review_rerun)
            {
                continue;
            }
            let mut running = executions
                .into_iter()
                .map(|execution| execution.id)
                .collect::<Vec<_>>();
            if running.is_empty() {
                continue;
            }
            running.sort();
            let key = format!("recover_task_after_restart:{}", running.join(","));
            let prefix = format!("{key}:");
            let queued: bool = self
                .db
                .pending_recovery_command_exists(&task.id, &prefix, &prefix)
                .await?;
            if queued {
                continue;
            }
            let id = db::new_uuid_v4();
            db::TaskStepRepo::enqueue_step(
                &*self.db,
                &db::EnqueueTaskStep {
                    id: id.clone(),
                    task_id: task.id.clone(),
                    kind: "command".into(),
                    payload_json: crate::task_service::commands::TaskCommand {
                        operation: "recover_task_after_restart".into(),
                        arguments: json!([task.id]),
                        preempt: false,
                    }
                    .payload_json()?,
                    causation_step_id: None,
                    causation_key: format!("{key}:{id}"),
                    chain_id: id,
                    chain_position: 1,
                    expected_status: task.status,
                    expected_version: task.version,
                    expected_epoch: None,
                    lane: "fast".into(),
                    available_at: now_rfc3339(),
                },
            )
            .await?;
        }

        // The inline review re-run of earlier versions left reviewer and
        // auditor executions `running` under leases only that pipeline
        // renewed. Nothing runs them now: stop them at start, whatever their
        // lease says, and let each Task's review entry run again.
        let leftover_tasks: Vec<String> = self.db.running_retired_review_task_ids().await?;
        for task_id in leftover_tasks {
            let mut leftovers = ExecutionRepo::list_running_by_task(&*self.db, &task_id)
                .await?
                .into_iter()
                .filter(crate::task_service::retired_review_rerun)
                .map(|execution| execution.id)
                .collect::<Vec<_>>();
            if leftovers.is_empty() {
                continue;
            }
            leftovers.sort();
            let Some(task) = TaskRepo::get_by_id(&*self.db, &task_id, false).await? else {
                continue;
            };
            let key = format!("settle_retired_review_rerun:{}", leftovers.join(","));
            let prefix = format!("{key}:");
            let queued: bool = self
                .db
                .pending_retired_review_settlement_exists(&task.id, &prefix, &prefix)
                .await?;
            if queued {
                continue;
            }
            let id = db::new_uuid_v4();
            db::TaskStepRepo::enqueue_step(
                &*self.db,
                &db::EnqueueTaskStep {
                    id: id.clone(),
                    task_id: task.id.clone(),
                    kind: "command".into(),
                    payload_json: crate::task_service::commands::TaskCommand {
                        operation: "settle_retired_review_rerun".into(),
                        arguments: json!([task.id]),
                        preempt: false,
                    }
                    .payload_json()?,
                    causation_step_id: None,
                    causation_key: format!("{key}:{id}"),
                    chain_id: id,
                    chain_position: 1,
                    expected_status: task.status,
                    expected_version: task.version,
                    expected_epoch: None,
                    lane: "fast".into(),
                    available_at: now_rfc3339(),
                },
            )
            .await?;
        }

        recovered += sweep_stale_recovery_annotations(&self.db).await?;

        // A Task configured before v0.14 may still name a review auditor,
        // which nothing runs any more: its owner is told on the Task.
        if let Err(error) = crate::task_service::state_retired_auditor_notices(&self.db).await {
            tracing::warn!(%error, "retired auditor notices could not be stated");
        }

        tracing::info!(recovered_tasks = recovered, "crash recovery completed");
        Ok(recovered)
    }
}

async fn reconcile_unreplayable_local_usage_invocations(db: &SqliteDb) -> Result<u64> {
    let invocations = UsageLedgerRepo::list_usage_invocations_needing_settlement(db, 5000).await?;
    let mut marked = 0_u64;
    for invocation in invocations {
        let Some(execution_id) = invocation.execution_id.as_deref() else {
            continue;
        };
        let Some(execution) = ExecutionRepo::get_by_id(db, execution_id).await? else {
            continue;
        };
        if execution.status == ExecutionStatus::Running {
            continue;
        }
        let previous_owner = db
            .terminal_execution_previous_owner(&execution.task_id, &execution.id)
            .await?
            .flatten();
        if previous_owner
            .as_deref()
            .is_some_and(|owner| owner.starts_with("daemon:"))
        {
            continue;
        }
        match UsageLedgerRepo::mark_usage_invocation_unsettled(
            db,
            MarkUsageInvocationUnsettled {
                id: invocation.id,
                expected_version: invocation.version,
                terminal_reason: "recovery_no_replayable_result".to_owned(),
                settled_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        {
            Ok(_) => marked += 1,
            Err(db::DbError::VersionConflict | db::DbError::IdempotencyConflict) => {
                // A late result may have settled the invocation while this
                // repair sweep was reading it. The newer lifecycle wins.
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(marked)
}
