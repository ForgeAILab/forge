//! Heartbeat placement suspension and reconciliation workers.

use super::*;

impl HeartbeatMonitor {
    pub(super) async fn suspend_unreachable_placements(&self) -> Result<()> {
        let Some(registry) = &self.daemon_connections else {
            return Ok(());
        };
        let running = ExecutionRepo::list_running(&*self.db).await?;
        let mut owners = HashSet::new();
        for placement in
            WorkspacePlacementRepo::list_by_state(&*self.db, PlacementState::Ready).await?
        {
            let Some(daemon_id) = placement_execution_daemon_id(&placement) else {
                continue;
            };
            let Some(daemon) = DaemonRepo::get_by_id(&*self.db, daemon_id).await? else {
                continue;
            };
            if placement.owner_kind == PlacementOwnerKind::Server
                && is_embedded_daemon_machine(&daemon.machine_id)
            {
                continue;
            }
            let owner_changed = registry.get(daemon_id).is_some_and(|connection| {
                let owner =
                    crate::daemon_transport::execution_lease_owner(daemon_id, connection.id());
                running.iter().any(|execution| {
                    execution.workspace_id.as_deref() == Some(&placement.workspace_id)
                        && execution.lease_owner.as_deref() != Some(owner.as_str())
                })
            });
            if daemon.status != db::DaemonStatus::Online
                || !registry.get(daemon_id).is_some_and(|connection| {
                    !connection.is_stale() && connection.protocol_allows_dispatch()
                })
                || owner_changed
            {
                owners.insert(daemon_id.to_owned());
            }
        }
        for daemon_id in owners {
            if let Err(error) =
                disconnect_daemon_placements(&self.db, &self.event_bus, &daemon_id).await
            {
                tracing::warn!(%daemon_id, %error, "owner suspension remains pending");
            }
        }
        Ok(())
    }

    pub(super) async fn check_workspace_placements(&self) -> Result<u64> {
        let mut settled = 0;
        // Expiry/repair is local and bounded by rows, never by an owner RPC.
        for state in [PlacementState::Disconnected, PlacementState::Failed] {
            for placement in WorkspacePlacementRepo::list_by_state(&*self.db, state).await? {
                let attempt = async {
                    if placement.state == PlacementState::Disconnected {
                        record_disconnected_attention(&self.db, &placement).await?;
                        repair_owner_recovery(&self.db, &placement).await?;
                        let expired = placement
                            .disconnected_at
                            .as_deref()
                            .and_then(|at| DateTime::parse_from_rfc3339(at).ok())
                            .and_then(|at| Utc::now().signed_duration_since(at).to_std().ok())
                            .is_some_and(|elapsed| elapsed >= self.max_disconnect);
                        if !expired {
                            return Ok(0);
                        }
                        let failed = WorkspacePlacementRepo::update(
                            &*self.db,
                            placement_update(
                                &placement,
                                PlacementState::Failed,
                                Some(PlacementFailureCause::OwnerDisconnectedTimeout),
                            ),
                        )
                        .await?;
                        return fail_placement_executions(
                            &self.db,
                            &self.event_bus,
                            &failed,
                            PlacementFailureCause::OwnerDisconnectedTimeout,
                        )
                        .await;
                    }
                    if placement.failure_cause
                        == Some(PlacementFailureCause::OwnerDisconnectedTimeout)
                    {
                        let count = fail_placement_executions(
                            &self.db,
                            &self.event_bus,
                            &placement,
                            PlacementFailureCause::OwnerDisconnectedTimeout,
                        )
                        .await?;
                        repair_owner_recovery(&self.db, &placement).await?;
                        return Ok(count);
                    }
                    Ok::<u64, ServiceError>(0)
                }
                .await;
                match attempt {
                    Ok(count) => settled += count,
                    Err(ServiceError::Db(db::DbError::VersionConflict)) => {}
                    Err(error) => tracing::warn!(placement_id = %placement.id,
                        daemon_id = ?placement_execution_daemon_id(&placement), %error, "placement maintenance remains pending"),
                }
            }
        }
        let Some(registry) = &self.daemon_connections else {
            return Ok(settled);
        };
        let upgraded_daemons: Vec<_> = registry
            .connection_snapshots()
            .into_iter()
            .filter(|(id, _)| {
                registry.get(id).is_some_and(|connection| {
                    !connection.is_stale() && connection.protocol_allows_dispatch()
                })
            })
            .map(|(id, _)| id)
            .collect();
        if !upgraded_daemons.is_empty() {
            if let Err(error) =
                crate::workflow::engine::wake_upgraded_daemon_tasks(&self.db, &upgraded_daemons)
                    .await
            {
                tracing::warn!(%error, "daemon upgrade dispatch wake failed");
            }
        }
        let retained = serde_json::to_string(&registry.retained_terminal_execution_ids())
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let recent = (Utc::now() - ChronoDuration::seconds(60)).to_rfc3339();
        let ready_to_retry: HashSet<String> = self
            .db
            .ready_owner_retry_placement_ids(retained, recent)
            .await?
            .into_iter()
            .collect();
        let mut groups: HashMap<String, Vec<String>> = HashMap::new();
        for state in [PlacementState::Disconnected, PlacementState::Ready] {
            for placement in WorkspacePlacementRepo::list_by_state(&*self.db, state).await? {
                if placement.state == PlacementState::Ready
                    && !ready_to_retry.contains(&placement.id)
                {
                    continue;
                }
                if let Some(daemon_id) = placement_execution_daemon_id(&placement) {
                    groups
                        .entry(daemon_id.to_owned())
                        .or_default()
                        .push(placement.id);
                }
            }
        }
        for (daemon_id, facts) in registry.connection_snapshots() {
            if !facts.workspace_incapable {
                groups.entry(daemon_id).or_default();
            }
        }
        for (daemon_id, ids) in groups {
            if !registry.get(&daemon_id).is_some_and(|connection| {
                !connection.is_stale() && connection.protocol_allows_dispatch()
            }) {
                continue;
            }
            let mut keys = vec![format!("daemon:{daemon_id}")];
            keys.extend(ids.iter().map(|id| format!("placement:{id}")));
            {
                let mut in_flight = self
                    .placement_in_flight
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if keys.iter().any(|key| in_flight.contains(key)) {
                    continue;
                }
                in_flight.extend(keys.iter().cloned());
            }
            let guard = PlacementAttemptGuard {
                in_flight: Arc::clone(&self.placement_in_flight),
                keys,
            };
            let workers = Arc::clone(&self.placement_workers);
            let db = Arc::clone(&self.db);
            let event_bus = Arc::clone(&self.event_bus);
            let registry = Arc::clone(registry);
            let task_service = self.task_service.clone();
            let mut handles = self
                .placement_worker_handles
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            handles.retain(|handle| !handle.is_finished());
            if self.is_stopped() {
                continue;
            }
            handles.push(tokio::spawn(async move {
                let _guard = guard;
                let Ok(_permit) = workers.acquire_owned().await else {
                    return;
                };
                // Every RPC has its own transport deadline. No cancelling
                // aggregate timeout surrounds database/terminal/cascade work.
                if let Err(error) = reconcile_owner_receipts(&db, &registry, &daemon_id).await {
                    tracing::warn!(%daemon_id, placement_ids = ?ids, %error, "owner receipts remain pending");
                    if matches!(error, ServiceError::DaemonTimeout { .. } | ServiceError::DaemonUnavailable { .. }) { return; }
                }
                for id in &ids {
                    let attempt = async {
                        let Some(placement) = WorkspacePlacementRepo::get_by_id(&*db, id).await?
                        else {
                            return Ok(());
                        };
                        let reconciled = if placement.state == PlacementState::Disconnected {
                            reconcile_workspace_placement(
                                &db,
                                &event_bus,
                                &registry,
                                task_service.as_deref(),
                                &placement,
                            )
                            .await?
                        } else {
                            false
                        };
                        if !reconciled {
                            if let Some(ready) = WorkspacePlacementRepo::get_by_id(&*db, id)
                                .await?
                                .filter(|placement| placement.state == PlacementState::Ready)
                            {
                                finish_reconciled_cascades(
                                    &db,
                                    &registry,
                                    task_service.as_deref(),
                                    &ready,
                                )
                                .await?;
                            }
                        }
                        Ok::<(), ServiceError>(())
                    }
                    .await;
                    if let Err(error) = attempt {
                        if matches!(error, ServiceError::WorkspaceResetRequired { .. }) {
                            tracing::debug!(placement_id = %id, %daemon_id, %error, "workspace reset remains pending");
                        } else {
                            tracing::warn!(placement_id = %id, %daemon_id, %error, "workspace reconciliation remains pending");
                        }
                        if matches!(error, ServiceError::DaemonTimeout { .. } | ServiceError::DaemonUnavailable { .. }) { return; }
                    }
                }
                tokio::spawn(async move {
                    if let Err(error) = registry.retry_retained_terminals(&daemon_id).await {
                        tracing::warn!(%daemon_id, placement_ids = ?ids, %error, "terminal acknowledgement remains pending");
                    }
                });
            }));
        }
        Ok(settled)
    }
}
