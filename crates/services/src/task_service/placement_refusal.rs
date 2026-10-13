use super::*;
use sha2::{Digest, Sha256};

impl TaskService {
    async fn placement_eligibility_key(&self, task: &Task) -> Result<String> {
        let project_id = &task.project_id;
        let assignments: Vec<(String, Option<String>, Option<String>, String)> = self
            .db
            .placement_eligibility_assignments(
                &task.id,
                task.parent_task_id.as_deref().unwrap_or(&task.id),
            )
            .await?
            .into_iter()
            .map(|row| {
                (
                    row.role_name,
                    row.assignee_type,
                    row.assignee_id,
                    row.updated_at,
                )
            })
            .collect();
        // Heartbeat timestamps are intentionally excluded: only eligibility
        // facts can wake a deterministic refusal.
        let locations: Vec<(String, i64, String, String)> = self
            .db
            .placement_eligibility_locations(project_id)
            .await?
            .into_iter()
            .map(|row| (row.id, row.version, row.status, row.runtime_status))
            .collect();
        let retries: Vec<(String, i64, String, i64)> = self
            .db
            .placement_eligibility_provision_retries(project_id)
            .await?
            .into_iter()
            .map(|row| {
                (
                    row.runtime_id,
                    row.attempts,
                    row.checks_digest,
                    row.connection_id,
                )
            })
            .collect();
        let machines: Vec<(String, String, String)> =
            self.db.placement_eligibility_machines().await?;
        let profiles: Vec<(String, i64, String)> =
            self.db.placement_eligibility_selected_profiles().await?;
        let profile_versions: Vec<(String, i64)> =
            self.db.placement_eligibility_profile_versions().await?;
        let credentials: Vec<(String, String, bool)> =
            self.db.placement_eligibility_credentials().await?;
        let project_version: i64 = self
            .db
            .placement_eligibility_project_version(project_id)
            .await?;
        let policies: Vec<(String, String, bool)> =
            self.db.placement_eligibility_cli_policies().await?;
        let mut handshakes: Vec<_> = self
            .daemon_connections
            .as_ref()
            .map(|registry| {
                registry
                    .connection_snapshots()
                    .into_iter()
                    .map(|(id, facts)| (id, facts.connection_id, facts.handshake))
                    .collect()
            })
            .unwrap_or_default();
        handshakes.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(hex::encode(Sha256::digest(
            serde_json::to_vec(&(
                locations,
                retries,
                assignments,
                (
                    &task.task_type,
                    &task.task_state_config,
                    &task.assignee_type,
                    &task.assignee_id,
                ),
                machines,
                profiles,
                profile_versions,
                credentials,
                project_version,
                policies,
                handshakes,
            ))
            .expect("eligibility facts serialize"),
        )))
    }

    pub(crate) async fn record_placement_dispatch_refusal(
        &self,
        task: &Task,
        error: &ServiceError,
    ) -> Result<bool> {
        let ServiceError::PlacementUnavailable(refusal) = error else {
            return Ok(false);
        };
        if refusal.needs_daemon_upgrade() || !refusal.is_deterministic() {
            return Ok(false);
        }
        let mut machines = Vec::new();
        let mut capabilities = Vec::new();
        for candidate in &refusal.rejected_candidates {
            if let Some(id) = &candidate.daemon_id {
                let machine: Option<String> = self.db.placement_refusal_machine_name(id).await?;
                machines.push(serde_json::json!({"daemon_id":id,"runtime_id":candidate.runtime_id,"machine":machine.unwrap_or_else(|| id.clone()),"filter_codes":candidate.filter_codes}));
                if candidate
                    .filter_codes
                    .contains(&crate::placement::PlacementFilterCode::CapabilityMissing)
                    && self
                        .daemon_connections
                        .as_ref()
                        .and_then(|registry| registry.get(id))
                        .and_then(|connection| connection.snapshot())
                        .is_some_and(|facts| {
                            !facts
                                .handshake
                                .capabilities
                                .iter()
                                .any(|cap| cap == api_types::DAEMON_CAPABILITY_PLAN_TRANSPORT)
                        })
                {
                    capabilities.push(api_types::DAEMON_CAPABILITY_PLAN_TRANSPORT);
                }
            }
        }
        capabilities.sort_unstable();
        capabilities.dedup();
        let names = machines
            .iter()
            .filter_map(|machine| machine["machine"].as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let unverified = refusal.rejected_candidates.iter().any(|candidate| {
            candidate
                .filter_codes
                .contains(&crate::placement::PlacementFilterCode::EnvironmentUnverified)
        });
        let failed = refusal.rejected_candidates.iter().any(|candidate| {
            candidate
                .filter_codes
                .contains(&crate::placement::PlacementFilterCode::ProvisionFailed)
        });
        let mut failures = Vec::new();
        if failed {
            let remote: Option<String> = self
                .db
                .placement_refusal_repo_remote_url(&refusal.repo_id)
                .await?
                .flatten();
            for machine in &mut machines {
                if machine["filter_codes"]
                    .as_array()
                    .is_some_and(|codes| codes.iter().any(|code| code == "provision_failed"))
                {
                    let candidate = refusal
                        .rejected_candidates
                        .iter()
                        .find(|candidate| {
                            candidate.daemon_id.as_deref() == machine["daemon_id"].as_str()
                                && candidate.runtime_id.as_deref() == machine["runtime_id"].as_str()
                        })
                        .expect("failed machine");
                    let error: Option<String> = self
                        .db
                        .placement_refusal_provision_error(&refusal.repo_id, &candidate.runtime_id)
                        .await?
                        .flatten();
                    let error = crate::project_environment::bounded_output_tail(
                        &git::redact_remote_credentials(
                            error
                                .as_deref()
                                .unwrap_or("provisioning retry limit reached without a reply"),
                            remote.as_deref().unwrap_or_default(),
                        ),
                    );
                    machine["last_error"] = serde_json::json!(error);
                    failures.push(format!(
                        "{}: {error}",
                        machine["machine"].as_str().unwrap_or("daemon machine")
                    ));
                }
            }
        }
        let message = if failed {
            format!(
                "Provisioning failed on {}. Last failure: {}. Reconnect the machine or update the Project's placement settings to retry",
                names,
                failures.join("; ")
            )
        } else if unverified {
            format!(
                "Environment unverified on {names}: mark a Project check machine, or add the repository on that machine"
            )
        } else if capabilities.is_empty() {
            format!("No eligible workspace owner for this role on {names}: {error}")
        } else {
            format!(
                "Upgrade the daemon on {names} to run this role; missing capability: {}",
                capabilities.join(", ")
            )
        };
        let key = self.placement_eligibility_key(task).await?;
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let metadata: Value = current
            .metadata_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_default();
        if metadata["placement_refusal"]["state"] == current.status
            && metadata["placement_refusal"]["eligibility_key"] == key
        {
            return Ok(true);
        }
        let annotation = serde_json::json!({"type":"dispatch_failed","code":"placement_unavailable","state":task.status,"message":message,"machines":machines,"missing_capabilities":capabilities,"rejected_candidates":refusal.rejected_candidates});
        let marker = serde_json::json!({"state":task.status,"repo_id":refusal.repo_id,"eligibility_key":key,"annotation":annotation});
        let wait_code = if failed {
            crate::placement::PlacementFilterCode::ProvisionFailed
        } else {
            crate::placement::PlacementFilterCode::EnvironmentUnverified
        };
        let wait=refusal.rejected_candidates.iter().find(|candidate|candidate.filter_codes.contains(&wait_code))
            .map(|candidate|serde_json::json!({"kind":if failed {"provision_failed"} else {"environment_unverified"},"machine":{"owner_kind":"daemon","daemon_id":candidate.daemon_id,"runtime_id":candidate.runtime_id},"checks":[]}));
        let result = self
            .db
            .record_placement_dispatch_refusal(db::RecordPlacementDispatchRefusalInput {
                annotation: annotation.to_string(),
                environment_wait: unverified || failed,
                marker: marker.to_string(),
                wait_json: serde_json::to_string(&wait).expect("wait"),
                marker_copy: marker.to_string(),
                updated_at: now_rfc3339(),
                task_id: &task.id,
                version: task.version,
            })
            .await?;
        if result != 1 {
            return Err(DbError::VersionConflict.into());
        }
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", &task.id))?;
        crate::deferred_dispatch::record_dispatch_disposition(
            &self.db,
            &current,
            &current.status,
            &message,
        )
        .await?;
        if unverified || failed {
            let wait_kind = if failed {
                "provision_failed"
            } else {
                "environment_unverified"
            };
            let now = now_rfc3339();
            let mut tx = db::begin_immediate(self.db.pool()).await?;
            let event = db::DomainEventRepo::append_event_in_tx(
                &*self.db,
                &mut tx,
                &db::CreateDomainEvent {
                    id: new_uuid_v4(),
                    event_type: format!("task.{wait_kind}"),
                    entity_type: "task".into(),
                    entity_id: task.id.clone(),
                    actor_type: "system".into(),
                    actor_id: None,
                    scope_type: "project".into(),
                    scope_id: task.project_id.clone(),
                    correlation_id: task.id.clone(),
                    causation_id: None,
                    causation_depth: 0,
                    dedupe_key: None,
                    payload_json: annotation.to_string(),
                    created_at: now.clone(),
                },
            )
            .await?;
            self.db.upsert_placement_refusal_attention_in_tx(&mut tx, db::UpsertPlacementRefusalAttentionInTxInput {
                id: new_uuid_v4(), project_id: &task.project_id, event_id: event.id, summary: &message,
                details_json: serde_json::json!({"task":{"id":task.id,"title":task.title},"machines":machines,"cause":wait_kind}).to_string(),
                dedupe_key: format!("task-{}:{}",if failed {"provision-failed"} else {"environment-unverified"},task.id),
                occurred_at: &now, updated_at: &now,
            }).await?;
            tx.commit().await?;
        }
        Ok(true)
    }

    /// A failed state-entry hook can record admission before the workflow rolls
    /// back. Finish that same refusal against the restored initial Task once.
    pub(crate) async fn finish_initial_unverified_refusal(&self, original: &Task) -> Result<bool> {
        let current = TaskRepo::get_by_id(&*self.db, &original.id, false)
            .await?
            .ok_or(db::DbError::NotFound)?;
        let metadata: Value = current
            .metadata_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_default();
        let marker = &metadata["placement_refusal"];
        if current.status != original.status
            || !matches!(
                metadata["environment_wait"]["kind"].as_str(),
                Some("environment_unverified" | "provision_failed")
            )
            || marker["eligibility_key"] != self.placement_eligibility_key(&current).await?
        {
            return Ok(false);
        }
        let repo_id = marker["repo_id"]
            .as_str()
            .ok_or_else(|| ServiceError::invalid_operation("placement refusal has no repository"))?
            .to_owned();
        let candidates =
            serde_json::from_value(marker["annotation"]["rejected_candidates"].clone())
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        self.record_placement_dispatch_refusal(
            &current,
            &ServiceError::PlacementUnavailable(crate::placement::PlacementUnavailable {
                task_id: current.id.clone(),
                repo_id,
                rejected_candidates: candidates,
            }),
        )
        .await
    }

    pub(crate) async fn refresh_placement_dispatch_refusal(&self, task: Task) -> Result<Task> {
        let metadata: Value = task
            .metadata_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_default();
        let marker = &metadata["placement_refusal"];
        if marker.is_null() {
            return Ok(task);
        }
        if marker["state"] == task.status
            && marker["eligibility_key"] == self.placement_eligibility_key(&task).await?
        {
            return Ok(task);
        }
        self.db
            .resolve_placement_refusal_attention(
                now_rfc3339(),
                now_rfc3339(),
                format!("task-environment-unverified:{}", task.id),
                format!("task-provision-failed:{}", task.id),
            )
            .await?;
        self.db
            .clear_placement_dispatch_refusal(db::ClearPlacementDispatchRefusalInput {
                annotation: marker["annotation"].to_string(),
                updated_at: now_rfc3339(),
                task_id: &task.id,
                version: task.version,
                metadata_json: &task.metadata_json,
            })
            .await?;
        TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", &task.id))
    }
}
