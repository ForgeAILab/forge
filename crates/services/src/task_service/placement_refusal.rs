use super::*;
use sha2::{Digest, Sha256};

impl TaskService {
    async fn placement_eligibility_key(&self, task: &Task) -> Result<String> {
        let project_id = &task.project_id;
        let assignments:Vec<(String,Option<String>,Option<String>,String)>=sqlx::query_as("SELECT role_name,assignee_type,assignee_id,updated_at FROM task_role_assignment WHERE task_id=? OR task_id=? ORDER BY task_id,role_name")
            .bind(&task.id).bind(task.parent_task_id.as_deref().unwrap_or(&task.id)).fetch_all(self.db.pool()).await?;
        // Heartbeat timestamps are intentionally excluded: only eligibility
        // facts can wake a deterministic refusal.
        let locations: Vec<(String, i64, String, String)> = sqlx::query_as("SELECT l.id,l.version,l.status,COALESCE(r.status,'') FROM repo_location l JOIN repo ON repo.id=l.repo_id LEFT JOIN runtime r ON r.id=l.runtime_id WHERE repo.project_id=? ORDER BY l.id")
            .bind(project_id).fetch_all(self.db.pool()).await?;
        let retries:Vec<(String,i64,String,i64)>=sqlx::query_as("SELECT j.runtime_id,j.attempts,j.checks_digest,j.connection_id FROM repo_provision_retry j JOIN repo r ON r.id=j.repo_id WHERE r.project_id=? ORDER BY j.repo_id,j.runtime_id")
            .bind(project_id).fetch_all(self.db.pool()).await?;
        let machines: Vec<(String, String, String)> =
            sqlx::query_as("SELECT id, status, detected_clis_json FROM daemon ORDER BY id")
                .fetch_all(self.db.pool())
                .await?;
        let profiles: Vec<(String, i64, String)> = sqlx::query_as(
            "SELECT id,version,COALESCE(selected_profile_id,'') FROM agent_identity ORDER BY id",
        )
        .fetch_all(self.db.pool())
        .await?;
        let profile_versions: Vec<(String, i64)> =
            sqlx::query_as("SELECT id,version FROM agent_profile ORDER BY id")
                .fetch_all(self.db.pool())
                .await?;
        let credentials: Vec<(String, String, bool)> =
            sqlx::query_as("SELECT id,updated_at,enabled FROM credential_handle ORDER BY id")
                .fetch_all(self.db.pool())
                .await?;
        let project_version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id=?")
            .bind(project_id)
            .fetch_one(self.db.pool())
            .await?;
        let policies: Vec<(String,String,bool)> = sqlx::query_as("SELECT daemon_id,executor_type,enabled FROM cli_runtime_policy ORDER BY daemon_id,executor_type")
            .fetch_all(self.db.pool()).await?;
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
                let machine: Option<String> =
                    sqlx::query_scalar("SELECT hostname FROM daemon WHERE id=?")
                        .bind(id)
                        .fetch_optional(self.db.pool())
                        .await?;
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
            let remote: Option<String> =
                sqlx::query_scalar("SELECT remote_url FROM repo WHERE id=?")
                    .bind(&refusal.repo_id)
                    .fetch_optional(self.db.pool())
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
                    let error:Option<String>=sqlx::query_scalar("SELECT COALESCE(l.last_error,j.last_error) FROM repo_provision_retry j LEFT JOIN repo_location l ON l.id=j.location_id WHERE j.repo_id=? AND j.runtime_id=?")
                        .bind(&refusal.repo_id).bind(&candidate.runtime_id).fetch_optional(self.db.pool()).await?.flatten();
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
        let result=sqlx::query("UPDATE task SET error_annotation=CASE WHEN error_annotation IS NULL OR json_extract(error_annotation,'$.type')='dispatch_failed' THEN ? ELSE error_annotation END,
            metadata_json=CASE WHEN ? THEN json_set(COALESCE(metadata_json,'{}'),'$.placement_refusal',json(?),'$.environment_wait',json(?)) ELSE json_set(COALESCE(metadata_json,'{}'),'$.placement_refusal',json(?)) END,version=version+1,updated_at=? WHERE id=? AND version=?")
            .bind(annotation.to_string()).bind(unverified || failed).bind(marker.to_string()).bind(serde_json::to_string(&wait).expect("wait")).bind(marker.to_string()).bind(now_rfc3339()).bind(&task.id).bind(task.version).execute(self.db.pool()).await?;
        if result.rows_affected() != 1 {
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
            sqlx::query("INSERT INTO attention_projection (id, attention_type, scope_type, scope_id, source_event_id, priority, status, summary, details_json, dedupe_key, occurred_at, updated_at, recommended_action) VALUES (?, 'human_input_required', 'project', ?, ?, 80, 'open', ?, ?, ?, ?, ?, 'configure_environment') ON CONFLICT(dedupe_key) DO UPDATE SET status='open', summary=excluded.summary, details_json=excluded.details_json, updated_at=excluded.updated_at, resolved_at=NULL, version=attention_projection.version+1")
                .bind(new_uuid_v4()).bind(&task.project_id).bind(event.id).bind(&message).bind(serde_json::json!({"task":{"id":task.id,"title":task.title},"machines":machines,"cause":wait_kind}).to_string()).bind(format!("task-{}:{}",if failed {"provision-failed"} else {"environment-unverified"},task.id)).bind(&now).bind(&now).execute(&mut *tx).await?;
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
        sqlx::query("UPDATE attention_projection SET status='resolved', resolved_at=?, updated_at=?, version=version+1 WHERE dedupe_key IN (?,?) AND status<>'resolved'").bind(now_rfc3339()).bind(now_rfc3339()).bind(format!("task-environment-unverified:{}",task.id)).bind(format!("task-provision-failed:{}",task.id)).execute(self.db.pool()).await?;
        sqlx::query("UPDATE task SET error_annotation=CASE WHEN error_annotation=? THEN NULL ELSE error_annotation END,
            metadata_json=CASE WHEN json_extract(metadata_json,'$.environment_wait.kind') IN ('environment_unverified','provision_failed') THEN json_remove(metadata_json,'$.placement_refusal','$.dispatch_disposition','$.environment_wait') ELSE json_remove(metadata_json,'$.placement_refusal','$.dispatch_disposition') END,version=version+1,updated_at=? WHERE id=? AND version=? AND metadata_json IS ?")
            .bind(marker["annotation"].to_string()).bind(now_rfc3339()).bind(&task.id).bind(task.version).bind(&task.metadata_json).execute(self.db.pool()).await?;
        TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", &task.id))
    }
}
