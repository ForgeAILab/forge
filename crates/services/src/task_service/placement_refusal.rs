use super::*;
use sha2::{Digest, Sha256};

impl TaskService {
    async fn placement_eligibility_key(&self, project_id: &str) -> Result<String> {
        // Heartbeat timestamps are intentionally excluded: only eligibility
        // facts can wake a deterministic refusal.
        let locations: Vec<(String, i64, String, String)> = sqlx::query_as("SELECT l.id,l.version,l.status,COALESCE(r.status,'') FROM repo_location l JOIN repo ON repo.id=l.repo_id LEFT JOIN runtime r ON r.id=l.runtime_id WHERE repo.project_id=? ORDER BY l.id")
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
                machines.push(serde_json::json!({"daemon_id":id,"machine":machine.unwrap_or_else(|| id.clone()),"filter_codes":candidate.filter_codes}));
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
        let message = if capabilities.is_empty() {
            format!("No eligible workspace owner for this role on {names}: {error}")
        } else {
            format!(
                "Upgrade the daemon on {names} to run this role; missing capability: {}",
                capabilities.join(", ")
            )
        };
        let key = self.placement_eligibility_key(&task.project_id).await?;
        let annotation = serde_json::json!({"type":"dispatch_failed","code":"placement_unavailable","state":task.status,"message":message,"machines":machines,"missing_capabilities":capabilities,"rejected_candidates":refusal.rejected_candidates});
        let marker =
            serde_json::json!({"state":task.status,"eligibility_key":key,"annotation":annotation});
        let result=sqlx::query("UPDATE task SET error_annotation=CASE WHEN error_annotation IS NULL OR json_extract(error_annotation,'$.type')='dispatch_failed' THEN ? ELSE error_annotation END,
            metadata_json=json_set(COALESCE(metadata_json,'{}'),'$.placement_refusal',json(?)),version=version+1,updated_at=? WHERE id=? AND version=?")
            .bind(annotation.to_string()).bind(marker.to_string()).bind(now_rfc3339()).bind(&task.id).bind(task.version).execute(self.db.pool()).await?;
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
        Ok(true)
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
            && marker["eligibility_key"] == self.placement_eligibility_key(&task.project_id).await?
        {
            return Ok(task);
        }
        sqlx::query("UPDATE task SET error_annotation=CASE WHEN error_annotation=? THEN NULL ELSE error_annotation END,
            metadata_json=json_remove(metadata_json,'$.placement_refusal','$.dispatch_disposition'),version=version+1,updated_at=? WHERE id=? AND version=? AND metadata_json IS ?")
            .bind(marker["annotation"].to_string()).bind(now_rfc3339()).bind(&task.id).bind(task.version).bind(&task.metadata_json).execute(self.db.pool()).await?;
        TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", &task.id))
    }
}
