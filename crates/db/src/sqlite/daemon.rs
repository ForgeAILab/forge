use super::*;
use crate::DaemonStatus;

#[async_trait]
impl DaemonRepo for SqliteDb {
    async fn upsert_by_machine_id(&self, input: UpsertDaemon) -> Result<Daemon> {
        sqlx::query("INSERT INTO daemon (id, machine_id, hostname, os, arch, agent_version, labels_json, status, registration_token_hash, owner_id, visibility, created_at, updated_at, max_concurrent_runs) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(machine_id) DO UPDATE SET hostname = excluded.hostname, os = excluded.os, arch = excluded.arch, agent_version = excluded.agent_version, labels_json = excluded.labels_json, status = excluded.status, registration_token_hash = excluded.registration_token_hash, owner_id = excluded.owner_id, visibility = excluded.visibility, updated_at = excluded.updated_at, max_concurrent_runs = COALESCE(excluded.max_concurrent_runs, daemon.max_concurrent_runs), version = daemon.version + 1 WHERE daemon.removed_at IS NULL")
            .bind(&input.id)
            .bind(&input.machine_id)
            .bind(&input.hostname)
            .bind(&input.os)
            .bind(&input.arch)
            .bind(input.agent_version.as_deref())
            .bind(&input.labels_json)
            .bind(input.status.to_string())
            .bind(input.registration_token_hash.as_deref())
            .bind(input.owner_id.as_deref())
            .bind(&input.visibility)
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .bind(input.max_concurrent_runs.map(i64::from))
            .execute(&self.pool)
            .await?;
        sqlx::query("SELECT * FROM daemon WHERE machine_id = ? AND removed_at IS NULL")
            .bind(&input.machine_id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_daemon)
            .transpose()?
            .ok_or(DbError::NotFound)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<Daemon>> {
        sqlx::query("SELECT * FROM daemon WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_daemon)
            .transpose()
    }

    async fn get_by_machine_id(&self, machine_id: &str) -> Result<Option<Daemon>> {
        sqlx::query("SELECT * FROM daemon WHERE machine_id = ? AND removed_at IS NULL")
            .bind(machine_id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_daemon)
            .transpose()
    }

    async fn list(&self, page: PageRequest) -> Result<Page<Daemon>> {
        let offset = decode_offset(&page.cursor)?;
        let sql = format!(
            "SELECT * FROM daemon WHERE removed_at IS NULL ORDER BY {} LIMIT ? OFFSET ?",
            order_clause_without_priority(&page)
        );
        let rows = sqlx::query(&sql)
            .bind(limit(&page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows
            .into_iter()
            .map(map_daemon)
            .collect::<Result<Vec<_>>>()?;
        let total = if page.include_total {
            total_count(
                &self.pool,
                "SELECT COUNT(*) FROM daemon WHERE removed_at IS NULL",
            )
            .await?
        } else {
            None
        };
        page_from_items(items, &page, offset, total)
    }

    async fn list_visible(&self, user_id: Option<&str>, page: PageRequest) -> Result<Page<Daemon>> {
        let offset = decode_offset(&page.cursor)?;
        let (sql, total_sql, has_user_param) = match user_id {
            Some(_) => (
                format!(
                    "SELECT * FROM daemon WHERE removed_at IS NULL AND (visibility = 'global' OR owner_id IS NULL OR owner_id = ?) ORDER BY {} LIMIT ? OFFSET ?",
                    order_clause_without_priority(&page)
                ),
                "SELECT COUNT(*) FROM daemon WHERE removed_at IS NULL AND (visibility = 'global' OR owner_id IS NULL OR owner_id = ?)"
                    as &str,
                true,
            ),
            None => (
                format!(
                    "SELECT * FROM daemon WHERE removed_at IS NULL AND (visibility = 'global' OR owner_id IS NULL) ORDER BY {} LIMIT ? OFFSET ?",
                    order_clause_without_priority(&page)
                ),
                "SELECT COUNT(*) FROM daemon WHERE removed_at IS NULL AND (visibility = 'global' OR owner_id IS NULL)"
                    as &str,
                false,
            ),
        };
        let mut query = sqlx::query(&sql);
        if let Some(uid) = user_id {
            query = query.bind(uid);
        }
        let rows = query
            .bind(limit(&page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows
            .into_iter()
            .map(map_daemon)
            .collect::<Result<Vec<_>>>()?;
        let total = if page.include_total {
            let mut tc = sqlx::query_scalar::<_, i64>(total_sql);
            if has_user_param {
                tc = tc.bind(user_id.unwrap());
            }
            Some(tc.fetch_one(&self.pool).await?)
        } else {
            None
        };
        page_from_items(items, &page, offset, total)
    }

    async fn get_visible(&self, id: &str, user_id: Option<&str>) -> Result<Option<Daemon>> {
        let daemon = DaemonRepo::get_by_id(self, id).await?;
        match daemon {
            Some(d) => {
                if self.daemon_removed(id).await? {
                    return Ok(None);
                }
                if d.visibility == "global" || d.owner_id.is_none() {
                    return Ok(Some(d));
                }
                if let Some(uid) = user_id {
                    if d.owner_id.as_deref() == Some(uid) {
                        return Ok(Some(d));
                    }
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn update_report(&self, input: UpdateDaemonReport) -> Result<Daemon> {
        let result = sqlx::query("UPDATE daemon SET last_report_at = ?, status = ?, detected_clis_json = ?, labels_json = COALESCE(?, labels_json), max_concurrent_runs = COALESCE(?, max_concurrent_runs), updated_at = ?, version = version + 1 WHERE id = ? AND removed_at IS NULL")
            .bind(&input.last_report_at)
            .bind(input.status.to_string())
            .bind(&input.detected_clis_json)
            .bind(&input.labels_json)
            .bind(input.max_concurrent_runs.map(i64::from))
            .bind(&input.updated_at)
            .bind(&input.id)
            .execute(&self.pool).await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        DaemonRepo::get_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn update_run_limit(
        &self,
        id: &str,
        version: i64,
        run_limit: Option<u32>,
    ) -> Result<Daemon> {
        let mut tx = crate::begin_immediate(&self.pool).await?;
        let result = sqlx::query("UPDATE daemon SET run_limit = ?, updated_at = ?, version = version + 1 WHERE id = ? AND version = ? AND removed_at IS NULL")
            .bind(run_limit.map(i64::from)).bind(crate::now_rfc3339()).bind(id).bind(version)
            .execute(&mut *tx).await?;
        if result.rows_affected() == 0 {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM daemon WHERE id = ? AND removed_at IS NULL)",
            )
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
            return Err(if exists {
                DbError::VersionConflict
            } else {
                DbError::NotFound
            });
        }
        let daemon = map_daemon(
            sqlx::query("SELECT * FROM daemon WHERE id = ? AND removed_at IS NULL")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?,
        )?;
        tx.commit().await?;
        Ok(daemon)
    }

    async fn mark_online(&self, id: &str, last_report_at: &str) -> Result<Daemon> {
        let result = sqlx::query(
            "UPDATE daemon SET status = 'online', last_report_at = ?, updated_at = ?, version = version + 1 WHERE id = ? AND removed_at IS NULL",
        )
        .bind(last_report_at)
        .bind(last_report_at)
        .bind(id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        DaemonRepo::get_by_id(self, id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn mark_offline(&self, id: &str, updated_at: &str) -> Result<Daemon> {
        let result = sqlx::query(
            "UPDATE daemon SET status = 'offline', updated_at = ?, version = version + 1 WHERE id = ? AND removed_at IS NULL",
        )
        .bind(updated_at)
        .bind(id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        DaemonRepo::get_by_id(self, id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn list_available_for_executor(&self, executor_type: &str) -> Result<Vec<Daemon>> {
        let rows = sqlx::query(
            "SELECT * FROM daemon WHERE removed_at IS NULL AND status = 'online' ORDER BY created_at ASC, id ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut daemons = Vec::new();
        for row in rows {
            let daemon = map_daemon(row)?;
            if daemon_supports_executor(&daemon, executor_type) {
                daemons.push(daemon);
            }
        }
        Ok(daemons)
    }
}

fn daemon_supports_executor(daemon: &Daemon, executor_type: &str) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&daemon.detected_clis_json) else {
        return false;
    };
    let Some(items) = value.as_array() else {
        return false;
    };
    items.iter().any(|item| {
        item.get("kind").and_then(serde_json::Value::as_str) == Some(executor_type)
            && item.get("availability").and_then(serde_json::Value::as_str) == Some("authenticated")
    })
}

impl SqliteDb {
    pub async fn daemon_removed(&self, id: &str) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM daemon WHERE id=? AND removed_at IS NOT NULL)",
        )
        .bind(id)
        .fetch_one(self.pool())
        .await?)
    }

    /// Revocation, cleanup and recovery enqueues commit together. History keeps
    /// its UUID and hostname; retiring the unique key permits fresh registration.
    pub async fn remove_daemon(
        &self,
        id: &str,
        actor_id: &str,
        is_admin: bool,
        local_machine_id: &str,
        transport_connected: bool,
    ) -> Result<api_types::RemoveDaemonResponse> {
        use crate::{DomainEventRepo, TaskStepRepo};
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let row = sqlx::query("SELECT * FROM daemon WHERE id=? AND removed_at IS NULL")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
        let daemon = map_daemon(row)?;
        if !match daemon.owner_id.as_deref() {
            Some(owner) => owner == actor_id,
            None => is_admin,
        } {
            return Err(DbError::NotFound);
        }
        if daemon.machine_id == local_machine_id || daemon.machine_id.starts_with("embedded:") {
            return Err(DbError::LocalMachine);
        }
        if transport_connected || daemon.status == DaemonStatus::Online {
            return Err(DbError::MachineConnected);
        }
        let now = crate::now_rfc3339();
        let tasks: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT t.id FROM task t WHERE t.deleted_at IS NULL AND (
                EXISTS(SELECT 1 FROM workspace_placement p WHERE p.task_id=t.id AND (p.daemon_id=? OR p.execution_daemon_id=?))
                OR EXISTS(SELECT 1 FROM pending_remote_cancel c LEFT JOIN workspace w ON w.id=c.workspace_id LEFT JOIN task_step s ON s.id=c.step_id WHERE c.daemon_id=? AND (w.task_id=t.id OR s.task_id=t.id OR EXISTS(SELECT 1 FROM execution e WHERE e.task_id=t.id AND e.workspace_id=c.workspace_id)))
                OR EXISTS(SELECT 1 FROM execution e LEFT JOIN workspace_placement p ON p.workspace_id=e.workspace_id LEFT JOIN agent_current a ON a.id=e.agent_id WHERE e.task_id=t.id AND e.status='running' AND CASE WHEN e.workspace_id IS NOT NULL THEN CASE p.owner_kind WHEN 'daemon' THEN p.daemon_id ELSE p.execution_daemon_id END ELSE COALESCE(CASE WHEN json_valid(e.executor_config_snapshot_json) THEN json_extract(e.executor_config_snapshot_json,'$.daemon_id') END,a.daemon_id) END=?)
                OR (json_valid(t.metadata_json) AND json_extract(t.metadata_json,'$.environment_wait.machine.daemon_id')=?)
            ) ORDER BY t.id")
            .bind(id).bind(id).bind(id).bind(id).bind(id).fetch_all(&mut *tx).await?;
        let changed = sqlx::query("UPDATE daemon SET removed_at=?, registration_token_hash=NULL, machine_id=?, run_limit=NULL, updated_at=?, version=version+1 WHERE id=? AND version=? AND status='offline' AND removed_at IS NULL")
            .bind(&now).bind(format!("removed:{id}")).bind(&now).bind(id).bind(daemon.version).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        let pending_remote_cancels_cleared =
            sqlx::query("DELETE FROM pending_remote_cancel WHERE daemon_id=?")
                .bind(id)
                .execute(&mut *tx)
                .await?
                .rows_affected();
        sqlx::query("UPDATE task_remote_operation SET state='cancelled' WHERE daemon_id=? AND state='running'").bind(id).execute(&mut *tx).await?;
        let cleanup_records_cleared = sqlx::query("DELETE FROM command_receipt WHERE principal_id='workspace-backend' AND operation LIKE 'daemon.workspace.%' AND json_valid(outcome_json) AND json_extract(outcome_json,'$.metadata.daemon_id')=?")
            .bind(id).execute(&mut *tx).await?.rows_affected();
        let provisioning_attempts_cleared = sqlx::query("DELETE FROM repo_provision_retry WHERE runtime_id IN (SELECT id FROM runtime WHERE daemon_id=?)")
            .bind(id).execute(&mut *tx).await?.rows_affected();
        let readiness_records_cleared = sqlx::query(
            "DELETE FROM project_machine_readiness WHERE owner_kind='daemon' AND daemon_id=?",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        sqlx::query("UPDATE runtime SET status='offline',updated_at=? WHERE daemon_id=?")
            .bind(&now)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE repo_location SET status='unavailable',version=version+1,updated_at=? WHERE daemon_id=?")
            .bind(&now).bind(id).execute(&mut *tx).await?;
        // The missing owner's worktree stays pinned. Existing owner-loss
        // settlement parks it; only an unplaced Task selects a different owner.
        let placements_failed = sqlx::query("UPDATE workspace_placement SET state='failed',failure_cause='owner_disconnected_timeout',disconnected_at=COALESCE(disconnected_at,?),reserved_until=NULL,updated_at=?,version=version+1 WHERE (daemon_id=? OR execution_daemon_id=?) AND state NOT IN ('cleaning','cleaned')")
            .bind(&now).bind(&now).bind(id).bind(id).execute(&mut *tx).await?.rows_affected();
        for task_id in &tasks {
            let step_id = crate::new_uuid_v4();
            let task = sqlx::query("SELECT status,version FROM task WHERE id=?")
                .bind(task_id)
                .fetch_one(&mut *tx)
                .await?;
            let queued=self.enqueue_step_in_tx(&mut tx,&crate::EnqueueTaskStep {
                id:step_id.clone(),task_id:task_id.clone(),kind:"command".to_owned(),
                payload_json:serde_json::json!({"operation":"settle_removed_machine","arguments":[task_id,id],"preempt":false}).to_string(),
                causation_step_id:None,causation_key:format!("machine-removed:{id}"),chain_id:step_id,chain_position:1,
                expected_status:task.get("status"),expected_version:task.get("version"),expected_epoch:None,
                lane:"fast".to_owned(),available_at:now.clone(),
            }).await?;
            self.mark_step_identity_fenced_in_tx(&mut tx, &queued)
                .await?;
        }
        let result = api_types::RemoveDaemonResponse {
            id: id.to_owned(),
            hostname: daemon.hostname.clone(),
            pending_remote_cancels_cleared,
            cleanup_records_cleared,
            provisioning_attempts_cleared,
            readiness_records_cleared,
            placements_failed,
            tasks_queued: tasks.len() as u64,
        };
        DomainEventRepo::append_event_in_tx(self,&mut tx,&crate::CreateDomainEvent {
            id:crate::new_uuid_v4(),event_type:"machine.removed".to_owned(),entity_type:"daemon".to_owned(),entity_id:id.to_owned(),
            actor_type:"user".to_owned(),actor_id:Some(actor_id.to_owned()),scope_type:"account".to_owned(),scope_id:actor_id.to_owned(),
            correlation_id:crate::new_uuid_v4(),causation_id:None,causation_depth:0,dedupe_key:Some(format!("machine-removed:{id}")),
            payload_json:serde_json::json!({"actor_id":actor_id,"machine_id":daemon.machine_id,"removal":result}).to_string(),created_at:now,
        }).await?;
        tx.commit().await?;
        self.domain_event_notify().notify_waiters();
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations};

    fn request_page() -> PageRequest {
        PageRequest {
            cursor: None,
            limit: 20,
            include_total: true,
            sort_by: crate::SortBy::CreatedAt,
            sort_order: crate::SortOrder::Asc,
        }
    }

    async fn fixture() -> (SqliteDb, Daemon) {
        let db = SqliteDb::new(create_sqlite_pool("sqlite::memory:").await.unwrap());
        run_migrations(db.pool()).await.unwrap();
        let now = now_rfc3339();
        let daemon = DaemonRepo::upsert_by_machine_id(
            &db,
            UpsertDaemon {
                id: new_uuid_v4(),
                machine_id: "dead-host".into(),
                hostname: "Old workstation".into(),
                os: "linux".into(),
                arch: "x86_64".into(),
                agent_version: None,
                labels_json: "{}".into(),
                status: DaemonStatus::Offline,
                registration_token_hash: Some("hash".into()),
                owner_id: None,
                visibility: "global".into(),
                max_concurrent_runs: Some(4),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO pending_remote_cancel VALUES ('orphan-operation','deleted-step','deleted-workspace','deleted-placement',?,'deleted-runtime',1,0,?)")
            .bind(&daemon.id).bind(now_rfc3339()).execute(db.pool()).await.unwrap();
        let event = crate::DomainEventRepo::append_event(
            &db,
            crate::CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "test.cleanup".into(),
                entity_type: "daemon".into(),
                entity_id: daemon.id.clone(),
                actor_type: "system".into(),
                actor_id: None,
                scope_type: "account".into(),
                scope_id: "admin".into(),
                correlation_id: new_uuid_v4(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: None,
                payload_json: "{}".into(),
                created_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        for (receipt, operation) in [
            ("cleanup-receipt", "daemon.workspace.cleanup"),
            ("history-receipt", "history.command"),
        ] {
            sqlx::query("INSERT INTO command_receipt(id,principal_type,principal_id,scope_type,scope_id,operation,idempotency_key,input_digest,policy_result,correlation_id,event_id,outcome_json,committed_at) VALUES (?,'system','workspace-backend','task','deleted-task',?,'key','digest','allowed','correlation',?,?,?)")
                .bind(receipt).bind(operation).bind(&event.id).bind(serde_json::json!({"metadata":{"daemon_id":daemon.id}}).to_string()).bind(now_rfc3339()).execute(db.pool()).await.unwrap();
        }
        (db, daemon)
    }

    #[tokio::test]
    async fn removal_clears_orphan_cleanup_and_preserves_paginated_history() {
        let (db, daemon) = fixture().await;
        assert!(
            sqlx::query("DELETE FROM command_receipt WHERE id='cleanup-receipt'")
                .execute(db.pool())
                .await
                .is_err()
        );
        let result = db
            .remove_daemon(&daemon.id, "admin", true, "local", false)
            .await
            .unwrap();
        assert_eq!(result.pending_remote_cancels_cleared, 1);
        assert_eq!(result.cleanup_records_cleared, 1);
        assert!(
            sqlx::query("DELETE FROM command_receipt WHERE id='history-receipt'")
                .execute(db.pool())
                .await
                .is_err()
        );
        assert_eq!(result.tasks_queued, 0);
        let historical = DaemonRepo::get_by_id(&db, &daemon.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(historical.hostname, daemon.hostname);
        assert!(historical.registration_token_hash.is_none());
        assert!(db
            .pending_remote_cancels(None, None)
            .await
            .unwrap()
            .is_empty());
        assert!(DaemonRepo::get_by_machine_id(&db, &daemon.machine_id)
            .await
            .unwrap()
            .is_none());
        let page = DaemonRepo::list(&db, request_page()).await.unwrap();
        assert!(page.items.is_empty());
        assert_eq!(page.total_count, Some(0));
        let page = DaemonRepo::list_visible(&db, None, request_page())
            .await
            .unwrap();
        assert!(page.items.is_empty());
        assert_eq!(page.total_count, Some(0));
        assert!(DaemonRepo::get_visible(&db, &daemon.id, None)
            .await
            .unwrap()
            .is_none());
        let mut tx = db.pool().begin().await.unwrap();
        let caps = crate::machine_capacity::list_machine_capacity(&mut tx, "local", None)
            .await
            .unwrap();
        assert!(!caps
            .iter()
            .any(|row| row.daemon_id.as_deref() == Some(&daemon.id)));
        tx.commit().await.unwrap();
        let (actor, payload): (Option<String>, String) = sqlx::query_as(
            "SELECT actor_id,payload_json FROM domain_event WHERE event_type='machine.removed'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(actor.as_deref(), Some("admin"));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&payload).unwrap()["removal"]
                ["pending_remote_cancels_cleared"],
            1
        );
    }

    #[tokio::test]
    async fn removal_is_atomic_when_audit_append_fails() {
        let (db, daemon) = fixture().await;
        sqlx::query("CREATE TRIGGER reject_removal BEFORE INSERT ON domain_event WHEN NEW.event_type='machine.removed' BEGIN SELECT RAISE(ABORT,'audit unavailable'); END")
            .execute(db.pool()).await.unwrap();
        assert!(db
            .remove_daemon(&daemon.id, "admin", true, "local", false)
            .await
            .is_err());
        assert!(!db.daemon_removed(&daemon.id).await.unwrap());
        assert_eq!(
            db.pending_remote_cancels(None, None).await.unwrap().len(),
            1
        );
        assert_eq!(
            DaemonRepo::get_by_id(&db, &daemon.id)
                .await
                .unwrap()
                .unwrap()
                .registration_token_hash,
            Some("hash".into())
        );
    }

    #[tokio::test]
    async fn removed_registration_rejects_late_online_report_and_runtime_writes() {
        let (db, daemon) = fixture().await;
        let runtime = CreateRuntime {
            id: new_uuid_v4(),
            daemon_id: daemon.id.clone(),
            kind: "local".into(),
            workspace_root: "workspace".into(),
            status: crate::RuntimeStatus::Ready,
            labels_json: "{}".into(),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        };
        RuntimeRepo::upsert_by_daemon_kind(&db, runtime.clone())
            .await
            .unwrap();
        db.remove_daemon(&daemon.id, "admin", true, "local", false)
            .await
            .unwrap();
        assert!(matches!(
            DaemonRepo::mark_online(&db, &daemon.id, &now_rfc3339()).await,
            Err(DbError::NotFound)
        ));
        assert!(matches!(
            DaemonRepo::update_report(
                &db,
                UpdateDaemonReport {
                    id: daemon.id.clone(),
                    last_report_at: now_rfc3339(),
                    status: DaemonStatus::Online,
                    detected_clis_json: "[]".into(),
                    labels_json: None,
                    max_concurrent_runs: None,
                    updated_at: now_rfc3339()
                }
            )
            .await,
            Err(DbError::NotFound)
        ));
        assert!(matches!(
            RuntimeRepo::upsert_by_daemon_kind(&db, runtime).await,
            Err(DbError::NotFound)
        ));
        assert!(RuntimeRepo::list(
            &db,
            RuntimeListQuery {
                daemon_id: Some(daemon.id.clone()),
                page: request_page()
            }
        )
        .await
        .unwrap()
        .items
        .is_empty());
        assert!(matches!(
            DaemonRepo::update_run_limit(&db, &daemon.id, daemon.version + 1, Some(2)).await,
            Err(DbError::NotFound)
        ));
    }

    #[tokio::test]
    async fn connected_local_and_non_owner_removals_leave_cleanup_intact() {
        let (db, daemon) = fixture().await;
        assert!(matches!(
            db.remove_daemon(&daemon.id, "member", false, "local", false)
                .await,
            Err(DbError::NotFound)
        ));
        assert!(matches!(
            db.remove_daemon(&daemon.id, "admin", true, &daemon.machine_id, false)
                .await,
            Err(DbError::LocalMachine)
        ));
        assert!(matches!(
            db.remove_daemon(&daemon.id, "admin", true, "local", true)
                .await,
            Err(DbError::MachineConnected)
        ));
        DaemonRepo::mark_online(&db, &daemon.id, &now_rfc3339())
            .await
            .unwrap();
        assert!(matches!(
            db.remove_daemon(&daemon.id, "admin", true, "local", false)
                .await,
            Err(DbError::MachineConnected)
        ));
        assert_eq!(
            db.pending_remote_cancels(None, None).await.unwrap().len(),
            1
        );
        assert!(!db.daemon_removed(&daemon.id).await.unwrap());
    }
}
