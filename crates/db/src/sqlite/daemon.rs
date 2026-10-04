use super::*;

#[async_trait]
impl DaemonRepo for SqliteDb {
    async fn upsert_by_machine_id(&self, input: UpsertDaemon) -> Result<Daemon> {
        sqlx::query("INSERT INTO daemon (id, machine_id, hostname, os, arch, agent_version, labels_json, status, registration_token_hash, owner_id, visibility, created_at, updated_at, max_concurrent_runs) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(machine_id) DO UPDATE SET hostname = excluded.hostname, os = excluded.os, arch = excluded.arch, agent_version = excluded.agent_version, labels_json = excluded.labels_json, status = excluded.status, registration_token_hash = excluded.registration_token_hash, owner_id = excluded.owner_id, visibility = excluded.visibility, updated_at = excluded.updated_at, max_concurrent_runs = COALESCE(excluded.max_concurrent_runs, daemon.max_concurrent_runs), version = daemon.version + 1")
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
        sqlx::query("SELECT * FROM daemon WHERE machine_id = ?")
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
        sqlx::query("SELECT * FROM daemon WHERE machine_id = ?")
            .bind(machine_id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_daemon)
            .transpose()
    }

    async fn list(&self, page: PageRequest) -> Result<Page<Daemon>> {
        let offset = decode_offset(&page.cursor)?;
        let sql = format!(
            "SELECT * FROM daemon ORDER BY {} LIMIT ? OFFSET ?",
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
            total_count(&self.pool, "SELECT COUNT(*) FROM daemon").await?
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
                    "SELECT * FROM daemon WHERE visibility = 'global' OR owner_id IS NULL OR owner_id = ? ORDER BY {} LIMIT ? OFFSET ?",
                    order_clause_without_priority(&page)
                ),
                "SELECT COUNT(*) FROM daemon WHERE visibility = 'global' OR owner_id IS NULL OR owner_id = ?"
                    as &str,
                true,
            ),
            None => (
                format!(
                    "SELECT * FROM daemon WHERE visibility = 'global' OR owner_id IS NULL ORDER BY {} LIMIT ? OFFSET ?",
                    order_clause_without_priority(&page)
                ),
                "SELECT COUNT(*) FROM daemon WHERE visibility = 'global' OR owner_id IS NULL"
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
        let result = sqlx::query("UPDATE daemon SET last_report_at = ?, status = ?, detected_clis_json = ?, labels_json = COALESCE(?, labels_json), max_concurrent_runs = COALESCE(?, max_concurrent_runs), updated_at = ?, version = version + 1 WHERE id = ?")
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
        let result = sqlx::query("UPDATE daemon SET run_limit = ?, updated_at = ?, version = version + 1 WHERE id = ? AND version = ?")
            .bind(run_limit.map(i64::from)).bind(crate::now_rfc3339()).bind(id).bind(version)
            .execute(&mut *tx).await?;
        if result.rows_affected() == 0 {
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM daemon WHERE id = ?)")
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
            sqlx::query("SELECT * FROM daemon WHERE id = ?")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?,
        )?;
        tx.commit().await?;
        Ok(daemon)
    }

    async fn mark_online(&self, id: &str, last_report_at: &str) -> Result<Daemon> {
        let result = sqlx::query(
            "UPDATE daemon SET status = 'online', last_report_at = ?, updated_at = ?, version = version + 1 WHERE id = ?",
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
            "UPDATE daemon SET status = 'offline', updated_at = ?, version = version + 1 WHERE id = ?",
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
            "SELECT * FROM daemon WHERE status = 'online' ORDER BY created_at ASC, id ASC",
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
