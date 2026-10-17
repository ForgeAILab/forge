use super::*;
use crate::now_rfc3339;

#[async_trait]
impl AttentionRepo for SqliteDb {
    async fn list_attention(&self, query: AttentionListQuery) -> Result<Page<AttentionProjection>> {
        let offset = decode_offset(&query.page.cursor)?;
        let mut predicates = vec!["1 = 1".to_owned()];
        let mut binds: Vec<String> = Vec::new();

        if let Some(account_id) = query.account_id.as_deref() {
            // Account views include the caller's account scope plus projects
            // that are globally visible, owned by the caller, or explicitly
            // shared with them.  The caller still supplies the account ID;
            // this query never trusts a project list from the client.
            predicates.push(
                "((a.scope_type = 'account' AND a.scope_id = ?)
                  OR (a.scope_type = 'project' AND EXISTS (
                      SELECT 1 FROM project p
                      LEFT JOIN project_member pm ON pm.project_id = p.id AND pm.user_id = ?
                      WHERE p.id = a.scope_id
                        AND (p.owner_id IS NULL OR p.owner_id = ? OR pm.user_id IS NOT NULL)
                  )))"
                .to_owned(),
            );
            binds.push(account_id.to_owned());
            binds.push(account_id.to_owned());
            binds.push(account_id.to_owned());
        }
        if let Some(project_id) = query.project_id.as_deref() {
            predicates.push("a.scope_type = 'project' AND a.scope_id = ?".to_owned());
            binds.push(project_id.to_owned());
        }
        if let Some(scope_type) = query.scope_type.as_deref() {
            predicates.push("a.scope_type = ?".to_owned());
            binds.push(scope_type.to_owned());
        }
        if let Some(status) = query.status.as_deref() {
            predicates.push("a.status = ?".to_owned());
            binds.push(status.to_owned());
        } else {
            predicates.push("a.status <> 'resolved'".to_owned());
        }
        if !query.include_snoozed {
            predicates.push("(a.snoozed_until IS NULL OR a.snoozed_until <= ?)".to_owned());
            binds.push(now_rfc3339());
        }

        let where_sql = predicates.join(" AND ");
        let sql = format!(
            "SELECT a.* FROM attention_projection a
             WHERE {where_sql}
             ORDER BY a.priority DESC, a.occurred_at ASC, a.id ASC
             LIMIT ? OFFSET ?"
        );
        let mut statement = sqlx::query(&sql);
        for bind in &binds {
            statement = statement.bind(bind);
        }
        let rows = statement
            .bind(limit(&query.page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        let items = rows
            .into_iter()
            .map(map_attention_projection)
            .collect::<Result<Vec<_>>>()?;

        let total_count = if query.page.include_total {
            let count_sql =
                format!("SELECT COUNT(*) FROM attention_projection a WHERE {where_sql}");
            let mut statement = sqlx::query_scalar::<_, i64>(&count_sql);
            for bind in &binds {
                statement = statement.bind(bind);
            }
            Some(statement.fetch_one(&self.pool).await?)
        } else {
            None
        };

        page_from_items(items, &query.page, offset, total_count)
    }

    async fn get_attention(&self, id: &str) -> Result<Option<AttentionProjection>> {
        sqlx::query("SELECT * FROM attention_projection WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_attention_projection)
            .transpose()
    }

    async fn insert_attention(
        &self,
        input: CreateAttentionProjection,
    ) -> Result<AttentionProjection> {
        let mut tx = crate::begin_immediate(&self.pool).await?;
        let result = self.insert_attention_in_tx(&mut tx, input).await?;
        tx.commit().await?;
        Ok(result)
    }

    async fn update_attention_lifecycle(
        &self,
        input: UpdateAttentionLifecycle,
    ) -> Result<AttentionProjection> {
        let mut set_parts = vec![
            "status = ?".to_owned(),
            "version = version + 1".to_owned(),
            "updated_at = ?".to_owned(),
        ];
        if input.acknowledged_at.is_some() {
            set_parts.push("acknowledged_at = ?".to_owned());
        }
        if input.snoozed_until.is_some() {
            set_parts.push("snoozed_until = ?".to_owned());
        }
        if input.resolved_at.is_some() {
            set_parts.push("resolved_at = ?".to_owned());
        }
        if input.updated_by_user_id.is_some() {
            set_parts.push("updated_by_user_id = ?".to_owned());
        }
        let sql = format!(
            "UPDATE attention_projection SET {} WHERE id = ? AND version = ?",
            set_parts.join(", ")
        );
        let mut statement = sqlx::query(&sql)
            .bind(&input.status)
            .bind(&input.updated_at);
        if let Some(value) = input.acknowledged_at.as_ref() {
            statement = statement.bind(value.as_deref());
        }
        if let Some(value) = input.snoozed_until.as_ref() {
            statement = statement.bind(value.as_deref());
        }
        if let Some(value) = input.resolved_at.as_ref() {
            statement = statement.bind(value.as_deref());
        }
        if let Some(value) = input.updated_by_user_id.as_deref() {
            statement = statement.bind(value);
        }
        let result = statement
            .bind(&input.id)
            .bind(input.expected_version)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return match self.get_attention(&input.id).await? {
                Some(_) => Err(DbError::VersionConflict),
                None => Err(DbError::NotFound),
            };
        }
        self.get_attention(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn resolve_attention_by_dedupe(
        &self,
        dedupe_key: &str,
        source_event_id: &str,
        updated_at: &str,
    ) -> Result<Option<AttentionProjection>> {
        let mut tx = crate::begin_immediate(&self.pool).await?;
        let result = self
            .resolve_attention_by_dedupe_in_tx(&mut tx, dedupe_key, source_event_id, updated_at)
            .await?;
        tx.commit().await?;
        Ok(result)
    }
}

fn map_attention_projection(row: SqliteRow) -> Result<AttentionProjection> {
    Ok(AttentionProjection {
        id: row.try_get("id")?,
        attention_type: row.try_get("attention_type")?,
        scope_type: row.try_get("scope_type")?,
        scope_id: row.try_get("scope_id")?,
        identity_id: row.try_get("identity_id")?,
        source_event_id: row.try_get("source_event_id")?,
        priority: row.try_get("priority")?,
        status: row.try_get("status")?,
        summary: row.try_get("summary")?,
        details_json: row.try_get("details_json")?,
        dedupe_key: row.try_get("dedupe_key")?,
        occurred_at: row.try_get("occurred_at")?,
        updated_at: row.try_get("updated_at")?,
        version: row.try_get("version")?,
        acknowledged_at: row.try_get("acknowledged_at")?,
        snoozed_until: row.try_get("snoozed_until")?,
        resolved_at: row.try_get("resolved_at")?,
        updated_by_user_id: row.try_get("updated_by_user_id")?,
        recommended_action: row.try_get("recommended_action")?,
        source_sequence: row.try_get("source_sequence")?,
    })
}

impl SqliteDb {
    pub async fn get_attention_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        id: &str,
    ) -> Result<Option<AttentionProjection>> {
        sqlx::query("SELECT * FROM attention_projection WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut **tx)
            .await?
            .map(map_attention_projection)
            .transpose()
    }

    pub async fn insert_attention_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        input: CreateAttentionProjection,
    ) -> Result<AttentionProjection> {
        sqlx::query(
            "INSERT INTO attention_projection (
                id, attention_type, scope_type, scope_id, identity_id,
                source_event_id, priority, status, summary, details_json,
                dedupe_key, occurred_at, updated_at, acknowledged_at,
                snoozed_until, resolved_at, updated_by_user_id,
                recommended_action, source_sequence
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(dedupe_key) DO UPDATE SET
                attention_type = excluded.attention_type,
                scope_type = excluded.scope_type,
                scope_id = excluded.scope_id,
                identity_id = COALESCE(excluded.identity_id, attention_projection.identity_id),
                source_event_id = excluded.source_event_id,
                priority = excluded.priority,
                summary = excluded.summary,
                details_json = excluded.details_json,
                occurred_at = excluded.occurred_at,
                recommended_action = excluded.recommended_action,
                source_sequence = excluded.source_sequence,
                status = CASE
                    WHEN attention_projection.status = 'resolved'
                     AND attention_projection.source_event_id <> excluded.source_event_id
                    THEN excluded.status
                    ELSE attention_projection.status
                END,
                resolved_at = CASE
                    WHEN attention_projection.status = 'resolved'
                     AND attention_projection.source_event_id <> excluded.source_event_id
                    THEN NULL
                    ELSE attention_projection.resolved_at
                END,
                snoozed_until = CASE
                    WHEN attention_projection.status = 'resolved'
                     AND attention_projection.source_event_id <> excluded.source_event_id
                    THEN NULL
                    ELSE attention_projection.snoozed_until
                END,
                version = attention_projection.version + 1,
                updated_at = excluded.updated_at",
        )
        .bind(&input.id)
        .bind(&input.attention_type)
        .bind(&input.scope_type)
        .bind(&input.scope_id)
        .bind(input.identity_id.as_deref())
        .bind(&input.source_event_id)
        .bind(input.priority)
        .bind(&input.status)
        .bind(&input.summary)
        .bind(&input.details_json)
        .bind(&input.dedupe_key)
        .bind(&input.occurred_at)
        .bind(&input.updated_at)
        .bind(input.acknowledged_at.as_deref())
        .bind(input.snoozed_until.as_deref())
        .bind(input.resolved_at.as_deref())
        .bind(input.updated_by_user_id.as_deref())
        .bind(&input.recommended_action)
        .bind(input.source_sequence)
        .execute(&mut **tx)
        .await?;

        sqlx::query("SELECT * FROM attention_projection WHERE dedupe_key = ?")
            .bind(&input.dedupe_key)
            .fetch_optional(&mut **tx)
            .await?
            .map(map_attention_projection)
            .transpose()?
            .ok_or(DbError::NotFound)
    }
    pub async fn resolve_attention_by_dedupe_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        dedupe_key: &str,
        source_event_id: &str,
        updated_at: &str,
    ) -> Result<Option<AttentionProjection>> {
        sqlx::query(
            "UPDATE attention_projection
             SET status = 'resolved', resolved_at = ?, snoozed_until = NULL,
                 source_event_id = ?, updated_at = ?, version = version + 1
             WHERE dedupe_key = ? AND status <> 'resolved'",
        )
        .bind(updated_at)
        .bind(source_event_id)
        .bind(updated_at)
        .bind(dedupe_key)
        .execute(&mut **tx)
        .await?;
        sqlx::query("SELECT * FROM attention_projection WHERE dedupe_key = ?")
            .bind(dedupe_key)
            .fetch_optional(&mut **tx)
            .await?
            .map(map_attention_projection)
            .transpose()
    }
}

impl SqliteDb {
    pub async fn cleanup_attention_is_open_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        dedupe_key: &str,
    ) -> crate::Result<i64> {
        let result = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (SELECT 1 FROM attention_projection
                            WHERE dedupe_key = ? AND status <> 'resolved')",
        )
        .bind(dedupe_key)
        .fetch_one(connection)
        .await?;
        Ok(result)
    }
}

impl SqliteDb {
    pub async fn resolve_task_workspace_cleanup_attention(
        &self,
        now: &str,
        task_id: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE attention_projection
             SET status = 'resolved', resolved_at = ?, snoozed_until = NULL,
                 updated_at = ?, version = version + 1
             WHERE status <> 'resolved'
               AND dedupe_key IN (SELECT 'workspace-cleanup:' || id FROM workspace
                                  WHERE task_id = ?)",
        )
        .bind(now)
        .bind(now)
        .bind(task_id)
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn resolve_workspace_cleanup_attention_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        now: &str,
        dedupe_key: &str,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE attention_projection
                 SET status = 'resolved', resolved_at = ?, snoozed_until = NULL,
                     updated_at = ?, version = version + 1
                 WHERE dedupe_key = ? AND status <> 'resolved'",
        )
        .bind(now)
        .bind(now)
        .bind(dedupe_key)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn workspace_disconnect_incident_is_current(
        &self,
        attention_key: String,
        event_key: String,
    ) -> crate::Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM attention_projection a JOIN domain_event e ON e.id = a.source_event_id
         WHERE a.dedupe_key = ? AND e.dedupe_key = ? AND a.status != 'resolved')")
.bind(attention_key)
.bind(event_key)
.fetch_one(self.pool())
.await?)
    }
}

impl SqliteDb {
    pub async fn resolve_reconciled_owner_attention_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        resolved_at: String,
        source_event_id: &str,
        updated_at: String,
        dedupe_key: String,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE attention_projection SET status = 'resolved', resolved_at = ?,
        snoozed_until = NULL, source_event_id = ?, updated_at = ?, version = version + 1
        WHERE dedupe_key = ? AND status <> 'resolved'",
        )
        .bind(resolved_at)
        .bind(source_event_id)
        .bind(updated_at)
        .bind(dedupe_key)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn resolve_owner_reconnect_review_attention_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        resolved_at: String,
        updated_at: String,
        review_key: String,
        owner_wait_key: String,
    ) -> crate::Result<u64> {
        let result = sqlx::query("UPDATE attention_projection SET status = 'resolved', resolved_at = ?, updated_at = ?, version = version + 1
        WHERE status <> 'resolved' AND dedupe_key IN (?, ?)")
.bind(resolved_at)
.bind(updated_at)
.bind(review_key)
.bind(owner_wait_key)
.execute(connection)
.await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn resolve_expired_placement_wait_attention_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        resolved_at: String,
        updated_at: String,
        owner_wait_key: String,
        environment_wait_key: String,
    ) -> crate::Result<u64> {
        let result = sqlx::query("UPDATE attention_projection SET status = 'resolved', resolved_at = ?, updated_at = ?, version = version + 1 WHERE dedupe_key IN (?, ?) AND status <> 'resolved'")
.bind(resolved_at)
.bind(updated_at)
.bind(owner_wait_key)
.bind(environment_wait_key)
.execute(connection)
.await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn resolve_claim_placement_wait_attention_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        resolved_at: String,
        updated_at: String,
        owner_wait_key: String,
        environment_wait_key: String,
    ) -> crate::Result<u64> {
        let result = sqlx::query("UPDATE attention_projection SET status = 'resolved', resolved_at = ?, updated_at = ?, version = version + 1
            WHERE dedupe_key IN (?, ?) AND status <> 'resolved'")
.bind(resolved_at)
.bind(updated_at)
.bind(owner_wait_key)
.bind(environment_wait_key)
.execute(connection)
.await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn open_project_autonomy_incident_count(
        &self,
        project_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*) FROM attention_projection
             WHERE scope_type = 'project' AND scope_id = ? AND status = 'open'",
        )
        .bind(project_id)
        .fetch_one(self.pool())
        .await?)
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct WakeSweepCandidateRow {
    pub id: crate::Result<String>,
    pub scope_type: crate::Result<String>,
    pub scope_id: crate::Result<String>,
    pub latest_digest: crate::Result<Option<String>>,
    pub latest_disposition: crate::Result<Option<String>>,
    pub latest_reason: crate::Result<String>,
    pub latest_identity: crate::Result<Option<String>>,
}

impl SqliteDb {
    pub async fn open_wake_sweep_candidates(
        &self,
        now: &str,
    ) -> crate::Result<Vec<WakeSweepCandidateRow>> {
        let rows = sqlx::query(
            "SELECT a.id, a.scope_type, a.scope_id, l.incident_digest AS latest_digest,
                    l.disposition AS latest_disposition, l.reason AS latest_reason,
                    l.identity_id AS latest_identity
             FROM attention_projection a
             LEFT JOIN agent_wake_attention_latest l ON l.attention_id = a.id
             WHERE a.status = 'open'
               AND a.recommended_action NOT IN ('answer_escalation', 'answer_lead_input')
               AND (a.snoozed_until IS NULL OR a.snoozed_until <= ?)
             ORDER BY a.scope_type, a.scope_id, a.occurred_at, a.id",
        )
        .bind(now)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(WakeSweepCandidateRow {
                    id: row.try_get("id").map_err(crate::DbError::from),
                    scope_type: row.try_get("scope_type").map_err(crate::DbError::from),
                    scope_id: row.try_get("scope_id").map_err(crate::DbError::from),
                    latest_digest: row.try_get("latest_digest").map_err(crate::DbError::from),
                    latest_disposition: row
                        .try_get("latest_disposition")
                        .map_err(crate::DbError::from),
                    latest_reason: row.try_get("latest_reason").map_err(crate::DbError::from),
                    latest_identity: row.try_get("latest_identity").map_err(crate::DbError::from),
                })
            })
            .collect()
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct WakeIncidentContextRow {
    pub id: crate::Result<String>,
    pub attention_type: crate::Result<String>,
    pub scope_type: crate::Result<String>,
    pub scope_id: crate::Result<String>,
    pub status: crate::Result<String>,
    pub source_event_id: crate::Result<String>,
    pub source_sequence: crate::Result<Option<i64>>,
    pub details_json: crate::Result<String>,
    pub recommended_action: crate::Result<String>,
    pub version: crate::Result<i64>,
}

impl SqliteDb {
    pub async fn wake_incident_context(
        &self,
        incident_key: &str,
    ) -> crate::Result<Option<WakeIncidentContextRow>> {
        let rows = sqlx::query(
            "SELECT id, attention_type, scope_type, scope_id, status,
                    source_event_id, source_sequence, details_json,
                    recommended_action, version
             FROM attention_projection WHERE dedupe_key = ?",
        )
        .bind(incident_key)
        .fetch_optional(self.pool())
        .await?;
        rows.map(|row| {
            Ok(WakeIncidentContextRow {
                id: row.try_get("id").map_err(crate::DbError::from),
                attention_type: row.try_get("attention_type").map_err(crate::DbError::from),
                scope_type: row.try_get("scope_type").map_err(crate::DbError::from),
                scope_id: row.try_get("scope_id").map_err(crate::DbError::from),
                status: row.try_get("status").map_err(crate::DbError::from),
                source_event_id: row.try_get("source_event_id").map_err(crate::DbError::from),
                source_sequence: row.try_get("source_sequence").map_err(crate::DbError::from),
                details_json: row.try_get("details_json").map_err(crate::DbError::from),
                recommended_action: row
                    .try_get("recommended_action")
                    .map_err(crate::DbError::from),
                version: row.try_get("version").map_err(crate::DbError::from),
            })
        })
        .transpose()
    }
}

impl SqliteDb {
    pub async fn wake_attention_status_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        attention_id: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar::<_, String>("SELECT status FROM attention_projection WHERE id = ?")
                .bind(attention_id)
                .fetch_optional(connection)
                .await?,
        )
    }
}

impl SqliteDb {
    pub async fn wake_incident_status_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        incident_key: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT status FROM attention_projection WHERE dedupe_key = ?",
        )
        .bind(incident_key)
        .fetch_optional(connection)
        .await?)
    }
}

impl SqliteDb {
    pub async fn resolve_stale_wake_attention_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        resolved_at: &str,
        updated_at: &str,
        attention_id: &str,
        expected_version: Option<i64>,
    ) -> crate::Result<u64> {
        let result = sqlx::query(
            "UPDATE attention_projection
                         SET status = 'resolved', resolved_at = ?, snoozed_until = NULL,
                             updated_at = ?, version = version + 1
                         WHERE id = ? AND version = ? AND status <> 'resolved'",
        )
        .bind(resolved_at)
        .bind(updated_at)
        .bind(attention_id)
        .bind(expected_version)
        .execute(connection)
        .await?;
        Ok(result.rows_affected())
    }
}

impl SqliteDb {
    pub async fn open_project_wake_incident_count_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        project_id: &str,
    ) -> crate::Result<i64> {
        Ok(sqlx::query_scalar("SELECT COUNT(*) FROM attention_projection WHERE scope_type = 'project' AND scope_id = ? AND status = 'open'")
.bind(project_id)
.fetch_one(connection)
.await?)
    }
}

/// Typed fields retain decoding failures for the caller's original conditional/error handling.
#[derive(Debug)]
pub struct AttentionWorkerHealthRow {
    pub last_error_kind: crate::Result<Option<String>>,
    pub last_error: crate::Result<Option<String>>,
    pub last_sequence: crate::Result<i64>,
    pub last_success_at: crate::Result<Option<String>>,
    pub updated_at: crate::Result<String>,
}

impl SqliteDb {
    pub async fn attention_worker_health(
        &self,
        consumer_name: &str,
    ) -> crate::Result<Option<AttentionWorkerHealthRow>> {
        let rows = sqlx::query("SELECT h.*, COALESCE(c.last_sequence, 0) AS last_sequence
            FROM worker_health h LEFT JOIN event_consumer_cursor c ON c.consumer_name = h.worker_name WHERE h.worker_name = ?")
.bind(consumer_name)
.fetch_optional(self.pool())
.await?;
        rows.map(|row| {
            Ok(AttentionWorkerHealthRow {
                last_error_kind: row.try_get("last_error_kind").map_err(crate::DbError::from),
                last_error: row.try_get("last_error").map_err(crate::DbError::from),
                last_sequence: row.try_get("last_sequence").map_err(crate::DbError::from),
                last_success_at: row.try_get("last_success_at").map_err(crate::DbError::from),
                updated_at: row.try_get("updated_at").map_err(crate::DbError::from),
            })
        })
        .transpose()
    }
}

// Preserve the original tuple query's positional decoding and error indices.
impl<'r> sqlx::FromRow<'r, SqliteRow> for FailedTurnIncidentRow {
    fn from_row(row: &'r SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(Self {
            dedupe_key: row.try_get(0)?,
            entity_id: row.try_get(1)?,
            id: row.try_get(2)?,
            payload_json: row.try_get(3)?,
        })
    }
}
#[derive(Debug)]
pub struct FailedTurnIncidentRow {
    pub dedupe_key: String,
    pub entity_id: String,
    pub id: String,
    pub payload_json: String,
}

impl SqliteDb {
    pub async fn unresolved_failed_turn_incidents(
        &self,
    ) -> crate::Result<Vec<FailedTurnIncidentRow>> {
        Ok(sqlx::query_as::<_, FailedTurnIncidentRow>(
            "SELECT a.dedupe_key, e.entity_id, e.id, e.payload_json FROM attention_projection a
             JOIN domain_event e ON e.id = a.source_event_id
             WHERE a.status <> 'resolved' AND e.event_type = 'agent_chat.turn.failed'",
        )
        .fetch_all(self.pool())
        .await?)
    }
}

#[derive(Debug)]
pub struct ResolveScopeProgressAttentionInTxInput<'a> {
    pub resolved_at: &'a str,
    pub source_event_id: &'a str,
    pub updated_at: &'a str,
    pub scope_type: &'a str,
    pub scope_id: &'a str,
}

impl SqliteDb {
    pub async fn resolve_scope_progress_attention_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: ResolveScopeProgressAttentionInTxInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query("UPDATE attention_projection SET status = 'resolved', resolved_at = ?, snoozed_until = NULL,
                source_event_id = ?, updated_at = ?, version = version + 1
                WHERE attention_type = 'delivery_followup' AND scope_type = ? AND scope_id = ? AND status <> 'resolved'")
.bind(input.resolved_at)
.bind(input.source_event_id)
.bind(input.updated_at)
.bind(input.scope_type)
.bind(input.scope_id)
.execute(connection)
.await?;
        Ok(result.rows_affected())
    }
}

#[derive(Debug)]
pub struct UpsertEnvironmentWaitAttentionInTxInput<'a> {
    pub id: String,
    pub project_id: &'a str,
    pub event_id: String,
    pub summary: String,
    pub details_json: String,
    pub dedupe_key: String,
    pub occurred_at: &'a str,
    pub updated_at: &'a str,
}

impl SqliteDb {
    pub async fn upsert_environment_wait_attention_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: UpsertEnvironmentWaitAttentionInTxInput<'_>,
    ) -> crate::Result<u64> {
        let result = sqlx::query("INSERT INTO attention_projection (id, attention_type, scope_type, scope_id, source_event_id, priority, status, summary, details_json, dedupe_key, occurred_at, updated_at, recommended_action) VALUES (?, 'environment_not_ready', 'project', ?, ?, 80, 'open', ?, ?, ?, ?, ?, 'wait') ON CONFLICT(dedupe_key) DO UPDATE SET status = 'open', summary = excluded.summary, details_json = excluded.details_json, source_event_id = excluded.source_event_id, resolved_at = NULL, acknowledged_at = NULL, snoozed_until = NULL, updated_at = excluded.updated_at, version = attention_projection.version + 1")
            .bind(input.id)
            .bind(input.project_id)
            .bind(input.event_id)
            .bind(input.summary)
            .bind(input.details_json)
            .bind(input.dedupe_key)
            .bind(input.occurred_at)
            .bind(input.updated_at)
            .execute(connection)
            .await?;
        Ok(result.rows_affected())
    }
}

pub struct UpsertPlacementRefusalAttentionInTxInput<'a> {
    pub id: String,
    pub project_id: &'a str,
    pub event_id: String,
    pub summary: &'a str,
    pub details_json: String,
    pub dedupe_key: String,
    pub occurred_at: &'a str,
    pub updated_at: &'a str,
}

impl SqliteDb {
    pub async fn upsert_placement_refusal_attention_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: UpsertPlacementRefusalAttentionInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("INSERT INTO attention_projection (id, attention_type, scope_type, scope_id, source_event_id, priority, status, summary, details_json, dedupe_key, occurred_at, updated_at, recommended_action) VALUES (?, 'human_input_required', 'project', ?, ?, 80, 'open', ?, ?, ?, ?, ?, 'configure_environment') ON CONFLICT(dedupe_key) DO UPDATE SET status='open', summary=excluded.summary, details_json=excluded.details_json, updated_at=excluded.updated_at, resolved_at=NULL, version=attention_projection.version+1").bind(input.id).bind(input.project_id).bind(input.event_id).bind(input.summary).bind(input.details_json).bind(input.dedupe_key).bind(input.occurred_at).bind(input.updated_at).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn resolve_placement_refusal_attention(
        &self,
        resolved_at: String,
        updated_at: String,
        environment_dedupe_key: String,
        provision_dedupe_key: String,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE attention_projection SET status='resolved', resolved_at=?, updated_at=?, version=version+1 WHERE dedupe_key IN (?,?) AND status<>'resolved'").bind(resolved_at).bind(updated_at).bind(environment_dedupe_key).bind(provision_dedupe_key).execute(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn resolve_execution_plan_wait_attention(
        &self,
        resolved_at: String,
        updated_at: String,
        dedupe_key: String,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE attention_projection SET status='resolved', resolved_at=?, updated_at=? WHERE dedupe_key=? AND status<>'resolved'").bind(resolved_at).bind(updated_at).bind(dedupe_key).execute(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn resolve_lead_input_attention(
        &self,
        now: &str,
        dedupe_key: String,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
        "UPDATE attention_projection SET status='resolved', resolved_at=?, updated_at=?, version=version+1
         WHERE dedupe_key=? AND status!='resolved'",
    ).bind(now).bind(now).bind(dedupe_key).execute(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn lead_input_attention_status_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        dedupe_key: &str,
    ) -> crate::Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT status FROM attention_projection WHERE dedupe_key=?")
                .bind(dedupe_key)
                .fetch_optional(connection)
                .await?,
        )
    }
}

pub struct ResolveWorkspaceAdmissionAttentionInTxInput<'a> {
    pub resolved_at: &'a str,
    pub updated_at: &'a str,
    pub review_dedupe_key: String,
    pub owner_dedupe_key: String,
    pub environment_dedupe_key: String,
    pub task_id: &'a str,
}

impl SqliteDb {
    pub async fn resolve_workspace_admission_attention_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: ResolveWorkspaceAdmissionAttentionInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE attention_projection SET status = 'resolved', resolved_at = ?, updated_at = ?, version = version + 1
        WHERE status <> 'resolved' AND (dedupe_key IN (?, ?, ?) OR (dedupe_key LIKE 'workspace-fence:%' AND json_extract(details_json, '$.task.id') = ?))").bind(input.resolved_at).bind(input.updated_at).bind(input.review_dedupe_key).bind(input.owner_dedupe_key).bind(input.environment_dedupe_key).bind(input.task_id).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn resolve_workspace_review_ci_attention(
        &self,
        now: &str,
        dedupe_key: String,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE attention_projection SET status = 'resolved', resolved_at = ?, updated_at = ?, version = version + 1 WHERE status <> 'resolved' AND dedupe_key = ?").bind(now).bind(now).bind(dedupe_key).execute(self.pool()).await?)
    }
}

pub struct RecordWorkspaceWaitAttentionInTxInput<'a> {
    pub id: String,
    pub kind: &'a str,
    pub project_id: &'a str,
    pub event_id: &'a str,
    pub reason: &'a str,
    pub details_json: String,
    pub dedupe_key: &'a str,
    pub occurred_at: &'a str,
    pub updated_at: &'a str,
    pub sequence: i64,
}

impl SqliteDb {
    pub async fn record_workspace_wait_attention_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        input: RecordWorkspaceWaitAttentionInTxInput<'_>,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("INSERT INTO attention_projection (id, attention_type, scope_type, scope_id,
        source_event_id, priority, status, summary, details_json, dedupe_key, occurred_at,
        updated_at, recommended_action, source_sequence)
        VALUES (?, ?, 'project', ?, ?, 80, 'open', ?, ?, ?, ?, ?, 'recover_task', ?)
        ON CONFLICT(dedupe_key) DO UPDATE SET summary = excluded.summary, details_json = excluded.details_json,
        source_event_id = excluded.source_event_id, status = 'open', resolved_at = NULL, acknowledged_at = NULL, snoozed_until = NULL,
        updated_at = excluded.updated_at, version = attention_projection.version + 1").bind(input.id).bind(input.kind).bind(input.project_id).bind(input.event_id).bind(input.reason).bind(input.details_json).bind(input.dedupe_key).bind(input.occurred_at).bind(input.updated_at).bind(input.sequence).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn away_repository_cleanup_raised_at(
        &self,
        dedupe_key: &str,
    ) -> crate::Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            "SELECT occurred_at FROM attention_projection
             WHERE dedupe_key = ? AND status <> 'resolved'
             ORDER BY occurred_at LIMIT 1",
        )
        .bind(dedupe_key)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn settle_away_repository_cleanup_attention(
        &self,
        evidence: &str,
        updated_at: String,
        dedupe_key: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query(
            "UPDATE attention_projection
             SET details_json = json_set(details_json, '$.settled', ?), updated_at = ?,
                 version = version + 1
             WHERE dedupe_key = ? AND status <> 'resolved'",
        )
        .bind(evidence)
        .bind(updated_at)
        .bind(dedupe_key)
        .execute(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn open_retired_auditor_notice_keys(&self) -> crate::Result<Vec<String>> {
        Ok(sqlx::query_scalar(
        "SELECT dedupe_key FROM attention_projection WHERE status <> 'resolved' AND dedupe_key LIKE 'retired-auditor:%'",
    ).fetch_all(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn acknowledge_retired_auditor_notice_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        dedupe_key: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE attention_projection SET status = 'acknowledged', acknowledged_at = updated_at WHERE dedupe_key = ? AND status = 'open'").bind(dedupe_key).execute(connection).await?)
    }
}

impl SqliteDb {
    pub async fn resolve_retired_auditor_notice(
        &self,
        now: &str,
        dedupe_key: &str,
    ) -> crate::Result<sqlx::sqlite::SqliteQueryResult> {
        Ok(sqlx::query("UPDATE attention_projection SET status = 'resolved', resolved_at = ?, updated_at = ?, version = version + 1 WHERE status <> 'resolved' AND dedupe_key = ?").bind(now).bind(now).bind(dedupe_key).execute(self.pool()).await?)
    }
}

impl SqliteDb {
    pub async fn domain_event_relay_error_status(
        &self,
        worker_name: &str,
    ) -> crate::Result<Option<(Option<String>, Option<String>)>> {
        Ok(sqlx::query_as(
            "SELECT last_error, last_error_at FROM worker_health WHERE worker_name = ?",
        )
        .bind(worker_name)
        .fetch_optional(self.pool())
        .await?)
    }
}

impl SqliteDb {
    pub async fn conflict_hotspot_attention_resolution_in_tx(
        &self,
        connection: &mut sqlx::SqliteConnection,
        dedupe_key: String,
    ) -> crate::Result<Option<(String, Option<String>)>> {
        Ok(sqlx::query_as(
            "SELECT status, resolved_at FROM attention_projection WHERE dedupe_key = ?",
        )
        .bind(dedupe_key)
        .fetch_optional(connection)
        .await?)
    }
}

impl SqliteDb {
    /// Merge occurrence/task sets transactionally; hook redelivery counts once.
    pub async fn record_ci_triage_attention(
        &self,
        mut input: CreateAttentionProjection,
        occurrence: &str,
        task: &str,
    ) -> Result<()> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let mut details: serde_json::Value =
            serde_json::from_str(&input.details_json).map_err(|e| DbError::Check(e.to_string()))?;
        // Reuse the command incident even if an earlier build keyed it by
        // base commit. Existing rows are retained as resolved history.
        let old = if input.attention_type == "base_check_failing" {
            sqlx::query("SELECT * FROM attention_projection WHERE scope_type='project' AND scope_id=? AND attention_type='base_check_failing' AND json_extract(details_json,'$.command')=? ORDER BY julianday(occurred_at) DESC,id LIMIT 1")
                .bind(&input.scope_id).bind(details["command"].as_str()).fetch_optional(&mut *tx).await?
        } else {
            sqlx::query("SELECT * FROM attention_projection WHERE dedupe_key=?")
                .bind(&input.dedupe_key).fetch_optional(&mut *tx).await?
        }.map(map_attention_projection).transpose()?;
        if let Some(old) = old
            .as_ref()
            .filter(|_| input.attention_type == "base_check_failing")
        {
            input.dedupe_key = old.dedupe_key.clone();
            sqlx::query("UPDATE attention_projection SET status='resolved',resolved_at=?,updated_at=?,source_event_id=?,version=version+1 WHERE scope_type='project' AND scope_id=? AND attention_type='base_check_failing' AND json_extract(details_json,'$.command')=? AND id<>? AND status<>'resolved'")
                .bind(&input.updated_at).bind(&input.updated_at).bind(&input.source_event_id).bind(&input.scope_id).bind(details["command"].as_str()).bind(&old.id).execute(&mut *tx).await?;
        }
        if input.attention_type == "base_check_failing" {
            if let Some(observed) = details["base_observed_at"].as_str() {
                input.occurred_at = observed.to_owned();
            }
            // Cached old red evidence must neither replace a newer base nor
            // reopen an incident settled by a newer passing base.
            let observed = chrono::DateTime::parse_from_rfc3339(&input.occurred_at)
                .map_err(|error| DbError::Check(error.to_string()))?;
            if old.as_ref().is_some_and(|old| {
                chrono::DateTime::parse_from_rfc3339(&old.occurred_at).is_ok_and(|at| observed < at)
                    || old.resolved_at.as_ref().is_some_and(|at| {
                        chrono::DateTime::parse_from_rfc3339(at).is_ok_and(|at| observed <= at)
                    })
            }) {
                tx.commit().await?;
                return Ok(());
            }
        }
        let old: serde_json::Value = old
            .and_then(|row| serde_json::from_str(&row.details_json).ok())
            .unwrap_or_default();
        // A flaky test can recur for months and its item never resolves: the
        // count is a counter, and only the latest occurrences and Tasks are
        // kept (a redelivery arrives right after its first delivery).
        const RECENT: usize = 64;
        let mut occurrences: Vec<String> =
            serde_json::from_value(old["occurrences"].clone()).unwrap_or_default();
        if occurrences.iter().any(|seen| seen == occurrence) {
            tx.commit().await?;
            return Ok(());
        }
        let count = old["count"]
            .as_u64()
            .unwrap_or(occurrences.len() as u64)
            .saturating_add(1);
        occurrences.push(occurrence.to_owned());
        occurrences.drain(..occurrences.len().saturating_sub(RECENT));
        let mut tasks: Vec<String> =
            serde_json::from_value(old["task_ids"].clone()).unwrap_or_default();
        tasks.retain(|seen| seen != task);
        tasks.push(task.to_owned());
        tasks.drain(..tasks.len().saturating_sub(RECENT));
        details["count"] = serde_json::json!(count);
        details["occurrences"] = serde_json::json!(occurrences);
        details["task_ids"] = serde_json::json!(tasks);
        input.details_json = details.to_string();
        self.insert_attention_in_tx(&mut tx, input).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn resolve_passing_base_attention(
        &self,
        project: &str,
        command: &str,
        observed_at: &str,
        event: &str,
    ) -> Result<()> {
        sqlx::query("UPDATE attention_projection SET status='resolved',resolved_at=?,updated_at=?,source_event_id=?,version=version+1 WHERE scope_type='project' AND scope_id=? AND attention_type='base_check_failing' AND status<>'resolved' AND json_extract(details_json,'$.command')=? AND julianday(occurred_at)<julianday(?)")
            .bind(observed_at).bind(observed_at).bind(event).bind(project).bind(command).bind(observed_at).execute(self.pool()).await?;
        Ok(())
    }
}
