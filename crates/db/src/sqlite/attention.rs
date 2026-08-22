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
        .execute(&self.pool)
        .await?;

        sqlx::query("SELECT * FROM attention_projection WHERE dedupe_key = ?")
            .bind(&input.dedupe_key)
            .fetch_optional(&self.pool)
            .await?
            .map(map_attention_projection)
            .transpose()?
            .ok_or(DbError::NotFound)
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
        .execute(&self.pool)
        .await?;
        sqlx::query("SELECT * FROM attention_projection WHERE dedupe_key = ?")
            .bind(dedupe_key)
            .fetch_optional(&self.pool)
            .await?
            .map(map_attention_projection)
            .transpose()
    }

    async fn get_attention_consumer_health(
        &self,
        consumer_name: &str,
    ) -> Result<Option<AttentionConsumerHealth>> {
        sqlx::query("SELECT * FROM attention_consumer_health WHERE consumer_name = ?")
            .bind(consumer_name)
            .fetch_optional(&self.pool)
            .await?
            .map(map_attention_consumer_health)
            .transpose()
    }

    async fn upsert_attention_consumer_health(
        &self,
        input: UpsertAttentionConsumerHealth,
    ) -> Result<AttentionConsumerHealth> {
        sqlx::query(
            "INSERT INTO attention_consumer_health (
                consumer_name, last_sequence, last_started_at, last_success_at,
                last_error_at, last_error_code, last_error_message, lease_owner,
                lease_until, processed_events, version, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
             ON CONFLICT(consumer_name) DO UPDATE SET
                last_sequence = MAX(attention_consumer_health.last_sequence, excluded.last_sequence),
                last_started_at = COALESCE(excluded.last_started_at, attention_consumer_health.last_started_at),
                last_success_at = COALESCE(excluded.last_success_at, attention_consumer_health.last_success_at),
                last_error_at = excluded.last_error_at,
                last_error_code = excluded.last_error_code,
                last_error_message = excluded.last_error_message,
                lease_owner = excluded.lease_owner,
                lease_until = excluded.lease_until,
                processed_events = attention_consumer_health.processed_events + excluded.processed_events,
                version = attention_consumer_health.version + 1,
                updated_at = excluded.updated_at",
        )
        .bind(&input.consumer_name)
        .bind(input.last_sequence)
        .bind(input.last_started_at.as_deref())
        .bind(input.last_success_at.as_deref())
        .bind(input.last_error_at.as_deref())
        .bind(input.last_error_code.as_deref())
        .bind(input.last_error_message.as_deref())
        .bind(input.lease_owner.as_deref())
        .bind(input.lease_until.as_deref())
        .bind(input.processed_events_delta)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await?;
        self.get_attention_consumer_health(&input.consumer_name)
            .await?
            .ok_or(DbError::NotFound)
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

fn map_attention_consumer_health(row: SqliteRow) -> Result<AttentionConsumerHealth> {
    Ok(AttentionConsumerHealth {
        consumer_name: row.try_get("consumer_name")?,
        last_sequence: row.try_get("last_sequence")?,
        last_started_at: row.try_get("last_started_at")?,
        last_success_at: row.try_get("last_success_at")?,
        last_error_at: row.try_get("last_error_at")?,
        last_error_code: row.try_get("last_error_code")?,
        last_error_message: row.try_get("last_error_message")?,
        lease_owner: row.try_get("lease_owner")?,
        lease_until: row.try_get("lease_until")?,
        processed_events: row.try_get("processed_events")?,
        version: row.try_get("version")?,
        updated_at: row.try_get("updated_at")?,
    })
}
