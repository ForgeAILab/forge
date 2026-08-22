use super::agent_chat::admit_agent_chat_turn_in_tx;
use super::domain_event::map_domain_event;
use super::*;

#[async_trait]
impl AgentWakeDispositionRepo for SqliteDb {
    async fn get_agent_wake_disposition(
        &self,
        consumer_name: &str,
        source_event_id: &str,
        attempt_number: i64,
    ) -> Result<Option<AgentWakeDisposition>> {
        sqlx::query(
            "SELECT * FROM agent_wake_disposition
             WHERE consumer_name = ? AND source_event_id = ? AND attempt_number = ?",
        )
        .bind(consumer_name)
        .bind(source_event_id)
        .bind(attempt_number)
        .fetch_optional(&self.pool)
        .await?
        .map(map_agent_wake_disposition)
        .transpose()
    }

    async fn get_current_agent_wake_disposition(
        &self,
        consumer_name: &str,
        source_event_id: &str,
    ) -> Result<Option<AgentWakeDisposition>> {
        sqlx::query(
            "SELECT disposition.*
             FROM agent_wake_disposition_current AS current
             JOIN agent_wake_disposition AS disposition
               ON disposition.id = current.disposition_id
             WHERE current.consumer_name = ? AND current.source_event_id = ?",
        )
        .bind(consumer_name)
        .bind(source_event_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_agent_wake_disposition)
        .transpose()
    }

    async fn list_due_agent_wake_dispositions(
        &self,
        consumer_name: &str,
        now: &str,
        limit: i64,
    ) -> Result<Vec<AgentWakeDisposition>> {
        sqlx::query(
            "SELECT disposition.*
             FROM agent_wake_disposition_current AS current
             JOIN agent_wake_disposition AS disposition
               ON disposition.id = current.disposition_id
             WHERE current.consumer_name = ?
               AND disposition.disposition = 'deferred'
               AND disposition.retry_at IS NOT NULL
               AND disposition.retry_at <= ?
               AND disposition.attempt_number < disposition.max_attempts
             ORDER BY disposition.retry_at ASC, disposition.source_event_sequence ASC,
                      disposition.id ASC
             LIMIT ?",
        )
        .bind(consumer_name)
        .bind(now)
        .bind(limit.clamp(1, 100))
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_wake_disposition)
        .collect()
    }

    async fn list_reconsiderable_agent_wake_dispositions(
        &self,
        consumer_name: &str,
        now: &str,
        limit: i64,
    ) -> Result<Vec<AgentWakeDisposition>> {
        sqlx::query(
            "SELECT disposition.*
             FROM agent_wake_disposition_current AS current
             JOIN agent_wake_disposition AS disposition
               ON disposition.id = current.disposition_id
             LEFT JOIN attention_projection AS attention
               ON attention.id = disposition.attention_id
             WHERE current.consumer_name = ?
               AND disposition.attempt_number < disposition.max_attempts
               AND (
                   (disposition.disposition = 'deferred'
                    AND disposition.retry_at IS NOT NULL
                    AND disposition.retry_at <= ?)
                   OR
                   (disposition.disposition = 'setup_required'
                    AND attention.id IS NOT NULL
                    AND (
                        attention.updated_at > disposition.updated_at
                        OR EXISTS (
                            SELECT 1
                            FROM project_agent_binding AS binding
                            WHERE attention.scope_type = 'project'
                              AND binding.project_id = attention.scope_id
                              AND binding.updated_at > disposition.updated_at
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM account_main_agent_binding AS binding
                            WHERE attention.scope_type = 'account'
                              AND binding.account_id = attention.scope_id
                              AND binding.updated_at > disposition.updated_at
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM agent_chat AS chat
                            WHERE attention.scope_type = 'agent_chat'
                              AND chat.id = attention.scope_id
                              AND chat.updated_at > disposition.updated_at
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM agent_identity AS identity
                            WHERE identity.id = attention.identity_id
                              AND (
                                  identity.updated_at > disposition.updated_at
                                  OR (
                                      disposition.profile_id IS NOT NULL
                                      AND (
                                          identity.selected_profile_id <> disposition.profile_id
                                          OR identity.selected_profile_id IS NULL
                                      )
                                  )
                              )
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM agent_profile AS profile
                            WHERE profile.identity_id = attention.identity_id
                              AND (
                                  profile.updated_at > disposition.updated_at
                                  OR (
                                      disposition.profile_id IS NOT NULL
                                      AND profile.id = disposition.profile_id
                                      AND disposition.profile_version IS NOT NULL
                                      AND profile.version <> disposition.profile_version
                                  )
                              )
                        )
                    ))
               )
             ORDER BY COALESCE(disposition.retry_at, attention.updated_at) ASC,
                      disposition.source_event_sequence ASC, disposition.id ASC
             LIMIT ?",
        )
        .bind(consumer_name)
        .bind(now)
        .bind(limit.clamp(1, 100))
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_wake_disposition)
        .collect()
    }

    async fn complete_claimed_agent_wake(
        &self,
        input: CompleteClaimedWake,
    ) -> Result<AgentWakeDisposition> {
        validate_disposition_input(&input.disposition)?;
        if input.disposition.consumer_name != input.completion.consumer_name
            || input.disposition.source_event_id != input.completion.event_id
            || input.disposition.source_event_sequence != input.completion.event_sequence
        {
            return Err(DbError::Check(
                "wake disposition and claimed event identity must match".to_owned(),
            ));
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let event = load_claimed_event(&mut transaction, &input.completion).await?;
        if event.sequence != input.disposition.source_event_sequence {
            return Err(DbError::Check(
                "wake disposition source sequence does not match event".to_owned(),
            ));
        }

        if let Some(expected_attention) = input.expected_attention.as_ref() {
            validate_expected_attention_in_tx(&mut transaction, expected_attention).await?;
        }

        let mut admitted_turn = false;
        match input.disposition.disposition {
            AgentWakeDispositionKind::TurnAdmitted => {
                let admission = input.admission.clone().ok_or_else(|| {
                    DbError::Check(
                        "turn_admitted wake disposition requires an atomic turn admission"
                            .to_owned(),
                    )
                })?;
                if admission.turn.id != input.disposition.turn_job_id.as_deref().unwrap_or_default()
                    || admission.message.id != admission.turn.triggering_message_id
                {
                    return Err(DbError::Check(
                        "wake disposition turn link does not match admission".to_owned(),
                    ));
                }
                let admitted =
                    admit_agent_chat_turn_in_tx(self, &mut transaction, admission).await?;
                if admitted.turn.id != input.disposition.turn_job_id.as_deref().unwrap_or_default()
                {
                    return Err(DbError::IdempotencyConflict);
                }
                admitted_turn = true;
            }
            _ if input.admission.is_some() => {
                return Err(DbError::Check(
                    "only turn_admitted wake dispositions may carry admission".to_owned(),
                ));
            }
            _ => {}
        }

        let existing = sqlx::query(
            "SELECT * FROM agent_wake_disposition
             WHERE consumer_name = ? AND source_event_id = ? AND attempt_number = ?",
        )
        .bind(&input.disposition.consumer_name)
        .bind(&input.disposition.source_event_id)
        .bind(input.disposition.attempt_number)
        .fetch_optional(&mut *transaction)
        .await?;

        let disposition = if let Some(row) = existing {
            let existing = map_agent_wake_disposition(row)?;
            if !wake_disposition_semantics_match(&input.disposition, &existing) {
                return Err(DbError::IdempotencyConflict);
            }
            ensure_current_pointer(&mut transaction, &existing, &input.disposition.updated_at)
                .await?;
            existing
        } else {
            if input.disposition.attempt_number > 1 {
                let current = current_disposition_in_tx(
                    &mut transaction,
                    &input.disposition.consumer_name,
                    &input.disposition.source_event_id,
                )
                .await?
                .ok_or(DbError::VersionConflict)?;
                if input.disposition.parent_disposition_id.as_deref() != Some(current.id.as_str())
                    || !matches!(
                        current.disposition,
                        AgentWakeDispositionKind::Deferred
                            | AgentWakeDispositionKind::SetupRequired
                    )
                {
                    return Err(DbError::VersionConflict);
                }
            } else if input.disposition.parent_disposition_id.is_some()
                || current_disposition_in_tx(
                    &mut transaction,
                    &input.disposition.consumer_name,
                    &input.disposition.source_event_id,
                )
                .await?
                .is_some()
            {
                return Err(DbError::IdempotencyConflict);
            }

            if input.disposition.disposition == AgentWakeDispositionKind::TurnAdmitted
                && !admitted_turn
            {
                return Err(DbError::Check(
                    "turn admission must be committed with turn_admitted disposition".to_owned(),
                ));
            }

            insert_disposition(&mut transaction, &input.disposition).await?;
            ensure_current_pointer(
                &mut transaction,
                &input.disposition,
                &input.disposition.updated_at,
            )
            .await?;
            disposition_from_create(&input.disposition)
        };

        complete_event_in_tx(&mut transaction, &input.completion).await?;
        transaction.commit().await?;
        Ok(disposition)
    }

    async fn retry_agent_wake(
        &self,
        input: RetryAgentWakeDisposition,
    ) -> Result<AgentWakeDisposition> {
        validate_disposition_input(&input.disposition)?;
        if input.disposition.consumer_name.trim().is_empty()
            || input.disposition.source_event_id.trim().is_empty()
        {
            return Err(DbError::Check(
                "wake retry source identity must be non-empty".to_owned(),
            ));
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        if let Some(expected_attention) = input.expected_attention.as_ref() {
            validate_expected_attention_in_tx(&mut transaction, expected_attention).await?;
        }
        match input.disposition.disposition {
            AgentWakeDispositionKind::TurnAdmitted => {
                let admission = input.admission.clone().ok_or_else(|| {
                    DbError::Check(
                        "turn_admitted wake retry requires an atomic turn admission".to_owned(),
                    )
                })?;
                if admission.turn.id != input.disposition.turn_job_id.as_deref().unwrap_or_default()
                    || admission.message.id != admission.turn.triggering_message_id
                {
                    return Err(DbError::Check(
                        "wake retry turn link does not match admission".to_owned(),
                    ));
                }
                let admitted =
                    admit_agent_chat_turn_in_tx(self, &mut transaction, admission).await?;
                if admitted.turn.id != input.disposition.turn_job_id.as_deref().unwrap_or_default()
                {
                    return Err(DbError::IdempotencyConflict);
                }
            }
            _ if input.admission.is_some() => {
                return Err(DbError::Check(
                    "only turn_admitted wake retries may carry admission".to_owned(),
                ));
            }
            _ => {}
        }
        let existing = sqlx::query(
            "SELECT * FROM agent_wake_disposition
             WHERE consumer_name = ? AND source_event_id = ? AND attempt_number = ?",
        )
        .bind(&input.disposition.consumer_name)
        .bind(&input.disposition.source_event_id)
        .bind(input.disposition.attempt_number)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            let existing = map_agent_wake_disposition(row)?;
            if !wake_disposition_semantics_match(&input.disposition, &existing) {
                return Err(DbError::IdempotencyConflict);
            }
            transaction.commit().await?;
            return Ok(existing);
        }

        let current = current_disposition_with_pointer_in_tx(
            &mut transaction,
            &input.disposition.consumer_name,
            &input.disposition.source_event_id,
        )
        .await?
        .ok_or(DbError::NotFound)?;
        if current.disposition.disposition != AgentWakeDispositionKind::Deferred
            && current.disposition.disposition != AgentWakeDispositionKind::SetupRequired
        {
            return Err(DbError::InvalidTransition);
        }
        if current.id != input.expected_parent_id
            || input.disposition.parent_disposition_id.as_deref() != Some(current.id.as_str())
            || input.disposition.attempt_number != current.attempt_number + 1
            || input.disposition.max_attempts != current.max_attempts
        {
            return Err(DbError::VersionConflict);
        }
        if current.disposition.disposition == AgentWakeDispositionKind::Deferred
            && current
                .retry_at
                .as_deref()
                .is_none_or(|retry_at| retry_at > input.now.as_str())
        {
            return Err(DbError::Check("deferred wake retry is not due".to_owned()));
        }
        if current.attempt_number >= current.max_attempts {
            return Err(DbError::Check("wake retry budget is exhausted".to_owned()));
        }

        insert_disposition(&mut transaction, &input.disposition).await?;
        let updated = sqlx::query(
            "UPDATE agent_wake_disposition_current
             SET disposition_id = ?, attempt_number = ?, version = version + 1,
                 updated_at = ?
             WHERE consumer_name = ? AND source_event_id = ?
               AND disposition_id = ? AND version = ?",
        )
        .bind(&input.disposition.id)
        .bind(input.disposition.attempt_number)
        .bind(&input.disposition.updated_at)
        .bind(&input.disposition.consumer_name)
        .bind(&input.disposition.source_event_id)
        .bind(&input.expected_parent_id)
        .bind(current.version)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }

        let disposition = disposition_from_create(&input.disposition);
        transaction.commit().await?;
        Ok(disposition)
    }
}

async fn load_claimed_event(
    transaction: &mut Transaction<'_, Sqlite>,
    input: &CompleteDomainEvent,
) -> Result<DomainEvent> {
    let event = sqlx::query("SELECT * FROM domain_event WHERE sequence = ?")
        .bind(input.event_sequence)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::NotFound)
        .and_then(|row| map_domain_event(row).map_err(DbError::from))?;
    if event.id != input.event_id {
        return Err(DbError::Check(
            "event id does not match the claimed sequence".to_owned(),
        ));
    }
    let expected_dedupe = event.dedupe_key.clone().unwrap_or_else(|| event.id.clone());
    if expected_dedupe != input.dedupe_key {
        return Err(DbError::Check(
            "event dedupe key does not match the claimed event".to_owned(),
        ));
    }
    Ok(event)
}

/// Re-read the Attention materialization inside the same write transaction as
/// turn admission.  This closes the resolver-to-admission race even when a
/// caller supplied a stale version whose legacy projection upsert failed to
/// increment that version: the canonical source/scope evidence and digest
/// are checked as well.
async fn validate_expected_attention_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    expected: &ExpectedAttentionSnapshot,
) -> Result<()> {
    if expected.id.trim().is_empty()
        || expected.version < 1
        || expected.status.trim().is_empty()
        || expected.canonical_scope_type.trim().is_empty()
        || expected.canonical_scope_id.trim().is_empty()
        || expected.source_event_id.trim().is_empty()
        || expected.dedupe_key.trim().is_empty()
    {
        return Err(DbError::Check(
            "expected Attention snapshot is incomplete".to_owned(),
        ));
    }

    let row = sqlx::query(
        "SELECT id, attention_type, scope_type, scope_id, identity_id,
                source_event_id, priority, status, summary, details_json,
                dedupe_key, occurred_at, updated_at, version,
                acknowledged_at, snoozed_until, resolved_at,
                updated_by_user_id, recommended_action, source_sequence
         FROM attention_projection
         WHERE id = ?",
    )
    .bind(&expected.id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::VersionConflict)?;

    let actual_version: i64 = row.try_get("version")?;
    if actual_version != expected.version {
        return Err(DbError::VersionConflict);
    }
    let actual_status: String = row.try_get("status")?;
    if actual_status != expected.status {
        return Err(DbError::InvalidTransition);
    }
    let actual_scope_type: String = row.try_get("scope_type")?;
    let actual_scope_id: String = row.try_get("scope_id")?;
    let actual_source_event_id: String = row.try_get("source_event_id")?;
    let actual_source_sequence: Option<i64> = row.try_get("source_sequence")?;
    let actual_dedupe_key: String = row.try_get("dedupe_key")?;
    if actual_scope_type != expected.canonical_scope_type
        || actual_scope_id != expected.canonical_scope_id
        || actual_source_event_id != expected.source_event_id
        || actual_source_sequence != expected.source_sequence
        || actual_dedupe_key != expected.dedupe_key
    {
        return Err(DbError::VersionConflict);
    }

    if let Some(expected_digest) = expected.digest.as_deref() {
        if expected_digest != canonical_attention_digest_from_row(&row)? {
            return Err(DbError::VersionConflict);
        }
    }
    Ok(())
}

fn canonical_attention_digest_from_row(row: &SqliteRow) -> Result<String> {
    let attention = AttentionProjection {
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
    };
    Ok(canonical_attention_incident_digest(&attention))
}

async fn complete_event_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    input: &CompleteDomainEvent,
) -> Result<()> {
    let cursor = sqlx::query_scalar::<_, i64>(
        "SELECT last_sequence FROM event_consumer_cursor WHERE consumer_name = ?",
    )
    .bind(&input.consumer_name)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    if input.event_sequence > cursor + 1 {
        return Err(DbError::Check(
            "domain events must be checkpointed in sequence order".to_owned(),
        ));
    }

    let receipt = sqlx::query(
        "SELECT dedupe_key FROM event_projection_receipt
         WHERE consumer_name = ? AND event_id = ?",
    )
    .bind(&input.consumer_name)
    .bind(&input.event_id)
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(row) = receipt {
        let dedupe_key: String = row.try_get("dedupe_key")?;
        if dedupe_key != input.dedupe_key {
            return Err(DbError::IdempotencyConflict);
        }
    } else {
        let lease_owner = sqlx::query_scalar::<_, String>(
            "SELECT lease_owner FROM event_processing_lease
             WHERE consumer_name = ? AND event_sequence = ?",
        )
        .bind(&input.consumer_name)
        .bind(input.event_sequence)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        if lease_owner != input.lease_owner {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO event_projection_receipt (
                consumer_name, event_id, dedupe_key, processed_at
             ) VALUES (?, ?, ?, ?)",
        )
        .bind(&input.consumer_name)
        .bind(&input.event_id)
        .bind(&input.dedupe_key)
        .bind(&input.completed_at)
        .execute(&mut **transaction)
        .await
        .map_err(map_wake_write_error)?;
    }

    if input.event_sequence == cursor + 1 {
        let updated = sqlx::query(
            "UPDATE event_consumer_cursor
             SET last_sequence = ?, version = version + 1, updated_at = ?
             WHERE consumer_name = ? AND last_sequence = ?",
        )
        .bind(input.event_sequence)
        .bind(&input.completed_at)
        .bind(&input.consumer_name)
        .bind(cursor)
        .execute(&mut **transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
    }

    sqlx::query(
        "DELETE FROM event_processing_lease
         WHERE consumer_name = ? AND event_sequence = ? AND lease_owner = ?",
    )
    .bind(&input.consumer_name)
    .bind(input.event_sequence)
    .bind(&input.lease_owner)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_disposition(
    transaction: &mut Transaction<'_, Sqlite>,
    input: &CreateAgentWakeDisposition,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO agent_wake_disposition (
            id, consumer_name, source_event_id, source_event_sequence,
            attempt_number, max_attempts, disposition, reason, turn_job_id,
            attention_id, retry_at, incident_key, incident_digest, binding_id,
            binding_version, profile_id, profile_version, provenance_json,
            parent_disposition_id, created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.consumer_name)
    .bind(&input.source_event_id)
    .bind(input.source_event_sequence)
    .bind(input.attempt_number)
    .bind(input.max_attempts)
    .bind(input.disposition.to_string())
    .bind(&input.reason)
    .bind(input.turn_job_id.as_deref())
    .bind(input.attention_id.as_deref())
    .bind(input.retry_at.as_deref())
    .bind(input.incident_key.as_deref())
    .bind(input.incident_digest.as_deref())
    .bind(input.binding_id.as_deref())
    .bind(input.binding_version)
    .bind(input.profile_id.as_deref())
    .bind(input.profile_version)
    .bind(input.provenance_json.as_deref())
    .bind(input.parent_disposition_id.as_deref())
    .bind(&input.created_at)
    .bind(&input.updated_at)
    .execute(&mut **transaction)
    .await
    .map_err(map_wake_write_error)?;
    Ok(())
}

async fn ensure_current_pointer(
    transaction: &mut Transaction<'_, Sqlite>,
    disposition: &impl WakeDispositionIdentity,
    updated_at: &str,
) -> Result<()> {
    let current = sqlx::query(
        "SELECT disposition_id, version FROM agent_wake_disposition_current
         WHERE consumer_name = ? AND source_event_id = ?",
    )
    .bind(disposition.consumer_name())
    .bind(disposition.source_event_id())
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(row) = current {
        let current_id: String = row.try_get("disposition_id")?;
        if current_id != disposition.id() {
            return Err(DbError::IdempotencyConflict);
        }
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO agent_wake_disposition_current (
            consumer_name, source_event_id, disposition_id, attempt_number,
            updated_at, version
         ) VALUES (?, ?, ?, ?, ?, 1)",
    )
    .bind(disposition.consumer_name())
    .bind(disposition.source_event_id())
    .bind(disposition.id())
    .bind(disposition.attempt_number())
    .bind(updated_at)
    .execute(&mut **transaction)
    .await
    .map_err(map_wake_write_error)?;
    Ok(())
}

async fn current_disposition_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    consumer_name: &str,
    source_event_id: &str,
) -> Result<Option<AgentWakeDisposition>> {
    sqlx::query(
        "SELECT disposition.*
         FROM agent_wake_disposition_current AS current
         JOIN agent_wake_disposition AS disposition
           ON disposition.id = current.disposition_id
         WHERE current.consumer_name = ? AND current.source_event_id = ?",
    )
    .bind(consumer_name)
    .bind(source_event_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(map_agent_wake_disposition)
    .transpose()
}

async fn current_disposition_with_pointer_in_tx(
    transaction: &mut Transaction<'_, Sqlite>,
    consumer_name: &str,
    source_event_id: &str,
) -> Result<Option<AgentWakeDispositionWithPointer>> {
    sqlx::query(
        "SELECT disposition.*, current.version AS current_version
         FROM agent_wake_disposition_current AS current
         JOIN agent_wake_disposition AS disposition
           ON disposition.id = current.disposition_id
         WHERE current.consumer_name = ? AND current.source_event_id = ?",
    )
    .bind(consumer_name)
    .bind(source_event_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(map_agent_wake_disposition_with_pointer)
    .transpose()
}

#[derive(Debug)]
struct AgentWakeDispositionWithPointer {
    disposition: AgentWakeDisposition,
    id: String,
    attempt_number: i64,
    max_attempts: i64,
    retry_at: Option<String>,
    version: i64,
}

trait WakeDispositionIdentity {
    fn id(&self) -> &str;
    fn consumer_name(&self) -> &str;
    fn source_event_id(&self) -> &str;
    fn attempt_number(&self) -> i64;
}

impl WakeDispositionIdentity for AgentWakeDisposition {
    fn id(&self) -> &str {
        &self.id
    }
    fn consumer_name(&self) -> &str {
        &self.consumer_name
    }
    fn source_event_id(&self) -> &str {
        &self.source_event_id
    }
    fn attempt_number(&self) -> i64 {
        self.attempt_number
    }
}

impl WakeDispositionIdentity for CreateAgentWakeDisposition {
    fn id(&self) -> &str {
        &self.id
    }
    fn consumer_name(&self) -> &str {
        &self.consumer_name
    }
    fn source_event_id(&self) -> &str {
        &self.source_event_id
    }
    fn attempt_number(&self) -> i64 {
        self.attempt_number
    }
}

fn validate_disposition_input(input: &CreateAgentWakeDisposition) -> Result<()> {
    if input.id.trim().is_empty()
        || input.consumer_name.trim().is_empty()
        || input.source_event_id.trim().is_empty()
        || input.reason.trim().is_empty()
    {
        return Err(DbError::Check(
            "wake disposition identity and reason must be non-empty".to_owned(),
        ));
    }
    if input.attempt_number < 1
        || input.attempt_number > input.max_attempts
        || input.max_attempts > 16
    {
        return Err(DbError::Check(
            "wake disposition attempt budget is invalid".to_owned(),
        ));
    }
    if input.reason.len() > 512 {
        return Err(DbError::Check(
            "wake disposition reason exceeds 512 bytes".to_owned(),
        ));
    }
    if let Some(provenance) = input.provenance_json.as_deref() {
        if provenance.len() > 8192 || serde_json::from_str::<serde_json::Value>(provenance).is_err()
        {
            return Err(DbError::Check(
                "wake disposition provenance must be bounded valid JSON".to_owned(),
            ));
        }
    }
    let valid_shape = match input.disposition {
        AgentWakeDispositionKind::TurnAdmitted => {
            input.turn_job_id.is_some() && input.attention_id.is_none() && input.retry_at.is_none()
        }
        AgentWakeDispositionKind::DeterministicallySuppressed => {
            input.turn_job_id.is_none() && input.attention_id.is_none() && input.retry_at.is_none()
        }
        AgentWakeDispositionKind::Deferred => {
            input.turn_job_id.is_none() && input.attention_id.is_none() && input.retry_at.is_some()
        }
        AgentWakeDispositionKind::SetupRequired => {
            input.turn_job_id.is_none() && input.attention_id.is_some() && input.retry_at.is_none()
        }
    };
    if !valid_shape {
        return Err(DbError::Check(
            "wake disposition fields do not match its typed outcome".to_owned(),
        ));
    }
    Ok(())
}

fn wake_disposition_semantics_match(
    input: &CreateAgentWakeDisposition,
    existing: &AgentWakeDisposition,
) -> bool {
    existing.id == input.id
        && existing.consumer_name == input.consumer_name
        && existing.source_event_id == input.source_event_id
        && existing.source_event_sequence == input.source_event_sequence
        && existing.attempt_number == input.attempt_number
        && existing.max_attempts == input.max_attempts
        && existing.disposition == input.disposition
        && existing.reason == input.reason
        && existing.turn_job_id == input.turn_job_id
        && existing.attention_id == input.attention_id
        && existing.retry_at == input.retry_at
        && existing.incident_key == input.incident_key
        && existing.incident_digest == input.incident_digest
        && existing.binding_id == input.binding_id
        && existing.binding_version == input.binding_version
        && existing.profile_id == input.profile_id
        && existing.profile_version == input.profile_version
        && existing.provenance_json == input.provenance_json
        && existing.parent_disposition_id == input.parent_disposition_id
}

fn disposition_from_create(input: &CreateAgentWakeDisposition) -> AgentWakeDisposition {
    AgentWakeDisposition {
        id: input.id.clone(),
        consumer_name: input.consumer_name.clone(),
        source_event_id: input.source_event_id.clone(),
        source_event_sequence: input.source_event_sequence,
        attempt_number: input.attempt_number,
        max_attempts: input.max_attempts,
        disposition: input.disposition,
        reason: input.reason.clone(),
        turn_job_id: input.turn_job_id.clone(),
        attention_id: input.attention_id.clone(),
        retry_at: input.retry_at.clone(),
        incident_key: input.incident_key.clone(),
        incident_digest: input.incident_digest.clone(),
        binding_id: input.binding_id.clone(),
        binding_version: input.binding_version,
        profile_id: input.profile_id.clone(),
        profile_version: input.profile_version,
        provenance_json: input.provenance_json.clone(),
        parent_disposition_id: input.parent_disposition_id.clone(),
        created_at: input.created_at.clone(),
        updated_at: input.updated_at.clone(),
        version: 1,
    }
}

fn map_agent_wake_disposition(row: SqliteRow) -> Result<AgentWakeDisposition> {
    map_agent_wake_disposition_ref(&row)
}

fn map_agent_wake_disposition_ref(row: &SqliteRow) -> Result<AgentWakeDisposition> {
    Ok(AgentWakeDisposition {
        id: row.try_get("id")?,
        consumer_name: row.try_get("consumer_name")?,
        source_event_id: row.try_get("source_event_id")?,
        source_event_sequence: row.try_get("source_event_sequence")?,
        attempt_number: row.try_get("attempt_number")?,
        max_attempts: row.try_get("max_attempts")?,
        disposition: row
            .try_get::<String, _>("disposition")?
            .parse()
            .map_err(DbError::Check)?,
        reason: row.try_get("reason")?,
        turn_job_id: row.try_get("turn_job_id")?,
        attention_id: row.try_get("attention_id")?,
        retry_at: row.try_get("retry_at")?,
        incident_key: row.try_get("incident_key")?,
        incident_digest: row.try_get("incident_digest")?,
        binding_id: row.try_get("binding_id")?,
        binding_version: row.try_get("binding_version")?,
        profile_id: row.try_get("profile_id")?,
        profile_version: row.try_get("profile_version")?,
        provenance_json: row.try_get("provenance_json")?,
        parent_disposition_id: row.try_get("parent_disposition_id")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        version: row.try_get("version")?,
    })
}

fn map_agent_wake_disposition_with_pointer(
    row: SqliteRow,
) -> Result<AgentWakeDispositionWithPointer> {
    Ok(AgentWakeDispositionWithPointer {
        disposition: map_agent_wake_disposition_ref(&row)?,
        id: row.try_get("id")?,
        attempt_number: row.try_get("attempt_number")?,
        max_attempts: row.try_get("max_attempts")?,
        retry_at: row.try_get("retry_at")?,
        version: row.try_get("current_version")?,
    })
}

fn map_wake_write_error(error: sqlx::Error) -> DbError {
    let message = error.to_string();
    if message.to_ascii_lowercase().contains("unique") {
        DbError::IdempotencyConflict
    } else if message.to_ascii_lowercase().contains("check") {
        DbError::Check("wake disposition constraint failed".to_owned())
    } else {
        error.into()
    }
}
