//! Agent Chat, message, turn job, and handoff repositories.

use super::*;

#[async_trait]
impl AgentChatRepo for SqliteDb {
    async fn get_agent_chat(&self, id: &str) -> Result<Option<AgentChat>> {
        sqlx::query("SELECT * FROM agent_chat WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent_chat)
            .transpose()
    }

    async fn get_main_chat(&self, account_id: &str) -> Result<Option<AgentChat>> {
        sqlx::query(
            "SELECT * FROM agent_chat
             WHERE kind = 'account_main' AND account_id = ?",
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_agent_chat)
        .transpose()
    }

    async fn get_project_chat(&self, project_id: &str) -> Result<Option<AgentChat>> {
        sqlx::query("SELECT * FROM agent_chat WHERE kind = 'project' AND project_id = ?")
            .bind(project_id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent_chat)
            .transpose()
    }

    async fn list_agent_chats(&self, account_id: &str) -> Result<Vec<AgentChat>> {
        sqlx::query(
            "SELECT chat.*
             FROM agent_chat AS chat
             LEFT JOIN project ON project.id = chat.project_id
             WHERE (chat.kind = 'account_main' AND chat.account_id = ?)
                OR (chat.kind = 'project' AND (
                    project.owner_id = ?
                    OR EXISTS (
                        SELECT 1 FROM project_member AS member
                        WHERE member.project_id = chat.project_id
                          AND member.user_id = ?
                    )
                ))
             ORDER BY CASE WHEN chat.kind = 'account_main' THEN 0 ELSE 1 END,
                      chat.updated_at DESC, chat.id ASC",
        )
        .bind(account_id)
        .bind(account_id)
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_chat)
        .collect()
    }

    async fn create_agent_chat(&self, input: CreateAgentChat) -> Result<AgentChat> {
        sqlx::query(
            "INSERT INTO agent_chat (
                id, kind, account_id, project_id, status,
                instruction_revision, message_count, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, 0, 1, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.kind)
        .bind(input.account_id.as_deref())
        .bind(input.project_id.as_deref())
        .bind(&input.status)
        .bind(input.instruction_revision)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_chat_write_error)?;

        self.get_agent_chat(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn update_agent_chat(&self, input: UpdateAgentChat) -> Result<AgentChat> {
        let current = self
            .get_agent_chat(&input.id)
            .await?
            .ok_or(DbError::NotFound)?;
        let status = input.status.unwrap_or(current.status);
        let instruction_revision = input
            .instruction_revision
            .unwrap_or(current.instruction_revision);
        let updated = sqlx::query(
            "UPDATE agent_chat
             SET status = ?, instruction_revision = ?, version = version + 1,
                 updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(status)
        .bind(instruction_revision)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&self.pool)
        .await?;
        if updated.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        self.get_agent_chat(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn list_chat_source_refs(&self, chat_id: &str) -> Result<Vec<AgentChatSourceRef>> {
        sqlx::query(
            "SELECT * FROM agent_chat_source_ref
             WHERE chat_id = ? ORDER BY source_type ASC, source_id ASC",
        )
        .bind(chat_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_chat_source_ref)
        .collect()
    }

    async fn list_chat_instructions(
        &self,
        chat_id: &str,
    ) -> Result<Vec<AgentChatInstructionRevision>> {
        sqlx::query(
            "SELECT * FROM agent_chat_instruction_revision
             WHERE chat_id = ? ORDER BY revision DESC, source_type ASC, source_id ASC",
        )
        .bind(chat_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_chat_instruction)
        .collect()
    }
}

#[async_trait]
impl AgentChatMessageRepo for SqliteDb {
    async fn get_agent_chat_message(&self, id: &str) -> Result<Option<AgentChatMessage>> {
        sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent_chat_message)
            .transpose()
    }

    async fn list_agent_chat_messages(
        &self,
        query: AgentChatMessageListQuery,
    ) -> Result<Page<AgentChatMessage>> {
        let offset = decode_offset(&query.page.cursor)?;
        let rows = if let Some(before_sequence) = query.before_sequence {
            sqlx::query(
                "SELECT * FROM agent_chat_message
                 WHERE chat_id = ? AND sequence < ?
                 ORDER BY sequence DESC LIMIT ? OFFSET ?",
            )
            .bind(&query.chat_id)
            .bind(before_sequence)
            .bind(limit(&query.page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT * FROM agent_chat_message
                 WHERE chat_id = ? ORDER BY sequence DESC
                 LIMIT ? OFFSET ?",
            )
            .bind(&query.chat_id)
            .bind(limit(&query.page) + 1)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?
        };
        let mut items = rows
            .into_iter()
            .map(map_agent_chat_message)
            .collect::<Result<Vec<_>>>()?;
        let page_limit = limit(&query.page) as usize;
        let has_next = items.len() > page_limit;
        if has_next {
            items.truncate(page_limit);
        }
        items.reverse();
        Ok(Page {
            items,
            next_cursor: if has_next {
                Some(encode_offset(offset + page_limit as i64)?)
            } else {
                None
            },
            total_count: None,
        })
    }

    async fn append_agent_chat_message(
        &self,
        input: CreateAgentChatMessage,
    ) -> Result<AgentChatMessage> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        // The message id is the idempotency key: a replay with the same id
        // returns the stored row without consuming another sequence.
        if let Some(existing) = sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
            .bind(&input.id)
            .fetch_optional(&mut *transaction)
            .await?
        {
            transaction.rollback().await?;
            return map_agent_chat_message(existing);
        }
        let sequence =
            allocate_chat_sequence(&mut transaction, &input.chat_id, &input.created_at).await?;
        let input = CreateAgentChatMessage { sequence, ..input };
        let message = insert_chat_message(&mut transaction, &input).await?;
        crate::sqlite::chat_read_events::append(
            self,
            &mut transaction,
            crate::sqlite::chat_read_events::ChatReadEvent {
                event_type: "agent_chat.message.appended",
                entity_type: "agent_chat_message",
                entity_id: &input.id,
                chat_id: &input.chat_id,
                status: Some(input.status.to_string().as_str()),
                dedupe_key: Some(format!("chat-message:{}", input.id)),
                created_at: &crate::now_rfc3339(),
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(message)
    }
}

#[async_trait]
impl AgentChatTurnJobRepo for SqliteDb {
    async fn get_agent_chat_turn_job(&self, id: &str) -> Result<Option<AgentChatTurnJob>> {
        sqlx::query(AGENT_CHAT_TURN_WITH_RETRY_STATE)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent_chat_turn_job)
            .transpose()
    }

    async fn list_agent_chat_turn_jobs(&self, chat_id: &str) -> Result<Vec<AgentChatTurnJob>> {
        sqlx::query(&AGENT_CHAT_TURN_WITH_RETRY_STATE.replace(
            "WHERE t.id = ?",
            "WHERE t.chat_id = ? ORDER BY t.created_at ASC, t.id ASC",
        ))
        .bind(chat_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_chat_turn_job)
        .collect()
    }

    async fn count_pending_agent_chat_turn_jobs(&self, chat_id: &str) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_chat_turn_job
             WHERE chat_id = ? AND status IN ('queued', 'leased', 'retry_wait')",
        )
        .bind(chat_id)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn create_agent_chat_turn_job(
        &self,
        input: CreateAgentChatTurnJob,
    ) -> Result<AgentChatTurnJob> {
        sqlx::query(
            "INSERT INTO agent_chat_turn_job (
                id, chat_id, triggering_message_id, responder_identity_id, profile_id,
                responder_binding_id, responder_binding_version, responder_identity_version,
                profile_version,
                operating_skill_revision_id, policy_revision, policy_digest,
                permission_policy_digest, tool_policy_digest, admission_digest,
                canonical_scope_provenance_json, canonical_scope_type, canonical_scope_id,
                status, dedupe_key,
                max_attempts, correlation_id, causation_id, causation_depth,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.chat_id)
        .bind(&input.triggering_message_id)
        .bind(&input.responder_identity_id)
        .bind(&input.profile_id)
        .bind(input.responder_binding_id.as_deref())
        .bind(input.responder_binding_version)
        .bind(input.responder_identity_version)
        .bind(input.profile_version)
        .bind(input.operating_skill_revision_id.as_deref())
        .bind(input.policy_revision.as_deref())
        .bind(input.policy_digest.as_deref())
        .bind(input.permission_policy_digest.as_deref())
        .bind(input.tool_policy_digest.as_deref())
        .bind(input.admission_digest.as_deref())
        .bind(input.canonical_scope_provenance_json.as_deref())
        .bind(&input.canonical_scope_type)
        .bind(&input.canonical_scope_id)
        .bind(&input.dedupe_key)
        .bind(input.max_attempts)
        .bind(&input.correlation_id)
        .bind(input.causation_id.as_deref())
        .bind(input.causation_depth)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_chat_write_error)?;
        self.get_agent_chat_turn_job(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn update_agent_chat_turn_job(
        &self,
        input: UpdateAgentChatTurnJob,
    ) -> Result<AgentChatTurnJob> {
        let current = self
            .get_agent_chat_turn_job(&input.id)
            .await?
            .ok_or(DbError::NotFound)?;
        if current.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        let pending_interaction_id = input
            .pending_interaction_id
            .unwrap_or(current.pending_interaction_id);
        let lease_owner = input.lease_owner.unwrap_or(current.lease_owner);
        let leased_until = input.leased_until.unwrap_or(current.leased_until);
        let attempt_count = input.attempt_count.unwrap_or(current.attempt_count);
        let next_attempt_at = input.next_attempt_at.unwrap_or(current.next_attempt_at);
        let response_message_id = input
            .response_message_id
            .unwrap_or(current.response_message_id);
        let error_code = input.error_code.unwrap_or(current.error_code);
        let error_message = input.error_message.unwrap_or(current.error_message);
        let (lease_owner, leased_until) = if matches!(
            input.status,
            AgentChatTurnState::Succeeded
                | AgentChatTurnState::Failed
                | AgentChatTurnState::Cancelled
                | AgentChatTurnState::AwaitingInput
        ) {
            (None, None)
        } else {
            (lease_owner, leased_until)
        };
        let updated = self.execute_agent_chat_turn_status_update(&input.id, sqlx::query(
            "UPDATE agent_chat_turn_job
             SET status = ?, pending_interaction_id = ?, lease_owner = ?, leased_until = ?, attempt_count = ?,
                 next_attempt_at = ?, response_message_id = ?, error_code = ?,
                 error_message = ?, failure_class_json = CASE WHEN ? THEN NULL ELSE failure_class_json END,
                 retry_decision = CASE WHEN ? THEN NULL ELSE retry_decision END, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
        )
        .bind(input.status.to_string())
        .bind(pending_interaction_id.as_deref())
        .bind(lease_owner.as_deref())
        .bind(leased_until.as_deref())
        .bind(attempt_count)
        .bind(next_attempt_at.as_deref())
        .bind(response_message_id.as_deref())
        .bind(error_code.as_deref())
        .bind(error_message.as_deref())
        .bind(input.status == AgentChatTurnState::Cancelled)
        .bind(input.status == AgentChatTurnState::Cancelled)
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        )
        .await?;
        if updated.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        self.get_agent_chat_turn_job(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }
}

#[async_trait]
impl AgentHandoffRepo for SqliteDb {
    async fn get_agent_handoff(&self, id: &str) -> Result<Option<AgentHandoff>> {
        sqlx::query("SELECT * FROM agent_handoff WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent_handoff)
            .transpose()
    }

    async fn list_agent_handoffs(&self, target_chat_id: &str) -> Result<Vec<AgentHandoff>> {
        sqlx::query(
            "SELECT * FROM agent_handoff
             WHERE target_chat_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(target_chat_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_handoff)
        .collect()
    }

    async fn create_agent_handoff(&self, input: CreateAgentHandoff) -> Result<AgentHandoff> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        sqlx::query(
            "INSERT INTO agent_handoff (
                id, source_chat_id, target_chat_id, source_message_id,
                source_turn_job_id, author_identity_id, content, content_guard_json,
                source_revisions_json, status, correlation_id, causation_id,
                dedupe_key, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.source_chat_id)
        .bind(&input.target_chat_id)
        .bind(input.source_message_id.as_deref())
        .bind(input.source_turn_job_id.as_deref())
        .bind(input.author_identity_id.as_deref())
        .bind(&input.content)
        .bind(&input.content_guard_json)
        .bind(&input.source_revisions_json)
        .bind(&input.correlation_id)
        .bind(input.causation_id.as_deref())
        .bind(&input.dedupe_key)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(map_chat_write_error)?;
        crate::sqlite::agent_chat_topic::enqueue_native_topic_rotation_in_tx(
            &mut transaction,
            Some(&input.source_chat_id),
            "After handoff",
            "handoff",
            &input.created_at,
        )
        .await?;
        let handoff = sqlx::query("SELECT * FROM agent_handoff WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_handoff)?;
        transaction.commit().await?;
        Ok(handoff)
    }
}
