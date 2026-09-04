use super::*;
use sha2::{Digest, Sha256};

#[async_trait]
impl AccountMainAgentBindingRepo for SqliteDb {
    async fn get_active_main_binding(
        &self,
        account_id: &str,
    ) -> Result<Option<AccountMainAgentBinding>> {
        sqlx::query(
            "SELECT * FROM account_main_agent_binding
             WHERE account_id = ? AND state = 'active'
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_account_main_binding)
        .transpose()
    }

    async fn get_main_binding(&self, id: &str) -> Result<Option<AccountMainAgentBinding>> {
        sqlx::query("SELECT * FROM account_main_agent_binding WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_account_main_binding)
            .transpose()
    }

    async fn list_main_binding_history(
        &self,
        account_id: &str,
    ) -> Result<Vec<AccountMainAgentBinding>> {
        sqlx::query(
            "SELECT * FROM account_main_agent_binding
             WHERE account_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(account_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_account_main_binding)
        .collect()
    }

    async fn create_main_binding(
        &self,
        input: CreateAccountMainAgentBinding,
    ) -> Result<AccountMainAgentBinding> {
        sqlx::query(
            "INSERT INTO account_main_agent_binding (
                id, account_id, identity_id, profile_id, state,
                autonomy_policy_json, tool_policy_revision, version,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.account_id)
        .bind(&input.identity_id)
        .bind(&input.profile_id)
        .bind(&input.autonomy_policy_json)
        .bind(&input.tool_policy_revision)
        .bind(1_i64)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_binding_write_error)?;

        self.get_main_binding(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn replace_main_binding(
        &self,
        input: ReplaceAccountMainAgentBinding,
    ) -> Result<AccountMainAgentBinding> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let current = sqlx::query(
            "UPDATE account_main_agent_binding
             SET state = 'replaced', replaced_by_binding_id = NULL,
                 replacement_reason = ?, version = version + 1, updated_at = ?
             WHERE account_id = ? AND state = 'active' AND version = ?
             RETURNING *",
        )
        .bind(input.replacement_reason.as_deref())
        .bind(&input.replacement.updated_at)
        .bind(&input.account_id)
        .bind(input.expected_version)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::VersionConflict)
        .and_then(map_account_main_binding)?;
        sqlx::query(
            "INSERT INTO account_main_agent_binding (
                id, account_id, identity_id, profile_id, state,
                autonomy_policy_json, tool_policy_revision, version,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?)",
        )
        .bind(&input.replacement.id)
        .bind(&input.replacement.account_id)
        .bind(&input.replacement.identity_id)
        .bind(&input.replacement.profile_id)
        .bind(&input.replacement.autonomy_policy_json)
        .bind(&input.replacement.tool_policy_revision)
        .bind(current.version)
        .bind(&input.replacement.created_at)
        .bind(&input.replacement.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(map_binding_write_error)?;
        sqlx::query(
            "UPDATE account_main_agent_binding
             SET replaced_by_binding_id = ?
             WHERE id = ? AND state = 'replaced'",
        )
        .bind(&input.replacement.id)
        .bind(&current.id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        self.get_main_binding(&input.replacement.id)
            .await?
            .ok_or(DbError::NotFound)
    }
}

#[async_trait]
impl ProjectAgentBindingRepo for SqliteDb {
    async fn get_active_project_binding(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectAgentBinding>> {
        sqlx::query(
            "SELECT * FROM project_agent_binding
             WHERE project_id = ? AND state IN ('active', 'agent_setup_required')
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await?
        .map(map_project_agent_binding)
        .transpose()
    }

    async fn get_project_binding(&self, id: &str) -> Result<Option<ProjectAgentBinding>> {
        sqlx::query("SELECT * FROM project_agent_binding WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_project_agent_binding)
            .transpose()
    }

    async fn list_project_binding_history(
        &self,
        project_id: &str,
    ) -> Result<Vec<ProjectAgentBinding>> {
        sqlx::query(
            "SELECT * FROM project_agent_binding
             WHERE project_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_project_agent_binding)
        .collect()
    }

    async fn create_project_binding(
        &self,
        input: CreateProjectAgentBinding,
    ) -> Result<ProjectAgentBinding> {
        sqlx::query(
            "INSERT INTO project_agent_binding (
                id, project_id, identity_id, profile_id, state,
                autonomy_policy_json, permission_ceiling_json, subscriptions_json,
                wake_budget, operating_skill_revision_id, policy_revision,
                policy_digest, charter_id, charter_revision_id,
                charter_setup_required, admission_receipt_id, charter_approval_id,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(input.identity_id.as_deref())
        .bind(input.profile_id.as_deref())
        .bind(&input.state)
        .bind(&input.autonomy_policy_json)
        .bind(&input.permission_ceiling_json)
        .bind(&input.subscriptions_json)
        .bind(input.wake_budget)
        .bind(input.operating_skill_revision_id.as_deref())
        .bind(&input.policy_revision)
        .bind(&input.policy_digest)
        .bind(input.charter_id.as_deref())
        .bind(input.charter_revision_id.as_deref())
        .bind(i64::from(input.charter_setup_required))
        .bind(input.admission_receipt_id.as_deref())
        .bind(input.charter_approval_id.as_deref())
        .bind(1_i64)
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_binding_write_error)?;

        self.get_project_binding(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn replace_project_binding(
        &self,
        input: ReplaceProjectAgentBinding,
    ) -> Result<ProjectAgentBinding> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let current = sqlx::query(
            "UPDATE project_agent_binding
             SET state = 'replaced', replaced_by_binding_id = NULL,
                 replacement_reason = ?, version = version + 1, updated_at = ?
             WHERE project_id = ? AND state IN ('active', 'agent_setup_required')
               AND version = ?
             RETURNING *",
        )
        .bind(input.replacement_reason.as_deref())
        .bind(&input.replacement.updated_at)
        .bind(&input.project_id)
        .bind(input.expected_version)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::VersionConflict)
        .and_then(map_project_agent_binding)?;

        sqlx::query(
            "INSERT INTO project_agent_binding (
                id, project_id, identity_id, profile_id, state,
                autonomy_policy_json, permission_ceiling_json, subscriptions_json,
                wake_budget, operating_skill_revision_id, policy_revision,
                policy_digest, charter_id, charter_revision_id,
                charter_setup_required, admission_receipt_id, charter_approval_id,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.replacement.id)
        .bind(&input.replacement.project_id)
        .bind(input.replacement.identity_id.as_deref())
        .bind(input.replacement.profile_id.as_deref())
        .bind(&input.replacement.state)
        .bind(&input.replacement.autonomy_policy_json)
        .bind(&input.replacement.permission_ceiling_json)
        .bind(&input.replacement.subscriptions_json)
        .bind(input.replacement.wake_budget)
        .bind(input.replacement.operating_skill_revision_id.as_deref())
        .bind(&input.replacement.policy_revision)
        .bind(&input.replacement.policy_digest)
        .bind(input.replacement.charter_id.as_deref())
        .bind(input.replacement.charter_revision_id.as_deref())
        .bind(i64::from(input.replacement.charter_setup_required))
        .bind(input.replacement.admission_receipt_id.as_deref())
        .bind(input.replacement.charter_approval_id.as_deref())
        .bind(current.version)
        .bind(&input.replacement.created_at)
        .bind(&input.replacement.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(map_binding_write_error)?;
        sqlx::query(
            "UPDATE project_agent_binding
             SET replaced_by_binding_id = ?
             WHERE id = ? AND state = 'replaced'",
        )
        .bind(&input.replacement.id)
        .bind(&current.id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        self.get_project_binding(&input.replacement.id)
            .await?
            .ok_or(DbError::NotFound)
    }
}

#[async_trait]
impl ProjectBindingCommandRepo for SqliteDb {
    async fn set_project_binding_command(
        &self,
        input: SetProjectAgentBindingCommand,
    ) -> Result<ProjectAgentBinding> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let current = sqlx::query(
            "SELECT * FROM project_agent_binding
             WHERE project_id = ? AND state IN ('active', 'agent_setup_required')
             ORDER BY created_at DESC, id DESC LIMIT 1",
        )
        .bind(&input.replacement.project_id)
        .fetch_optional(&mut *transaction)
        .await?
        .map(map_project_agent_binding)
        .transpose()?;

        let replacement_version = match (current.as_ref(), input.expected_version) {
            (Some(current), Some(expected)) if current.version == expected => {
                let updated = sqlx::query(
                    "UPDATE project_agent_binding
                     SET state = 'replaced', replaced_by_binding_id = NULL,
                         replacement_reason = ?, version = version + 1, updated_at = ?
                     WHERE id = ? AND project_id = ?
                       AND state IN ('active', 'agent_setup_required') AND version = ?",
                )
                .bind(input.replacement_reason.as_deref())
                .bind(&input.replacement.updated_at)
                .bind(&current.id)
                .bind(&input.replacement.project_id)
                .bind(expected)
                .execute(&mut *transaction)
                .await?;
                if updated.rows_affected() != 1 {
                    return Err(DbError::VersionConflict);
                }
                expected + 1
            }
            (None, None) => 1,
            _ => return Err(DbError::VersionConflict),
        };

        sqlx::query(
            "INSERT INTO project_agent_binding (
                id, project_id, identity_id, profile_id, state,
                autonomy_policy_json, permission_ceiling_json, subscriptions_json,
                wake_budget, operating_skill_revision_id, policy_revision,
                policy_digest, charter_id, charter_revision_id,
                charter_setup_required, admission_receipt_id, charter_approval_id,
                version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.replacement.id)
        .bind(&input.replacement.project_id)
        .bind(input.replacement.identity_id.as_deref())
        .bind(input.replacement.profile_id.as_deref())
        .bind(&input.replacement.state)
        .bind(&input.replacement.autonomy_policy_json)
        .bind(&input.replacement.permission_ceiling_json)
        .bind(&input.replacement.subscriptions_json)
        .bind(input.replacement.wake_budget)
        .bind(input.replacement.operating_skill_revision_id.as_deref())
        .bind(&input.replacement.policy_revision)
        .bind(&input.replacement.policy_digest)
        .bind(input.replacement.charter_id.as_deref())
        .bind(input.replacement.charter_revision_id.as_deref())
        .bind(i64::from(input.replacement.charter_setup_required))
        .bind(input.replacement.admission_receipt_id.as_deref())
        .bind(input.replacement.charter_approval_id.as_deref())
        .bind(replacement_version)
        .bind(&input.replacement.created_at)
        .bind(&input.replacement.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(map_binding_write_error)?;

        if let Some(current) = current.as_ref() {
            sqlx::query(
                "UPDATE project_agent_binding SET replaced_by_binding_id = ?
                 WHERE id = ? AND project_id = ? AND state = 'replaced'",
            )
            .bind(&input.replacement.id)
            .bind(&current.id)
            .bind(&input.replacement.project_id)
            .execute(&mut *transaction)
            .await?;
        }

        let chat_status = if input.replacement.state == "active" {
            "ready"
        } else {
            "agent_setup_required"
        };
        sqlx::query(
            "UPDATE agent_chat SET status = ?, version = version + 1, updated_at = ?
             WHERE kind = 'project' AND project_id = ? AND status != ?",
        )
        .bind(chat_status)
        .bind(&input.replacement.updated_at)
        .bind(&input.replacement.project_id)
        .bind(chat_status)
        .execute(&mut *transaction)
        .await?;

        DomainEventRepo::append_event_in_tx(
            self,
            &mut transaction,
            &CreateDomainEvent {
                id: input.event_id.clone(),
                event_type: "project.agent_binding.set".to_owned(),
                entity_type: "project_agent_binding".to_owned(),
                entity_id: input.replacement.id.clone(),
                actor_type: "user".to_owned(),
                actor_id: Some(input.actor_user_id),
                scope_type: "project".to_owned(),
                scope_id: input.replacement.project_id.clone(),
                correlation_id: input.correlation_id,
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!(
                    "project-agent-binding-set:{}",
                    input.replacement.id
                )),
                payload_json: serde_json::json!({
                    "project_id": input.replacement.project_id,
                    "binding_id": input.replacement.id,
                    "replaced_binding_id": current.as_ref().map(|value| value.id.as_str()),
                    "state": input.replacement.state,
                    "identity_id": input.replacement.identity_id,
                    "profile_id": input.replacement.profile_id,
                    "admission_receipt_id": input.replacement.admission_receipt_id,
                    "charter_approval_id": input.replacement.charter_approval_id,
                    "charter_id": input.replacement.charter_id,
                    "charter_revision_id": input.replacement.charter_revision_id,
                    "operating_skill_revision_id": input.replacement.operating_skill_revision_id,
                })
                .to_string(),
                created_at: input.replacement.updated_at.clone(),
            },
        )
        .await?;

        let binding = sqlx::query("SELECT * FROM project_agent_binding WHERE id = ?")
            .bind(&input.replacement.id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_project_agent_binding)?;
        transaction.commit().await?;
        Ok(binding)
    }
}

#[async_trait]
impl ProjectAdmissionReceiptRepo for SqliteDb {
    async fn get_project_admission_receipt(
        &self,
        id: &str,
    ) -> Result<Option<ProjectAdmissionReceipt>> {
        sqlx::query("SELECT * FROM project_admission_receipt WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_project_admission_receipt)
            .transpose()
    }

    async fn get_project_admission_receipt_for_project(
        &self,
        project_id: &str,
    ) -> Result<Option<ProjectAdmissionReceipt>> {
        sqlx::query("SELECT * FROM project_admission_receipt WHERE project_id = ?")
            .bind(project_id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_project_admission_receipt)
            .transpose()
    }

    async fn create_project_admission_receipt(
        &self,
        input: CreateProjectAdmissionReceipt,
    ) -> Result<ProjectAdmissionReceipt> {
        sqlx::query(
            "INSERT INTO project_admission_receipt (
                id, project_id, source_kind, handoff_id,
                initial_charter_approval_id, initial_charter_id,
                initial_charter_revision_id, payload_digest,
                validation_schema_version, validated_at, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.project_id)
        .bind(&input.source_kind)
        .bind(input.handoff_id.as_deref())
        .bind(&input.initial_charter_approval_id)
        .bind(&input.initial_charter_id)
        .bind(&input.initial_charter_revision_id)
        .bind(&input.payload_digest)
        .bind(&input.validation_schema_version)
        .bind(&input.validated_at)
        .bind(&input.created_at)
        .execute(&self.pool)
        .await
        .map_err(map_binding_write_error)?;

        self.get_project_admission_receipt(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn resolve_current_project_binding_authority(
        &self,
        project_id: &str,
    ) -> Result<Option<CurrentProjectBindingAuthority>> {
        sqlx::query(
            "SELECT p.id AS project_id,
                    receipt.id AS admission_receipt_id,
                    approval.id AS charter_approval_id,
                    p.current_charter_id AS charter_id,
                    p.current_charter_revision_id AS charter_revision_id,
                    skill.current_revision_id AS operating_skill_revision_id
             FROM project p
             JOIN project_admission_receipt receipt ON receipt.project_id = p.id
             JOIN project_charter_approval approval
               ON approval.consumed_project_id = p.id
              AND approval.charter_id = p.current_charter_id
              AND approval.revision_id = p.current_charter_revision_id
              AND approval.lifecycle = 'consumed'
             JOIN operating_skill skill
               ON skill.skill_key = 'forge.project.orchestration/v1'
              AND skill.lifecycle = 'active'
              AND skill.current_revision_id IS NOT NULL
             JOIN operating_skill_revision revision
               ON revision.id = skill.current_revision_id
              AND revision.operating_skill_id = skill.id
             WHERE p.id = ?
               AND p.charter_status = 'charter_backed'
               AND p.charter_setup_required = 0
             ORDER BY approval.consumed_at DESC, approval.updated_at DESC,
                      approval.id DESC
             LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await?
        .map(|row| {
            Ok(CurrentProjectBindingAuthority {
                project_id: row.try_get("project_id")?,
                admission_receipt_id: row.try_get("admission_receipt_id")?,
                charter_approval_id: row.try_get("charter_approval_id")?,
                charter_id: row.try_get("charter_id")?,
                charter_revision_id: row.try_get("charter_revision_id")?,
                operating_skill_revision_id: row.try_get("operating_skill_revision_id")?,
            })
        })
        .transpose()
    }

    async fn get_current_project_operating_skill_revision(&self) -> Result<Option<String>> {
        sqlx::query_scalar(
            "SELECT revision.id
             FROM operating_skill skill
             JOIN operating_skill_revision revision
               ON revision.id = skill.current_revision_id
              AND revision.operating_skill_id = skill.id
             WHERE skill.skill_key = 'forge.project.orchestration/v1'
               AND skill.lifecycle = 'active'
             LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::from)
    }
}

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
        transaction.commit().await?;
        Ok(message)
    }
}

#[async_trait]
impl AgentChatTurnJobRepo for SqliteDb {
    async fn get_agent_chat_turn_job(&self, id: &str) -> Result<Option<AgentChatTurnJob>> {
        sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_agent_chat_turn_job)
            .transpose()
    }

    async fn list_agent_chat_turn_jobs(&self, chat_id: &str) -> Result<Vec<AgentChatTurnJob>> {
        sqlx::query(
            "SELECT * FROM agent_chat_turn_job
             WHERE chat_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(chat_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(map_agent_chat_turn_job)
        .collect()
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
        let updated = sqlx::query(
            "UPDATE agent_chat_turn_job
             SET status = ?, pending_interaction_id = ?, lease_owner = ?, leased_until = ?, attempt_count = ?,
                 next_attempt_at = ?, response_message_id = ?, error_code = ?,
                 error_message = ?, version = version + 1, updated_at = ?
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
        .bind(&input.updated_at)
        .bind(&input.id)
        .bind(input.expected_version)
        .execute(&self.pool)
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
        .execute(&self.pool)
        .await
        .map_err(map_chat_write_error)?;
        self.get_agent_handoff(&input.id)
            .await?
            .ok_or(DbError::NotFound)
    }
}

/// Admit a chat message and queued turn using a caller-owned transaction.
/// Wake disposition persistence uses this same primitive so a turn admission,
/// its message-admitted event, the disposition, and the source-event receipt
/// share one commit boundary.
pub(super) async fn admit_agent_chat_turn_in_tx(
    db: &SqliteDb,
    transaction: &mut Transaction<'_, Sqlite>,
    input: AdmitAgentChatTurn,
) -> Result<AdmittedAgentChatTurn> {
    if input.message.chat_id != input.turn.chat_id
        || input.message.id != input.turn.triggering_message_id
    {
        return Err(DbError::Check(
            "chat turn message and job scope must match".to_owned(),
        ));
    }
    if let Some(existing) = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE dedupe_key = ?")
        .bind(&input.turn.dedupe_key)
        .fetch_optional(&mut **transaction)
        .await?
    {
        let turn = map_agent_chat_turn_job(existing)?;
        if !turn_admission_semantics_match(&input.turn, &turn) {
            return Err(DbError::IdempotencyConflict);
        }
        let message = sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
            .bind(&turn.triggering_message_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(DbError::NotFound)
            .and_then(map_agent_chat_message)?;
        return Ok(AdmittedAgentChatTurn { message, turn });
    }

    // The resolver runs before this boundary, but writers can replace the
    // binding or selected Profile between those reads and the insert.  Hold
    // the IMMEDIATE transaction lock while checking every frozen identity
    // version so a new job is never admitted from a mixed snapshot.
    validate_agent_chat_turn_admission(transaction, &input.turn).await?;

    let sequence = allocate_chat_sequence(
        transaction,
        &input.message.chat_id,
        &input.message.created_at,
    )
    .await?;
    let mut message_input = input.message.clone();
    message_input.sequence = sequence;
    let message = insert_chat_message(transaction, &message_input).await?;

    // A direct user message intentionally supersedes a parked interaction;
    // autonomous/wake/handoff messages must leave a pending user question
    // intact and cannot silently cancel it.
    if matches!(&input.message.author_type, AgentChatMessageAuthorType::User) {
        let parked_rows = sqlx::query(
            "SELECT id, pending_interaction_id, version FROM agent_chat_turn_job
             WHERE chat_id = ? AND status = 'awaiting_input'",
        )
        .bind(&input.turn.chat_id)
        .fetch_all(&mut **transaction)
        .await?;

        for row in parked_rows {
            let pid: String = row.try_get("id")?;
            let pending_interaction_id: Option<String> = row.try_get("pending_interaction_id")?;
            let pver: i64 = row.try_get("version")?;
            if let Some(iid) = pending_interaction_id {
                let _ = sqlx::query(
                    "UPDATE protected_interaction SET status = 'cancelled', version = version + 1, updated_at = ?
                     WHERE id = ? AND status = 'pending'",
                )
                .bind(&input.turn.created_at)
                .bind(&iid)
                .execute(&mut **transaction)
                .await;
            }
            let _ = sqlx::query(
                "UPDATE agent_chat_turn_job
                 SET status = 'cancelled', pending_interaction_id = NULL,
                     error_code = 'superseded_by_user_message',
                     error_message = 'superseded by newer user message',
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ? AND status = 'awaiting_input'",
            )
            .bind(&input.turn.created_at)
            .bind(&pid)
            .bind(pver)
            .execute(&mut **transaction)
            .await;
        }
    }

    sqlx::query(
        "INSERT INTO agent_chat_turn_job (
            id, chat_id, triggering_message_id, responder_identity_id, profile_id,
            responder_binding_id, responder_binding_version, responder_identity_version,
            profile_version,
            operating_skill_revision_id, policy_revision, policy_digest,
            permission_policy_digest, tool_policy_digest, admission_digest,
            canonical_scope_provenance_json,
            canonical_scope_type, canonical_scope_id, status, dedupe_key,
            max_attempts, correlation_id, causation_id, causation_depth,
            created_at, updated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.turn.id)
    .bind(&input.turn.chat_id)
    .bind(&input.turn.triggering_message_id)
    .bind(&input.turn.responder_identity_id)
    .bind(&input.turn.profile_id)
    .bind(input.turn.responder_binding_id.as_deref())
    .bind(input.turn.responder_binding_version)
    .bind(input.turn.responder_identity_version)
    .bind(input.turn.profile_version)
    .bind(input.turn.operating_skill_revision_id.as_deref())
    .bind(input.turn.policy_revision.as_deref())
    .bind(input.turn.policy_digest.as_deref())
    .bind(input.turn.permission_policy_digest.as_deref())
    .bind(input.turn.tool_policy_digest.as_deref())
    .bind(input.turn.admission_digest.as_deref())
    .bind(input.turn.canonical_scope_provenance_json.as_deref())
    .bind(&input.turn.canonical_scope_type)
    .bind(&input.turn.canonical_scope_id)
    .bind(&input.turn.dedupe_key)
    .bind(input.turn.max_attempts)
    .bind(&input.turn.correlation_id)
    .bind(input.turn.causation_id.as_deref())
    .bind(input.turn.causation_depth)
    .bind(&input.turn.created_at)
    .bind(&input.turn.updated_at)
    .execute(&mut **transaction)
    .await
    .map_err(map_chat_write_error)?;
    let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
        .bind(&input.turn.id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(DbError::from)
        .and_then(map_agent_chat_turn_job)?;
    append_agent_chat_event(
        db,
        transaction,
        "agent_chat.message.admitted",
        &message,
        input.turn.correlation_id.clone(),
        input.turn.causation_id.clone(),
        input.turn.causation_depth,
    )
    .await?;
    Ok(AdmittedAgentChatTurn { message, turn })
}

#[async_trait]
impl AgentChatTransactionRepo for SqliteDb {
    async fn admit_agent_chat_turn(
        &self,
        input: AdmitAgentChatTurn,
    ) -> Result<AdmittedAgentChatTurn> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let admitted = admit_agent_chat_turn_in_tx(self, &mut transaction, input).await?;
        transaction.commit().await?;
        Ok(admitted)
    }

    async fn admit_agent_chat_turn_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: AdmitAgentChatTurn,
    ) -> Result<AdmittedAgentChatTurn> {
        admit_agent_chat_turn_in_tx(self, transaction, input).await
    }

    async fn admit_agent_chat_continuation_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateAgentChatTurnJob,
    ) -> Result<AgentChatTurnJob> {
        if let Some(existing) =
            sqlx::query("SELECT * FROM agent_chat_turn_job WHERE dedupe_key = ?")
                .bind(&input.dedupe_key)
                .fetch_optional(&mut **transaction)
                .await?
        {
            let turn = map_agent_chat_turn_job(existing)?;
            if !turn_admission_semantics_match(&input, &turn) {
                return Err(DbError::IdempotencyConflict);
            }
            return Ok(turn);
        }

        let trigger_chat_id =
            sqlx::query_scalar::<_, String>("SELECT chat_id FROM agent_chat_message WHERE id = ?")
                .bind(&input.triggering_message_id)
                .fetch_optional(&mut **transaction)
                .await?
                .ok_or(DbError::NotFound)?;
        if trigger_chat_id != input.chat_id {
            return Err(DbError::Check(
                "chat continuation trigger does not belong to the canonical chat".to_owned(),
            ));
        }

        validate_agent_chat_turn_admission(transaction, &input).await?;
        sqlx::query(
            "INSERT INTO agent_chat_turn_job (
                id, chat_id, triggering_message_id, responder_identity_id, profile_id,
                responder_binding_id, responder_binding_version, responder_identity_version,
                profile_version, operating_skill_revision_id, policy_revision, policy_digest,
                permission_policy_digest, tool_policy_digest, admission_digest,
                canonical_scope_provenance_json, canonical_scope_type, canonical_scope_id,
                status, dedupe_key, max_attempts, correlation_id, causation_id,
                causation_depth, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       'queued', ?, ?, ?, ?, ?, ?, ?)",
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
        .execute(&mut **transaction)
        .await
        .map_err(map_chat_write_error)?;

        sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_turn_job)
    }

    async fn complete_agent_chat_turn(
        &self,
        input: CompleteAgentChatTurn,
    ) -> Result<CompletedAgentChatTurn> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let current_row = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.turn_job_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let current = map_agent_chat_turn_job(current_row)?;
        if current.status == AgentChatTurnState::Succeeded {
            let response_id = current
                .response_message_id
                .clone()
                .ok_or(DbError::NotFound)?;
            let response = sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
                .bind(response_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(DbError::from)
                .and_then(map_agent_chat_message)?;
            transaction.commit().await?;
            return Ok(CompletedAgentChatTurn {
                response,
                turn: current,
            });
        }
        if current.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        if current.status != AgentChatTurnState::Leased
            || current.lease_owner.as_deref() != Some(input.lease_owner.as_str())
        {
            return Err(DbError::VersionConflict);
        }
        if matches!(
            current.status,
            AgentChatTurnState::Failed | AgentChatTurnState::Cancelled
        ) {
            return Err(DbError::InvalidTransition);
        }
        if input.response.chat_id != current.chat_id
            || input.response.id == current.triggering_message_id
        {
            return Err(DbError::Check(
                "response message must belong to turn chat and differ from trigger".to_owned(),
            ));
        }
        if let Some(existing) = sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
            .bind(&input.response.id)
            .fetch_optional(&mut *transaction)
            .await?
        {
            let response = map_agent_chat_message(existing)?;
            if response.chat_id != current.chat_id {
                return Err(DbError::Check("response message chat mismatch".to_owned()));
            }
            let updated = sqlx::query(
                "UPDATE agent_chat_turn_job
                 SET status = 'succeeded', response_message_id = ?,
                     lease_owner = NULL, leased_until = NULL,
                     next_attempt_at = NULL, error_code = NULL,
                     error_message = NULL,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ?
                   AND status = 'leased' AND lease_owner = ?",
            )
            .bind(&response.id)
            .bind(&input.updated_at)
            .bind(&input.turn_job_id)
            .bind(input.expected_version)
            .bind(&input.lease_owner)
            .execute(&mut *transaction)
            .await?;
            if updated.rows_affected() == 0 {
                return Err(DbError::VersionConflict);
            }
            let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
                .bind(&input.turn_job_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(DbError::from)
                .and_then(map_agent_chat_turn_job)?;
            transaction.commit().await?;
            return Ok(CompletedAgentChatTurn { response, turn });
        }

        let sequence = allocate_chat_sequence(
            &mut transaction,
            &current.chat_id,
            &input.response.created_at,
        )
        .await?;
        let mut response_input = input.response.clone();
        response_input.sequence = sequence;
        let response = insert_chat_message(&mut transaction, &response_input).await?;
        let updated = sqlx::query(
            "UPDATE agent_chat_turn_job
             SET status = 'succeeded', response_message_id = ?,
                 lease_owner = NULL, leased_until = NULL,
                 next_attempt_at = NULL, error_code = NULL,
                 error_message = NULL,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?
               AND status = 'leased' AND lease_owner = ?",
        )
        .bind(&response.id)
        .bind(&input.updated_at)
        .bind(&input.turn_job_id)
        .bind(input.expected_version)
        .bind(&input.lease_owner)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        append_agent_chat_event(
            self,
            &mut transaction,
            "agent_chat.response.completed",
            &response,
            current.correlation_id.clone(),
            current.causation_id.clone(),
            current.causation_depth,
        )
        .await?;
        let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.turn_job_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_turn_job)?;
        transaction.commit().await?;
        Ok(CompletedAgentChatTurn { response, turn })
    }

    async fn complete_agent_chat_control_transfer(
        &self,
        input: CompleteAgentChatControlTransfer,
    ) -> Result<AgentChatTurnJob> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let current_row = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.turn_job_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let current = map_agent_chat_turn_job(current_row)?;
        if current.status == AgentChatTurnState::Succeeded && current.response_message_id.is_none()
        {
            transaction.commit().await?;
            return Ok(current);
        }
        if current.version != input.expected_version
            || current.status != AgentChatTurnState::Leased
            || current.lease_owner.as_deref() != Some(input.lease_owner.as_str())
        {
            return Err(DbError::VersionConflict);
        }

        let outcome_json = sqlx::query_scalar::<_, String>(
            "SELECT outcome_json FROM command_receipt
             WHERE id = ? AND operation = 'genesis.start'",
        )
        .bind(&input.command_receipt_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let outcome: serde_json::Value = serde_json::from_str(&outcome_json)
            .map_err(|_| DbError::Check("Genesis start receipt outcome is invalid".to_owned()))?;
        if outcome
            .get("source_turn_id")
            .and_then(serde_json::Value::as_str)
            != Some(current.id.as_str())
            || outcome
                .get("admitted_turn_id")
                .and_then(serde_json::Value::as_str)
                != Some(input.continuation_turn_id.as_str())
            || outcome
                .get("session_id")
                .and_then(serde_json::Value::as_str)
                != Some(input.genesis_session_id.as_str())
            || outcome
                .get("control_transfer")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
        {
            return Err(DbError::Check(
                "Genesis start receipt does not authorize this turn control transfer".to_owned(),
            ));
        }
        let continuation_valid: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM agent_chat_turn_job
             WHERE id = ? AND chat_id = ? AND triggering_message_id = ?
               AND status IN ('queued', 'retry_wait', 'leased', 'awaiting_input', 'succeeded')",
        )
        .bind(&input.continuation_turn_id)
        .bind(&current.chat_id)
        .bind(&current.triggering_message_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if continuation_valid.is_none() {
            return Err(DbError::Check(
                "Genesis control transfer continuation is unavailable".to_owned(),
            ));
        }

        let updated = sqlx::query(
            "UPDATE agent_chat_turn_job
             SET status = 'succeeded', response_message_id = NULL,
                 lease_owner = NULL, leased_until = NULL, next_attempt_at = NULL,
                 error_code = NULL, error_message = NULL,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND status = 'leased' AND lease_owner = ?",
        )
        .bind(&input.updated_at)
        .bind(&input.turn_job_id)
        .bind(input.expected_version)
        .bind(&input.lease_owner)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }

        DomainEventRepo::append_event_in_tx(
            self,
            &mut transaction,
            &CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "agent_chat.turn.control_transferred".to_owned(),
                entity_type: "agent_chat_turn_job".to_owned(),
                entity_id: current.id.clone(),
                actor_type: "system".to_owned(),
                actor_id: None,
                scope_type: "agent_chat".to_owned(),
                scope_id: current.chat_id.clone(),
                correlation_id: current.correlation_id.clone(),
                causation_id: Some(input.command_receipt_id.clone()),
                causation_depth: current.causation_depth.saturating_add(1).min(16),
                dedupe_key: Some(format!("agent-chat-control-transfer:{}", current.id)),
                payload_json: serde_json::json!({
                    "operation": "genesis.start",
                    "source_turn_id": current.id,
                    "continuation_turn_id": input.continuation_turn_id,
                    "genesis_session_id": input.genesis_session_id,
                    "command_receipt_id": input.command_receipt_id,
                    "response_message_committed": false,
                })
                .to_string(),
                created_at: input.updated_at.clone(),
            },
        )
        .await?;
        let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.turn_job_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_turn_job)?;
        transaction.commit().await?;
        Ok(turn)
    }

    async fn fail_agent_chat_turn(&self, input: FailAgentChatTurn) -> Result<AgentChatTurnJob> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let error_code = bounded_event_text(&input.error_code, 128);
        let error_message = bounded_event_text(&input.error_message, 2048);
        let updated = sqlx::query(
            "UPDATE agent_chat_turn_job
             SET status = ?, lease_owner = NULL, leased_until = NULL,
                 attempt_count = ?, next_attempt_at = ?, error_code = ?,
                 error_message = ?, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND status = 'leased' AND lease_owner = ?",
        )
        .bind(input.status.to_string())
        .bind(input.attempt_count)
        .bind(input.next_attempt_at.as_deref())
        .bind(&error_code)
        .bind(&error_message)
        .bind(&input.updated_at)
        .bind(&input.turn_job_id)
        .bind(input.expected_version)
        .bind(&input.lease_owner)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.turn_job_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_turn_job)?;
        append_agent_chat_turn_failure_event(self, &mut transaction, &turn, &input).await?;
        transaction.commit().await?;
        Ok(turn)
    }

    async fn park_agent_chat_turn(&self, input: ParkAgentChatTurn) -> Result<AgentChatTurnJob> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let updated = sqlx::query(
            "UPDATE agent_chat_turn_job
             SET status = 'awaiting_input', pending_interaction_id = ?,
                 lease_owner = NULL, leased_until = NULL,
                 attempt_count = MAX(0, attempt_count - 1),
                 next_attempt_at = NULL, error_code = NULL,
                 error_message = NULL, version = version + 1, updated_at = ?
             WHERE id = ? AND version = ? AND status = 'leased' AND lease_owner = ?",
        )
        .bind(&input.pending_interaction_id)
        .bind(&input.updated_at)
        .bind(&input.turn_job_id)
        .bind(input.expected_version)
        .bind(&input.lease_owner)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.turn_job_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_turn_job)?;
        append_agent_chat_turn_awaiting_event(self, &mut transaction, &turn).await?;
        transaction.commit().await?;
        Ok(turn)
    }

    async fn cancel_agent_chat_turn(&self, input: CancelAgentChatTurn) -> Result<AgentChatTurnJob> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let dedupe_key = format!(
            "agent-chat-turn-cancel:{}:{}",
            input.turn_job_id, input.idempotency_key
        );

        // Cancellation idempotency is durable in the same ledger that records
        // the state transition.  A replay returns the terminal job without
        // rechecking the caller's old optimistic version or appending another
        // event.
        if let Some(existing_entity_id) = sqlx::query_scalar::<_, String>(
            "SELECT entity_id FROM domain_event WHERE dedupe_key = ?",
        )
        .bind(&dedupe_key)
        .fetch_optional(&mut *transaction)
        .await?
        {
            if existing_entity_id != input.turn_job_id {
                return Err(DbError::Check(
                    "turn cancellation idempotency key belongs to another turn".to_owned(),
                ));
            }
            let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
                .bind(&input.turn_job_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(DbError::from)
                .and_then(map_agent_chat_turn_job)?;
            transaction.commit().await?;
            return Ok(turn);
        }

        let current = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.turn_job_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)
            .and_then(map_agent_chat_turn_job)?;
        if current.version != input.expected_version {
            return Err(DbError::VersionConflict);
        }
        if !matches!(
            current.status,
            AgentChatTurnState::Queued
                | AgentChatTurnState::Leased
                | AgentChatTurnState::RetryWait
                | AgentChatTurnState::AwaitingInput
        ) {
            return Err(DbError::VersionConflict);
        }

        let updated = sqlx::query(
            "UPDATE agent_chat_turn_job
             SET status = 'cancelled', lease_owner = NULL, leased_until = NULL,
                 next_attempt_at = NULL, error_code = 'cancelled_by_user',
                 error_message = 'cancelled by user', version = version + 1,
                 updated_at = ?
             WHERE id = ? AND version = ?
               AND status IN ('queued', 'leased', 'retry_wait', 'awaiting_input')",
        )
        .bind(&input.updated_at)
        .bind(&input.turn_job_id)
        .bind(input.expected_version)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }

        let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.turn_job_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_turn_job)?;
        let event = CreateDomainEvent {
            id: new_uuid_v4(),
            event_type: "agent_chat.turn.cancelled".to_owned(),
            entity_type: "agent_chat_turn_job".to_owned(),
            entity_id: turn.id.clone(),
            actor_type: "user".to_owned(),
            actor_id: Some(input.actor_user_id),
            scope_type: "agent_chat".to_owned(),
            scope_id: turn.chat_id.clone(),
            correlation_id: turn.correlation_id.clone(),
            causation_id: turn.causation_id.clone(),
            causation_depth: turn.causation_depth,
            dedupe_key: Some(dedupe_key),
            payload_json: serde_json::json!({
                "turn_job_id": turn.id,
                "chat_id": turn.chat_id,
                "status": turn.status.to_string(),
                "version": turn.version,
            })
            .to_string(),
            created_at: input.updated_at,
        };
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;
        transaction.commit().await?;
        Ok(turn)
    }

    async fn admit_agent_handoff(&self, input: AdmitAgentHandoff) -> Result<AdmittedAgentHandoff> {
        if input.handoff.source_chat_id == input.handoff.target_chat_id
            || input.target_message.chat_id != input.handoff.target_chat_id
            || input.target_turn.chat_id != input.handoff.target_chat_id
            || input.target_turn.triggering_message_id != input.target_message.id
        {
            return Err(DbError::Check(
                "handoff source/target and turn scope must match".to_owned(),
            ));
        }
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        if let Some(existing_row) = sqlx::query("SELECT * FROM agent_handoff WHERE dedupe_key = ?")
            .bind(&input.handoff.dedupe_key)
            .fetch_optional(&mut *transaction)
            .await?
        {
            let handoff = map_agent_handoff(existing_row)?;
            let target_message_id = handoff.target_message_id.clone().ok_or(DbError::NotFound)?;
            let target_turn_id = handoff
                .target_turn_job_id
                .clone()
                .ok_or(DbError::NotFound)?;
            let message = sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
                .bind(target_message_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(DbError::from)
                .and_then(map_agent_chat_message)?;
            let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
                .bind(target_turn_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(DbError::from)
                .and_then(map_agent_chat_turn_job)?;
            if !turn_admission_semantics_match(&input.target_turn, &turn) {
                return Err(DbError::IdempotencyConflict);
            }
            transaction.commit().await?;
            return Ok(AdmittedAgentHandoff {
                handoff,
                message,
                turn,
            });
        }

        if let Some(source_provenance) = input.source_responder_provenance_json.as_deref() {
            validate_handoff_source_responder(
                &mut transaction,
                &input.handoff.source_chat_id,
                source_provenance,
                input.handoff.author_identity_id.as_deref(),
            )
            .await?;
        }
        validate_agent_chat_turn_admission(&mut transaction, &input.target_turn).await?;

        let sequence = allocate_chat_sequence(
            &mut transaction,
            &input.handoff.target_chat_id,
            &input.target_message.created_at,
        )
        .await?;
        let mut target_message_input = input.target_message.clone();
        target_message_input.sequence = sequence;
        target_message_input.handoff_id = Some(input.handoff.id.clone());
        let handoff = sqlx::query(
            "INSERT INTO agent_handoff (
                id, source_chat_id, target_chat_id, source_message_id,
                source_turn_job_id, target_message_id, target_turn_job_id,
                author_identity_id, content, content_guard_json, source_revisions_json,
                status, correlation_id, causation_id, dedupe_key, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'delivered', ?, ?, ?, ?, ?)",
        )
        .bind(&input.handoff.id)
        .bind(&input.handoff.source_chat_id)
        .bind(&input.handoff.target_chat_id)
        .bind(input.handoff.source_message_id.as_deref())
        .bind(input.handoff.source_turn_job_id.as_deref())
        .bind(&target_message_input.id)
        .bind(&input.target_turn.id)
        .bind(input.handoff.author_identity_id.as_deref())
        .bind(&input.handoff.content)
        .bind(&input.handoff.content_guard_json)
        .bind(&input.handoff.source_revisions_json)
        .bind(&input.handoff.correlation_id)
        .bind(input.handoff.causation_id.as_deref())
        .bind(&input.handoff.dedupe_key)
        .bind(&input.handoff.created_at)
        .bind(&input.handoff.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(map_chat_write_error)?;
        if handoff.rows_affected() == 0 {
            return Err(DbError::NotFound);
        }
        let message = insert_chat_message(&mut transaction, &target_message_input).await?;
        sqlx::query(
            "INSERT INTO agent_chat_turn_job (
                id, chat_id, triggering_message_id, responder_identity_id, profile_id,
                responder_binding_id, responder_binding_version, responder_identity_version,
                profile_version,
                operating_skill_revision_id, policy_revision, policy_digest,
                permission_policy_digest, tool_policy_digest, admission_digest,
                canonical_scope_provenance_json,
                canonical_scope_type, canonical_scope_id, status, dedupe_key,
                max_attempts, correlation_id, causation_id, causation_depth,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.target_turn.id)
        .bind(&input.target_turn.chat_id)
        .bind(&input.target_turn.triggering_message_id)
        .bind(&input.target_turn.responder_identity_id)
        .bind(&input.target_turn.profile_id)
        .bind(input.target_turn.responder_binding_id.as_deref())
        .bind(input.target_turn.responder_binding_version)
        .bind(input.target_turn.responder_identity_version)
        .bind(input.target_turn.profile_version)
        .bind(input.target_turn.operating_skill_revision_id.as_deref())
        .bind(input.target_turn.policy_revision.as_deref())
        .bind(input.target_turn.policy_digest.as_deref())
        .bind(input.target_turn.permission_policy_digest.as_deref())
        .bind(input.target_turn.tool_policy_digest.as_deref())
        .bind(input.target_turn.admission_digest.as_deref())
        .bind(input.target_turn.canonical_scope_provenance_json.as_deref())
        .bind(&input.target_turn.canonical_scope_type)
        .bind(&input.target_turn.canonical_scope_id)
        .bind(&input.target_turn.dedupe_key)
        .bind(input.target_turn.max_attempts)
        .bind(&input.target_turn.correlation_id)
        .bind(input.target_turn.causation_id.as_deref())
        .bind(input.target_turn.causation_depth)
        .bind(&input.target_turn.created_at)
        .bind(&input.target_turn.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(map_chat_write_error)?;
        sqlx::query(
            "INSERT INTO agent_handoff_delivery (
                handoff_id, delivery_sequence, status, target_message_id,
                target_turn_job_id, created_at
             ) VALUES (?, 1, 'delivered', ?, ?, ?)",
        )
        .bind(&input.handoff.id)
        .bind(&message.id)
        .bind(&input.target_turn.id)
        .bind(&input.handoff.updated_at)
        .execute(&mut *transaction)
        .await?;
        append_agent_chat_event(
            self,
            &mut transaction,
            "agent_chat.message.admitted",
            &message,
            input.handoff.correlation_id.clone(),
            input.handoff.causation_id.clone(),
            input.target_turn.causation_depth,
        )
        .await?;
        let handoff = sqlx::query("SELECT * FROM agent_handoff WHERE id = ?")
            .bind(&input.handoff.id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_handoff)?;
        let turn = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
            .bind(&input.target_turn.id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(DbError::from)
            .and_then(map_agent_chat_turn_job)?;
        transaction.commit().await?;
        Ok(AdmittedAgentHandoff {
            handoff,
            message,
            turn,
        })
    }
}

async fn append_agent_chat_event(
    db: &SqliteDb,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    event_type: &str,
    message: &AgentChatMessage,
    correlation_id: String,
    causation_id: Option<String>,
    causation_depth: i64,
) -> Result<DomainEvent> {
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: event_type.to_owned(),
        entity_type: "agent_chat_message".to_owned(),
        entity_id: message.id.clone(),
        actor_type: message.author_type.to_string(),
        actor_id: message.author_id.clone(),
        scope_type: "agent_chat".to_owned(),
        scope_id: message.chat_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!("agent-chat-event:{event_type}:{}", message.id)),
        payload_json: serde_json::json!({
            "message_id": message.id,
            "chat_id": message.chat_id,
            "sequence": message.sequence,
            "source_type": message.source_type,
        })
        .to_string(),
        created_at: message.created_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, transaction, &event).await
}

async fn append_agent_chat_turn_failure_event(
    db: &SqliteDb,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    turn: &AgentChatTurnJob,
    input: &FailAgentChatTurn,
) -> Result<DomainEvent> {
    // Error details are operational metadata, not a transcript. Keep the
    // event useful for Attention/recovery while preventing adapter output or
    // protected content from becoming an unbounded durable payload.
    let error_code = bounded_event_text(&input.error_code, 128);
    let error_message = bounded_event_text(&input.error_message, 512);
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "agent_chat.turn.failed".to_owned(),
        entity_type: "agent_chat_turn_job".to_owned(),
        entity_id: turn.id.clone(),
        actor_type: "system".to_owned(),
        actor_id: None,
        scope_type: "agent_chat".to_owned(),
        scope_id: turn.chat_id.clone(),
        correlation_id: turn.correlation_id.clone(),
        causation_id: turn.causation_id.clone(),
        causation_depth: turn.causation_depth,
        dedupe_key: Some(format!(
            "agent-chat-event:agent_chat.turn.failed:{}:{}",
            turn.id, input.expected_version
        )),
        payload_json: serde_json::json!({
            "turn_job_id": turn.id,
            "chat_id": turn.chat_id,
            "responder_identity_id": turn.responder_identity_id,
            "status": turn.status.to_string(),
            "attempt_count": turn.attempt_count,
            "max_attempts": turn.max_attempts,
            "error_code": error_code,
            "error_message": error_message,
            "next_attempt_at": turn.next_attempt_at,
            "version": turn.version,
        })
        .to_string(),
        created_at: input.updated_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, transaction, &event).await
}

async fn append_agent_chat_turn_awaiting_event(
    db: &SqliteDb,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    turn: &AgentChatTurnJob,
) -> Result<DomainEvent> {
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "agent_chat.turn.awaiting_input".to_owned(),
        entity_type: "agent_chat_turn_job".to_owned(),
        entity_id: turn.id.clone(),
        actor_type: "system".to_owned(),
        actor_id: None,
        scope_type: "agent_chat".to_owned(),
        scope_id: turn.chat_id.clone(),
        correlation_id: turn.correlation_id.clone(),
        causation_id: turn.causation_id.clone(),
        causation_depth: turn.causation_depth,
        dedupe_key: Some(format!(
            "agent-chat-event:agent_chat.turn.awaiting_input:{}:{}",
            turn.id, turn.version
        )),
        payload_json: serde_json::json!({
            "turn_job_id": turn.id,
            "chat_id": turn.chat_id,
            "responder_identity_id": turn.responder_identity_id,
            "status": turn.status.to_string(),
            "pending_interaction_id": turn.pending_interaction_id,
            "attempt_count": turn.attempt_count,
            "max_attempts": turn.max_attempts,
            "version": turn.version,
        })
        .to_string(),
        created_at: turn.updated_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, transaction, &event).await
}

fn bounded_event_text(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

async fn allocate_chat_sequence(
    transaction: &mut Transaction<'_, Sqlite>,
    chat_id: &str,
    timestamp: &str,
) -> Result<i64> {
    let count = sqlx::query_scalar::<_, i64>(
        "UPDATE agent_chat
         SET message_count = message_count + 1,
             last_message_at = CASE
                 WHEN last_message_at IS NULL OR last_message_at < ? THEN ?
                 ELSE last_message_at END,
             version = version + 1, updated_at = ?
         WHERE id = ?
         RETURNING message_count",
    )
    .bind(timestamp)
    .bind(timestamp)
    .bind(timestamp)
    .bind(chat_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    Ok(count - 1)
}

async fn insert_chat_message(
    transaction: &mut Transaction<'_, Sqlite>,
    input: &CreateAgentChatMessage,
) -> Result<AgentChatMessage> {
    sqlx::query(
        "INSERT INTO agent_chat_message (
            id, chat_id, sequence, author_type, author_id, content,
            content_guard_json, sensitivity, status, outcome, model, profile_id,
            session_id, context_manifest_id, token_usage_json, duration_ms, error,
            correlation_id, causation_id, handoff_id, source_type, source_id,
            source_message_id, source_room_id, source_conversation_id,
            source_sequence, source_metadata_json, created_at
             ) VALUES (
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?,
                 ?, ?, ?, ?
             )",
    )
    .bind(&input.id)
    .bind(&input.chat_id)
    .bind(input.sequence)
    .bind(input.author_type.to_string())
    .bind(input.author_id.as_deref())
    .bind(&input.content)
    .bind(&input.content_guard_json)
    .bind(&input.sensitivity)
    .bind(input.status.to_string())
    .bind(input.outcome.as_deref())
    .bind(input.model.as_deref())
    .bind(input.profile_id.as_deref())
    .bind(input.session_id.as_deref())
    .bind(input.context_manifest_id.as_deref())
    .bind(input.token_usage_json.as_deref())
    .bind(input.duration_ms)
    .bind(input.error.as_deref())
    .bind(&input.correlation_id)
    .bind(input.causation_id.as_deref())
    .bind(input.handoff_id.as_deref())
    .bind(&input.source_type)
    .bind(input.source_id.as_deref())
    .bind(input.source_message_id.as_deref())
    .bind(input.source_room_id.as_deref())
    .bind(input.source_conversation_id.as_deref())
    .bind(input.source_sequence)
    .bind(&input.source_metadata_json)
    .bind(&input.created_at)
    .execute(&mut **transaction)
    .await
    .map_err(map_chat_write_error)?;
    sqlx::query("SELECT * FROM agent_chat_message WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(DbError::from)
        .and_then(map_agent_chat_message)
}

fn map_account_main_binding(row: SqliteRow) -> Result<AccountMainAgentBinding> {
    Ok(AccountMainAgentBinding {
        id: row.try_get("id")?,
        account_id: row.try_get("account_id")?,
        identity_id: row.try_get("identity_id")?,
        profile_id: row.try_get("profile_id")?,
        state: row.try_get("state")?,
        autonomy_policy_json: row.try_get("autonomy_policy_json")?,
        tool_policy_revision: row.try_get("tool_policy_revision")?,
        version: row.try_get("version")?,
        replaced_by_binding_id: row.try_get("replaced_by_binding_id")?,
        replacement_reason: row.try_get("replacement_reason")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_project_agent_binding(row: SqliteRow) -> Result<ProjectAgentBinding> {
    Ok(ProjectAgentBinding {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        identity_id: row.try_get("identity_id")?,
        profile_id: row.try_get("profile_id")?,
        state: row.try_get("state")?,
        autonomy_policy_json: row.try_get("autonomy_policy_json")?,
        permission_ceiling_json: row.try_get("permission_ceiling_json")?,
        subscriptions_json: row.try_get("subscriptions_json")?,
        wake_budget: row.try_get("wake_budget")?,
        operating_skill_revision_id: row.try_get("operating_skill_revision_id")?,
        policy_revision: row.try_get("policy_revision")?,
        policy_digest: row.try_get("policy_digest")?,
        charter_id: row.try_get("charter_id")?,
        charter_revision_id: row.try_get("charter_revision_id")?,
        charter_setup_required: row.try_get::<i64, _>("charter_setup_required")? != 0,
        admission_receipt_id: row.try_get("admission_receipt_id")?,
        charter_approval_id: row.try_get("charter_approval_id")?,
        version: row.try_get("version")?,
        replaced_by_binding_id: row.try_get("replaced_by_binding_id")?,
        replacement_reason: row.try_get("replacement_reason")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_project_admission_receipt(row: SqliteRow) -> Result<ProjectAdmissionReceipt> {
    Ok(ProjectAdmissionReceipt {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        source_kind: row.try_get("source_kind")?,
        handoff_id: row.try_get("handoff_id")?,
        initial_charter_approval_id: row.try_get("initial_charter_approval_id")?,
        initial_charter_id: row.try_get("initial_charter_id")?,
        initial_charter_revision_id: row.try_get("initial_charter_revision_id")?,
        payload_digest: row.try_get("payload_digest")?,
        validation_schema_version: row.try_get("validation_schema_version")?,
        validated_at: row.try_get("validated_at")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_agent_chat(row: SqliteRow) -> Result<AgentChat> {
    Ok(AgentChat {
        id: row.try_get("id")?,
        kind: row.try_get("kind")?,
        account_id: row.try_get("account_id")?,
        project_id: row.try_get("project_id")?,
        status: row.try_get("status")?,
        instruction_revision: row.try_get("instruction_revision")?,
        message_count: row.try_get("message_count")?,
        last_message_at: row.try_get("last_message_at")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_agent_chat_source_ref(row: SqliteRow) -> Result<AgentChatSourceRef> {
    Ok(AgentChatSourceRef {
        chat_id: row.try_get("chat_id")?,
        source_type: row.try_get("source_type")?,
        source_id: row.try_get("source_id")?,
        source_scope_type: row.try_get("source_scope_type")?,
        source_scope_id: row.try_get("source_scope_id")?,
        source_revision: row.try_get("source_revision")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_agent_chat_instruction(row: SqliteRow) -> Result<AgentChatInstructionRevision> {
    Ok(AgentChatInstructionRevision {
        id: row.try_get("id")?,
        chat_id: row.try_get("chat_id")?,
        source_type: row.try_get("source_type")?,
        source_id: row.try_get("source_id")?,
        revision: row.try_get("revision")?,
        body: row.try_get("body")?,
        content_guard_json: row.try_get("content_guard_json")?,
        sensitivity: row.try_get("sensitivity")?,
        created_by_type: row.try_get("created_by_type")?,
        created_by_id: row.try_get("created_by_id")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_agent_chat_message(row: SqliteRow) -> Result<AgentChatMessage> {
    Ok(AgentChatMessage {
        id: row.try_get("id")?,
        chat_id: row.try_get("chat_id")?,
        sequence: row.try_get("sequence")?,
        author_type: parse_enum(row.try_get::<String, _>("author_type")?)?,
        author_id: row.try_get("author_id")?,
        content: row.try_get("content")?,
        content_guard_json: row.try_get("content_guard_json")?,
        sensitivity: row.try_get("sensitivity")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        outcome: row.try_get("outcome")?,
        model: row.try_get("model")?,
        profile_id: row.try_get("profile_id")?,
        session_id: row.try_get("session_id")?,
        context_manifest_id: row.try_get("context_manifest_id")?,
        token_usage_json: row.try_get("token_usage_json")?,
        duration_ms: row.try_get("duration_ms")?,
        error: row.try_get("error")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        handoff_id: row.try_get("handoff_id")?,
        source_type: row.try_get("source_type")?,
        source_id: row.try_get("source_id")?,
        source_message_id: row.try_get("source_message_id")?,
        source_room_id: row.try_get("source_room_id")?,
        source_conversation_id: row.try_get("source_conversation_id")?,
        source_sequence: row.try_get("source_sequence")?,
        source_metadata_json: row.try_get("source_metadata_json")?,
        created_at: row.try_get("created_at")?,
    })
}

fn map_agent_chat_turn_job(row: SqliteRow) -> Result<AgentChatTurnJob> {
    Ok(AgentChatTurnJob {
        id: row.try_get("id")?,
        chat_id: row.try_get("chat_id")?,
        triggering_message_id: row.try_get("triggering_message_id")?,
        responder_identity_id: row.try_get("responder_identity_id")?,
        profile_id: row.try_get("profile_id")?,
        responder_binding_id: row.try_get("responder_binding_id")?,
        responder_binding_version: row.try_get("responder_binding_version")?,
        responder_identity_version: row.try_get("responder_identity_version")?,
        profile_version: row.try_get("profile_version")?,
        operating_skill_revision_id: row.try_get("operating_skill_revision_id")?,
        policy_revision: row.try_get("policy_revision")?,
        policy_digest: row.try_get("policy_digest")?,
        permission_policy_digest: row.try_get("permission_policy_digest")?,
        tool_policy_digest: row.try_get("tool_policy_digest")?,
        admission_digest: row.try_get("admission_digest")?,
        canonical_scope_provenance_json: row.try_get("canonical_scope_provenance_json")?,
        canonical_scope_type: row.try_get("canonical_scope_type")?,
        canonical_scope_id: row.try_get("canonical_scope_id")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        pending_interaction_id: row.try_get("pending_interaction_id")?,
        dedupe_key: row.try_get("dedupe_key")?,
        lease_owner: row.try_get("lease_owner")?,
        leased_until: row.try_get("leased_until")?,
        attempt_count: row.try_get("attempt_count")?,
        max_attempts: row.try_get("max_attempts")?,
        next_attempt_at: row.try_get("next_attempt_at")?,
        response_message_id: row.try_get("response_message_id")?,
        error_code: row.try_get("error_code")?,
        error_message: row.try_get("error_message")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        causation_depth: row.try_get("causation_depth")?,
        version: row.try_get("version")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn turn_admission_semantics_match(
    input: &CreateAgentChatTurnJob,
    existing: &AgentChatTurnJob,
) -> bool {
    match (&input.admission_digest, &existing.admission_digest) {
        (Some(expected), Some(actual)) => {
            expected == actual
                && input.responder_binding_id == existing.responder_binding_id
                && input.responder_binding_version == existing.responder_binding_version
                && input.responder_identity_version == existing.responder_identity_version
                && input.profile_version == existing.profile_version
                && input.operating_skill_revision_id == existing.operating_skill_revision_id
                && input.policy_revision == existing.policy_revision
                && input.policy_digest == existing.policy_digest
                && input.permission_policy_digest == existing.permission_policy_digest
                && input.tool_policy_digest == existing.tool_policy_digest
                && input.canonical_scope_provenance_json == existing.canonical_scope_provenance_json
        }
        // Legacy rows predate the admission digest.  Retain their old
        // dedupe behavior, but never treat a newly prepared admission as an
        // authenticated replay of one of those rows.
        (None, None) => {
            input.chat_id == existing.chat_id
                && input.responder_identity_id
                    == existing
                        .responder_identity_id
                        .as_deref()
                        .unwrap_or_default()
                && input.profile_id == existing.profile_id.as_deref().unwrap_or_default()
                && input.canonical_scope_type == existing.canonical_scope_type
                && input.canonical_scope_id == existing.canonical_scope_id
                && input.dedupe_key == existing.dedupe_key
                && input.causation_id == existing.causation_id
                && input.causation_depth == existing.causation_depth
        }
        _ => false,
    }
}

/// Revalidate the resolver's expected-version contract while the admission
/// transaction holds SQLite's IMMEDIATE write lock.  This deliberately does
/// not require the historical binding to remain active after admission; it
/// only prevents a resolver snapshot from straddling a concurrent replacement
/// or Profile selection edit.
async fn validate_agent_chat_turn_admission(
    transaction: &mut Transaction<'_, Sqlite>,
    turn: &CreateAgentChatTurnJob,
) -> Result<()> {
    let frozen = [
        turn.responder_binding_id.is_some(),
        turn.responder_binding_version.is_some(),
        turn.responder_identity_version.is_some(),
        turn.profile_version.is_some(),
        turn.operating_skill_revision_id.is_some(),
        turn.policy_revision.is_some(),
        turn.policy_digest.is_some(),
        turn.permission_policy_digest.is_some(),
        turn.tool_policy_digest.is_some(),
        turn.admission_digest.is_some(),
        turn.canonical_scope_provenance_json.is_some(),
    ];
    if frozen.iter().all(|present| !present) {
        // Pre-V088 producers remain processable through the conservative
        // legacy worker path.  New producers must populate the complete set.
        return Ok(());
    }
    if frozen.iter().any(|present| !present)
        || turn.canonical_scope_type != "agent_chat"
        || turn.canonical_scope_id != turn.chat_id
    {
        return Err(DbError::Check(
            "agent turn admission provenance is incomplete or out of scope".to_owned(),
        ));
    }

    let chat = sqlx::query(
        "SELECT kind, account_id, project_id, status
         FROM agent_chat WHERE id = ?",
    )
    .bind(&turn.chat_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    let kind: String = chat.try_get("kind")?;
    let account_id: Option<String> = chat.try_get("account_id")?;
    let project_id: Option<String> = chat.try_get("project_id")?;
    let status: String = chat.try_get("status")?;
    if status != "ready" {
        return Err(DbError::VersionConflict);
    }

    let (
        binding_identity_id,
        binding_policy_revision,
        binding_policy_digest,
        binding_permission_json,
        binding_skill_revision,
    ) = match kind.as_str() {
        "account_main" => {
            let account_id = account_id
                .ok_or_else(|| DbError::Check("Main Chat has no account scope".to_owned()))?;
            let row = sqlx::query(
                "SELECT identity_id, version, state, autonomy_policy_json,
                        tool_policy_revision
                 FROM account_main_agent_binding
                 WHERE id = ? AND account_id = ?",
            )
            .bind(turn.responder_binding_id.as_deref())
            .bind(account_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(DbError::VersionConflict)?;
            let state: String = row.try_get("state")?;
            let version: i64 = row.try_get("version")?;
            if state != "active" || Some(version) != turn.responder_binding_version {
                return Err(DbError::VersionConflict);
            }
            let autonomy_policy_json: String = row.try_get("autonomy_policy_json")?;
            let tool_policy_revision: String = row.try_get("tool_policy_revision")?;
            (
                row.try_get("identity_id")?,
                tool_policy_revision,
                admission_policy_digest(&autonomy_policy_json)?,
                None,
                None,
            )
        }
        "project" => {
            let project_id = project_id
                .ok_or_else(|| DbError::Check("Project Chat has no project scope".to_owned()))?;
            let row = sqlx::query(
                "SELECT identity_id, version, state, permission_ceiling_json,
                        operating_skill_revision_id, policy_revision, policy_digest
                 FROM project_agent_binding
                 WHERE id = ? AND project_id = ?",
            )
            .bind(turn.responder_binding_id.as_deref())
            .bind(project_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(DbError::VersionConflict)?;
            let state: String = row.try_get("state")?;
            let version: i64 = row.try_get("version")?;
            if state != "active" || Some(version) != turn.responder_binding_version {
                return Err(DbError::VersionConflict);
            }
            let permission_json: String = row.try_get("permission_ceiling_json")?;
            let policy_revision: String = row.try_get("policy_revision")?;
            let stored_policy_digest: String = row.try_get("policy_digest")?;
            let effective_policy_digest = if stored_policy_digest.trim().is_empty() {
                admission_policy_digest(&permission_json)?
            } else {
                stored_policy_digest
            };
            (
                row.try_get::<Option<String>, _>("identity_id")?
                    .ok_or_else(|| DbError::VersionConflict)?,
                policy_revision,
                effective_policy_digest,
                Some(permission_json),
                row.try_get("operating_skill_revision_id")?,
            )
        }
        _ => return Err(DbError::Check("unsupported Agent Chat kind".to_owned())),
    };

    if binding_identity_id != turn.responder_identity_id {
        return Err(DbError::VersionConflict);
    }
    let identity = sqlx::query(
        "SELECT version, selected_profile_id, account_permission_ceiling, paused
         FROM agent_identity WHERE id = ?",
    )
    .bind(&turn.responder_identity_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::VersionConflict)?;
    let identity_version: i64 = identity.try_get("version")?;
    let selected_profile_id: Option<String> = identity.try_get("selected_profile_id")?;
    let account_permission_json: String = identity.try_get("account_permission_ceiling")?;
    let paused: i64 = identity.try_get("paused")?;
    if paused != 0 {
        return Err(DbError::VersionConflict);
    }
    if Some(identity_version) != turn.responder_identity_version {
        return Err(DbError::VersionConflict);
    }
    let provenance: serde_json::Value = serde_json::from_str(
        turn.canonical_scope_provenance_json
            .as_deref()
            .ok_or_else(|| DbError::Check("missing turn provenance".to_owned()))?,
    )
    .map_err(|_| DbError::Check("turn provenance JSON is invalid".to_owned()))?;
    if provenance
        .get("identity_version")
        .and_then(serde_json::Value::as_i64)
        != Some(identity_version)
    {
        return Err(DbError::VersionConflict);
    }
    if turn.policy_revision.as_deref() != Some(binding_policy_revision.as_str())
        || turn.policy_digest.as_deref() != Some(binding_policy_digest.as_str())
    {
        return Err(DbError::VersionConflict);
    }
    let permission_json = binding_permission_json
        .as_deref()
        .unwrap_or(&account_permission_json);
    if turn.permission_policy_digest.as_deref()
        != Some(admission_policy_digest(permission_json)?.as_str())
    {
        return Err(DbError::VersionConflict);
    }
    let profile = sqlx::query(
        "SELECT identity_id, version, tool_policy_json
         FROM agent_profile WHERE id = ?",
    )
    .bind(&turn.profile_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(DbError::VersionConflict)?;
    let profile_identity_id: String = profile.try_get("identity_id")?;
    let profile_version: i64 = profile.try_get("version")?;
    let profile_tool_policy_json: String = profile.try_get("tool_policy_json")?;
    if profile_identity_id != turn.responder_identity_id
        || selected_profile_id.as_deref() != Some(turn.profile_id.as_str())
        || Some(profile_version) != turn.profile_version
        || turn.tool_policy_digest.as_deref()
            != Some(admission_policy_digest(&profile_tool_policy_json)?.as_str())
    {
        return Err(DbError::VersionConflict);
    }

    let expected_skill_revision = if kind == "account_main" {
        let genesis_active = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(
                 SELECT 1 FROM product_genesis_session
                 WHERE main_chat_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
             )",
        )
        .bind(&turn.chat_id)
        .fetch_one(&mut **transaction)
        .await?
            != 0;
        if genesis_active {
            sqlx::query_scalar::<_, String>(
                "SELECT current_revision_id FROM operating_skill
                 WHERE id = 'forge.main.project-discovery/v2' AND lifecycle = 'active'",
            )
            .fetch_optional(&mut **transaction)
            .await?
        } else {
            let revision = turn
                .operating_skill_revision_id
                .as_deref()
                .filter(|revision| supported_main_baseline_revision(revision))
                .ok_or(DbError::VersionConflict)?;
            Some(revision.to_owned())
        }
    } else {
        binding_skill_revision
    };
    if expected_skill_revision.as_deref() != turn.operating_skill_revision_id.as_deref() {
        return Err(DbError::VersionConflict);
    }

    Ok(())
}

/// The compiled Main baseline revisions admission will accept.
///
/// This must list every revision `operating_skills` still resolves a body
/// for. Bumping the baseline without adding it here rejects every new Main
/// Chat message with a bare version conflict, which is exactly as confusing
/// as it sounds -- see the paired test.
pub fn supported_main_baseline_revision(revision: &str) -> bool {
    matches!(
        revision,
        "forge.main.baseline/v1@1" | "forge.main.baseline/v1@2" | "forge.main.baseline/v1@3"
    )
}

#[cfg(test)]
mod main_baseline_revision_tests {
    use super::supported_main_baseline_revision;

    #[test]
    fn only_frozen_main_baseline_revisions_are_supported() {
        assert!(supported_main_baseline_revision("forge.main.baseline/v1@1"));
        assert!(supported_main_baseline_revision("forge.main.baseline/v1@2"));
        assert!(supported_main_baseline_revision("forge.main.baseline/v1@3"));
        // A revision this build has no compiled body for is refused, so a
        // downgrade cannot render a contract it does not have.
        assert!(!supported_main_baseline_revision(
            "forge.main.baseline/v1@4"
        ));
        assert!(!supported_main_baseline_revision(
            "forge.main.project-discovery/v2@2"
        ));
        assert!(!supported_main_baseline_revision(""));
    }
}

pub(crate) fn admission_policy_digest(value: &str) -> Result<String> {
    let parsed: serde_json::Value = serde_json::from_str(value)
        .map_err(|_| DbError::Check("Agent Chat policy JSON is invalid".to_owned()))?;
    let envelope = serde_json::json!({
        "schema_version": "forge.agent-turn-policy/v1",
        "value": canonicalize_admission_json(&parsed),
    });
    let canonical = serde_json::to_string(&canonicalize_admission_json(&envelope))
        .map_err(|_| DbError::Check("Agent Chat policy JSON is invalid".to_owned()))?;
    Ok(hex::encode(Sha256::digest(canonical.as_bytes())))
}

/// Compute the same schema-versioned digest used by the services admission
/// resolver for a pre-canonicalized JSON value.  The Genesis composite lives
/// in `db`, so it uses this helper rather than importing service-layer types.
pub(crate) fn admission_digest_for_json(value: &serde_json::Value) -> Result<String> {
    let envelope = serde_json::json!({
        "schema_version": "forge.agent-turn-admission/v1",
        "value": value,
    });
    let canonical = serde_json::to_string(&canonicalize_admission_json(&envelope))
        .map_err(|_| DbError::Check("Agent Chat admission JSON is invalid".to_owned()))?;
    Ok(hex::encode(Sha256::digest(canonical.as_bytes())))
}

pub(crate) fn handoff_content_digest_for_admission(
    content: &str,
    source_revisions_json: &str,
    source_message_id: Option<&str>,
    source_turn_job_id: Option<&str>,
) -> Result<String> {
    let value = serde_json::json!({
        "content": content,
        "source_message_id": source_message_id,
        "source_revisions_json": source_revisions_json,
        "source_turn_job_id": source_turn_job_id,
    });
    let envelope = serde_json::json!({
        "schema_version": "forge.agent-chat-content/v1",
        "value": value,
    });
    let canonical = serde_json::to_string(&canonicalize_admission_json(&envelope))
        .map_err(|_| DbError::Check("Agent Chat handoff content is invalid".to_owned()))?;
    Ok(hex::encode(Sha256::digest(canonical.as_bytes())))
}

pub(crate) fn handoff_admission_digest_for_provenance(
    dedupe_key: &str,
    content_digest: &str,
    causation_depth: i64,
    target_provenance: &serde_json::Value,
    source_provenance: Option<&serde_json::Value>,
) -> Result<String> {
    let value = serde_json::json!({
        "causation_depth": causation_depth,
        "causation_id": serde_json::Value::Null,
        "content_digest": content_digest,
        "dedupe_key": dedupe_key,
        "responder": target_provenance,
        "source_responder": source_provenance,
        "trigger": "main_project_handoff",
    });
    admission_digest_for_json(&value)
}

pub(crate) async fn validate_agent_chat_turn_job_id(
    transaction: &mut Transaction<'_, Sqlite>,
    turn_job_id: &str,
) -> Result<()> {
    let row = sqlx::query("SELECT * FROM agent_chat_turn_job WHERE id = ?")
        .bind(turn_job_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::VersionConflict)?;
    let turn = map_agent_chat_turn_job(row)?;
    let input = CreateAgentChatTurnJob {
        id: turn.id,
        chat_id: turn.chat_id,
        triggering_message_id: turn.triggering_message_id,
        responder_identity_id: turn.responder_identity_id.unwrap_or_default(),
        profile_id: turn.profile_id.unwrap_or_default(),
        responder_binding_id: turn.responder_binding_id,
        responder_binding_version: turn.responder_binding_version,
        responder_identity_version: turn.responder_identity_version,
        profile_version: turn.profile_version,
        operating_skill_revision_id: turn.operating_skill_revision_id,
        policy_revision: turn.policy_revision,
        policy_digest: turn.policy_digest,
        permission_policy_digest: turn.permission_policy_digest,
        tool_policy_digest: turn.tool_policy_digest,
        admission_digest: turn.admission_digest,
        canonical_scope_provenance_json: turn.canonical_scope_provenance_json,
        canonical_scope_type: turn.canonical_scope_type,
        canonical_scope_id: turn.canonical_scope_id,
        dedupe_key: turn.dedupe_key,
        max_attempts: turn.max_attempts,
        correlation_id: turn.correlation_id,
        causation_id: turn.causation_id,
        causation_depth: turn.causation_depth,
        created_at: turn.created_at,
        updated_at: turn.updated_at,
    };
    validate_agent_chat_turn_admission(transaction, &input).await
}

fn canonicalize_admission_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(values) => {
            let mut keys: Vec<_> = values.keys().collect();
            keys.sort_unstable();
            let mut result = serde_json::Map::new();
            for key in keys {
                result.insert(key.clone(), canonicalize_admission_json(&values[key]));
            }
            serde_json::Value::Object(result)
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(canonicalize_admission_json).collect())
        }
        scalar => scalar.clone(),
    }
}

#[derive(Debug, Deserialize)]
struct HandoffSourceResponderProvenance {
    chat_id: String,
    canonical_scope_type: String,
    canonical_scope_id: String,
    readiness: String,
    binding_id: Option<String>,
    binding_version: Option<i64>,
    identity_id: Option<String>,
    identity_version: Option<i64>,
    profile_id: Option<String>,
    profile_version: Option<i64>,
    operating_skill_revision: Option<String>,
    policy_revision: Option<String>,
    policy_digest: Option<String>,
    permission_policy_digest: Option<String>,
    tool_policy_digest: Option<String>,
}

async fn validate_handoff_source_responder(
    transaction: &mut Transaction<'_, Sqlite>,
    source_chat_id: &str,
    provenance_json: &str,
    expected_author_identity_id: Option<&str>,
) -> Result<()> {
    let provenance: HandoffSourceResponderProvenance = serde_json::from_str(provenance_json)
        .map_err(|_| DbError::Check("handoff source responder provenance is invalid".to_owned()))?;
    if provenance.readiness != "ready"
        || provenance.chat_id != source_chat_id
        || provenance.canonical_scope_type != "agent_chat"
        || provenance.canonical_scope_id != source_chat_id
    {
        return Err(DbError::VersionConflict);
    }
    let identity_id = provenance
        .identity_id
        .ok_or_else(|| DbError::VersionConflict)?;
    if expected_author_identity_id != Some(identity_id.as_str()) {
        return Err(DbError::VersionConflict);
    }
    let profile_id = provenance
        .profile_id
        .ok_or_else(|| DbError::VersionConflict)?;
    let profile_version = provenance
        .profile_version
        .ok_or_else(|| DbError::VersionConflict)?;
    let turn = CreateAgentChatTurnJob {
        id: "handoff-source-responder-validation".to_owned(),
        chat_id: source_chat_id.to_owned(),
        triggering_message_id: "handoff-source-responder-validation".to_owned(),
        responder_identity_id: identity_id,
        profile_id,
        responder_binding_id: provenance.binding_id,
        responder_binding_version: provenance.binding_version,
        responder_identity_version: provenance.identity_version,
        profile_version: Some(profile_version),
        operating_skill_revision_id: provenance.operating_skill_revision,
        policy_revision: provenance.policy_revision,
        policy_digest: provenance.policy_digest,
        permission_policy_digest: provenance.permission_policy_digest,
        tool_policy_digest: provenance.tool_policy_digest,
        admission_digest: Some("handoff-source-responder-validation".to_owned()),
        canonical_scope_provenance_json: Some(provenance_json.to_owned()),
        canonical_scope_type: "agent_chat".to_owned(),
        canonical_scope_id: source_chat_id.to_owned(),
        dedupe_key: "handoff-source-responder-validation".to_owned(),
        max_attempts: 1,
        correlation_id: "handoff-source-responder-validation".to_owned(),
        causation_id: None,
        causation_depth: 0,
        created_at: "handoff-source-responder-validation".to_owned(),
        updated_at: "handoff-source-responder-validation".to_owned(),
    };
    validate_agent_chat_turn_admission(transaction, &turn).await
}

fn map_agent_handoff(row: SqliteRow) -> Result<AgentHandoff> {
    Ok(AgentHandoff {
        id: row.try_get("id")?,
        source_chat_id: row.try_get("source_chat_id")?,
        target_chat_id: row.try_get("target_chat_id")?,
        source_message_id: row.try_get("source_message_id")?,
        source_turn_job_id: row.try_get("source_turn_job_id")?,
        target_message_id: row.try_get("target_message_id")?,
        target_turn_job_id: row.try_get("target_turn_job_id")?,
        author_identity_id: row.try_get("author_identity_id")?,
        content: row.try_get("content")?,
        content_guard_json: row.try_get("content_guard_json")?,
        source_revisions_json: row.try_get("source_revisions_json")?,
        status: parse_enum(row.try_get::<String, _>("status")?)?,
        error_code: row.try_get("error_code")?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        dedupe_key: row.try_get("dedupe_key")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn map_binding_write_error(error: sqlx::Error) -> DbError {
    if error.to_string().to_ascii_lowercase().contains("unique") {
        DbError::Check("only one active Main/Project binding is allowed".to_owned())
    } else {
        error.into()
    }
}

fn map_chat_write_error(error: sqlx::Error) -> DbError {
    if error.to_string().to_ascii_lowercase().contains("unique") {
        DbError::Check("duplicate Agent Chat id, sequence, or deduplication key".to_owned())
    } else {
        error.into()
    }
}
