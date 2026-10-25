//! Main and Project Agent bindings and admission receipts.

use super::*;

impl SqliteDb {
    /// A delegated Project grant filters to one visible Project before its
    /// cursor/count projection; it never reads or paginates the account list.
    pub async fn list_visible_bound_project(
        &self,
        id: &str,
        user_id: &str,
        page: PageRequest,
    ) -> Result<Page<Project>> {
        let offset = decode_offset(&page.cursor)?;
        let visible = ProjectRepo::get_visible_by_id(self, id, user_id).await?;
        let total = i64::from(visible.is_some());
        let items = if offset == 0 {
            visible.into_iter().collect()
        } else {
            Vec::new()
        };
        page_from_items(items, &page, offset, Some(total))
    }
}

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
        .bind({
            operation_registry::authority::parse_permissions(&input.permission_ceiling_json)?;
            &input.permission_ceiling_json
        })
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
        .bind({
            operation_registry::authority::parse_permissions(
                &input.replacement.permission_ceiling_json,
            )?;
            &input.replacement.permission_ceiling_json
        })
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
        .bind({
            operation_registry::authority::parse_permissions(
                &input.replacement.permission_ceiling_json,
            )?;
            &input.replacement.permission_ceiling_json
        })
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
