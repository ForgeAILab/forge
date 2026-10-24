//! User attention actions and scope authorization.

use super::*;

impl AttentionService {
    pub async fn list_for_user(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        status: Option<&str>,
        include_snoozed: bool,
        page: PageRequest,
    ) -> Result<(
        Page<AttentionProjection>,
        Option<AttentionConsumerHealthResponse>,
    )> {
        if let Some(project_id) = project_id {
            self.require_project_access(user_id, project_id).await?;
        }
        let items = AttentionRepo::list_attention(
            &*self.db,
            AttentionListQuery {
                account_id: Some(user_id.to_owned()),
                project_id: project_id.map(str::to_owned),
                scope_type: None,
                status: status.map(str::to_owned),
                include_snoozed,
                page,
            },
        )
        .await?;
        let health = self.consumer_health().await?;
        Ok((items, health))
    }

    pub async fn acknowledge(
        &self,
        user_id: &str,
        id: &str,
        expected_version: i64,
    ) -> Result<AttentionProjection> {
        let current = self.authorized_attention(user_id, id).await?;
        if current.status == "resolved" {
            return Err(ServiceError::conflict(
                "resolved attention cannot be acknowledged",
            ));
        }
        AttentionRepo::update_attention_lifecycle(
            &*self.db,
            UpdateAttentionLifecycle {
                id: id.to_owned(),
                expected_version,
                status: "acknowledged".to_owned(),
                acknowledged_at: Some(Some(now_rfc3339())),
                snoozed_until: Some(None),
                resolved_at: Some(None),
                updated_by_user_id: Some(user_id.to_owned()),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(ServiceError::from)
    }

    pub async fn snooze(
        &self,
        user_id: &str,
        id: &str,
        expected_version: i64,
        snoozed_until: &str,
    ) -> Result<AttentionProjection> {
        let current = self.authorized_attention(user_id, id).await?;
        if current.status == "resolved" {
            return Err(ServiceError::conflict(
                "resolved attention cannot be snoozed",
            ));
        }
        let until = DateTime::parse_from_rfc3339(snoozed_until)
            .map_err(|_| ServiceError::invalid_operation("snoozed_until must be RFC3339"))?
            .with_timezone(&Utc);
        let now = Utc::now();
        if until <= now || until > now + Duration::days(30) {
            return Err(ServiceError::invalid_operation(
                "snoozed_until must be within the next 30 days",
            ));
        }
        AttentionRepo::update_attention_lifecycle(
            &*self.db,
            UpdateAttentionLifecycle {
                id: id.to_owned(),
                expected_version,
                status: "acknowledged".to_owned(),
                acknowledged_at: Some(Some(now_rfc3339())),
                snoozed_until: Some(Some(until.to_rfc3339())),
                resolved_at: Some(None),
                updated_by_user_id: Some(user_id.to_owned()),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(ServiceError::from)
    }

    pub async fn resolve(
        &self,
        user_id: &str,
        id: &str,
        expected_version: i64,
    ) -> Result<AttentionProjection> {
        self.resolve_with_answer(user_id, id, expected_version, None)
            .await
    }

    /// Resolve an item. An item that carries a lead's question
    /// ([`ANSWER_LEAD_INPUT`]) is resolved only with an answer: the answer
    /// becomes a comment on the root, the root's wait ends and its lead runs
    /// again. Every other item takes no answer.
    pub async fn resolve_with_answer(
        &self,
        user_id: &str,
        id: &str,
        expected_version: i64,
        answer: Option<&str>,
    ) -> Result<AttentionProjection> {
        let current = self.authorized_attention(user_id, id).await?;
        let answer = answer.map(str::trim).filter(|answer| !answer.is_empty());
        if current.status == "resolved" {
            if current.version != expected_version {
                return Err(ServiceError::from(db::DbError::VersionConflict));
            }
            return Ok(current);
        }
        if current.recommended_action == ANSWER_LEAD_INPUT {
            if current.version != expected_version {
                return Err(ServiceError::from(db::DbError::VersionConflict));
            }
            let details =
                serde_json::from_str::<Value>(&current.details_json).unwrap_or(Value::Null);
            // Read by a person (a client without an answer box shows it as
            // is): what the item is, the question, and what to send.
            let answer = answer.filter(|answer| answer.chars().count() <= 4000).ok_or_else(|| {
                ServiceError::invalid_operation(format!(
                    "This item is a question from a Task's lead and is resolved by answering it. Question: \"{}\". Send the answer as `answer` (1 to 4000 characters) with `expected_version` to POST /api/v1/mission-control/attention/{id}/resolve; the Task waits until then.",
                    details["question"]
                        .as_str()
                        .unwrap_or(current.summary.as_str())
                        .chars()
                        .take(500)
                        .collect::<String>()
                ))
            })?;
            let (Some(task_id), Some(comment_id)) =
                (details["task_id"].as_str(), details["comment_id"].as_str())
            else {
                return Err(ServiceError::invalid_operation(
                    "Attention item has no question to answer",
                ));
            };
            match crate::task_service::lead::answer_lead_input(
                &self.db, task_id, comment_id, answer, "owner",
            )
            .await
            {
                // A deleted root has nobody to read the answer; its question
                // is closed all the same, so the item is never unresolvable.
                Ok(_) | Err(ServiceError::NotFound { .. }) => {}
                Err(error) => return Err(error),
            }
            // The answer resolved the item with the wait. A root that had
            // already left its planning state has no wait left: close it here.
            let latest = self
                .db
                .get_attention(id)
                .await?
                .ok_or_else(|| ServiceError::not_found("attention", id.to_owned()))?;
            if latest.status == "resolved" {
                return Ok(latest);
            }
            return AttentionRepo::update_attention_lifecycle(
                &*self.db,
                UpdateAttentionLifecycle {
                    id: id.to_owned(),
                    expected_version: latest.version,
                    status: "resolved".to_owned(),
                    acknowledged_at: None,
                    snoozed_until: Some(None),
                    resolved_at: Some(Some(now_rfc3339())),
                    updated_by_user_id: Some(user_id.to_owned()),
                    updated_at: now_rfc3339(),
                },
            )
            .await
            .map_err(ServiceError::from);
        }
        if answer.is_some() {
            return Err(ServiceError::invalid_operation(
                "this Attention item takes no answer",
            ));
        }
        // Resolving an owner escalation is the owner's answer: it closes the
        // escalation and wakes the Agent, so no blocker stays un-wakeable.
        if current.recommended_action == "answer_escalation" {
            if current.version != expected_version {
                return Err(ServiceError::from(db::DbError::VersionConflict));
            }
            crate::project_escalation::ProjectEscalationService::new(Arc::clone(&self.db))
                .resolve_by_owner(&current, user_id)
                .await?;
            return self
                .db
                .get_attention(id)
                .await?
                .ok_or_else(|| ServiceError::not_found("attention", id.to_owned()));
        }
        AttentionRepo::update_attention_lifecycle(
            &*self.db,
            UpdateAttentionLifecycle {
                id: id.to_owned(),
                expected_version,
                status: "resolved".to_owned(),
                acknowledged_at: None,
                snoozed_until: Some(None),
                resolved_at: Some(Some(now_rfc3339())),
                updated_by_user_id: Some(user_id.to_owned()),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(ServiceError::from)
    }

    async fn authorized_attention(&self, user_id: &str, id: &str) -> Result<AttentionProjection> {
        let item = AttentionRepo::get_attention(&*self.db, id)
            .await?
            .ok_or_else(|| ServiceError::not_found("attention", id.to_owned()))?;
        self.require_scope_access(user_id, &item.scope_type, &item.scope_id)
            .await?;
        Ok(item)
    }

    pub(super) async fn require_scope_access(
        &self,
        user_id: &str,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<()> {
        match scope_type {
            "account" => {
                if scope_id != user_id {
                    return Err(ServiceError::not_found("attention", scope_id.to_owned()));
                }
            }
            "project" => self.require_project_access(user_id, scope_id).await?,
            "agent_chat" => {
                let scope = self
                    .db
                    .attention_chat_account_project_scope(scope_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("attention", scope_id.to_owned()))?;
                if let Some(project_id) = scope.1 {
                    self.require_project_access(user_id, &project_id).await?;
                } else if scope.0.as_deref() != Some(user_id) {
                    return Err(ServiceError::not_found("attention", scope_id.to_owned()));
                }
            }
            "task" => {
                let project_id = self
                    .db
                    .attention_scope_task_project_id(scope_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", scope_id.to_owned()))?;
                self.require_project_access(user_id, &project_id).await?;
            }
            "agent" => self.require_agent_access(user_id, scope_id).await?,
            _ => return Err(ServiceError::not_found("attention", scope_id.to_owned())),
        }
        Ok(())
    }

    pub(super) async fn require_project_access(
        &self,
        user_id: &str,
        project_id: &str,
    ) -> Result<()> {
        let project = ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        if project.owner_id.is_none() || project.owner_id.as_deref() == Some(user_id) {
            return Ok(());
        }
        ProjectMemberRepo::get_member(&*self.db, project_id, user_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        Ok(())
    }

    pub(super) async fn require_agent_access(
        &self,
        user_id: &str,
        identity_id: &str,
    ) -> Result<()> {
        let row = self
            .db
            .attention_identity_owner(identity_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", identity_id.to_owned()))?;
        let owner_id: Option<String> = row.owner_id?;
        if owner_id.is_none() || owner_id.as_deref() == Some(user_id) {
            return Ok(());
        }
        let visible = self
            .db
            .attention_identity_visible_project_count(user_id, identity_id, user_id)
            .await?;
        if visible == 0 {
            return Err(ServiceError::not_found("agent", identity_id.to_owned()));
        }
        Ok(())
    }
}
