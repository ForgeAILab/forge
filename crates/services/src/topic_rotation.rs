//! Durable topic rotation at the idle boundary before turn admission.
use crate::{EmbeddedAgentService, Result, ServiceError};
use async_trait::async_trait;
use db::{
    AgentChatRepo, AgentChatTopicTransactionRepo, AgentProfileRepo, AgentRepo, AgentSessionRepo,
    SqliteDb,
};
use forge_agent_host::{
    AgentTurnRequest, CanonicalScope, CanonicalScopeType, NativeProviderConfig, WorkspaceAccess,
};
use sqlx::Row;
use std::sync::Arc;

#[async_trait]
pub trait TopicRotator: Send + Sync {
    async fn rotate_pending(&self, chat_id: &str) -> Result<bool>;
}

pub struct TopicRotationCoordinator {
    db: Arc<SqliteDb>,
    embedded: Arc<EmbeddedAgentService>,
}
impl TopicRotationCoordinator {
    pub fn new(db: Arc<SqliteDb>, embedded: Arc<EmbeddedAgentService>) -> Self {
        Self { db, embedded }
    }
}

#[async_trait]
impl TopicRotator for TopicRotationCoordinator {
    async fn rotate_pending(&self, chat_id: &str) -> Result<bool> {
        let owner = db::new_uuid_v4();
        let lease_until = (chrono::Utc::now() + chrono::Duration::seconds(30)).to_rfc3339();
        let row = sqlx::query("UPDATE agent_chat_topic_rotation SET owner_token = ?, lease_until = ? WHERE chat_id = ? AND (owner_token IS NULL OR julianday(lease_until) <= julianday(?)) AND NOT EXISTS (SELECT 1 FROM agent_chat_turn_job WHERE chat_id = ? AND status = 'leased') AND NOT EXISTS (SELECT 1 FROM agent_chat_turn_job WHERE id = origin_turn_id AND status IN ('queued','leased','retry_wait','awaiting_input')) RETURNING *")
            .bind(&owner).bind(lease_until).bind(chat_id).bind(db::now_rfc3339()).bind(chat_id)
            .fetch_optional(self.db.pool()).await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let stop = tokio_util::sync::CancellationToken::new();
        let _guard = stop.clone().drop_guard();
        let renew_db = self.db.clone();
        let renew_owner = owner.clone();
        let renew_stop = stop.clone();
        let heartbeat = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = renew_stop.cancelled() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                        let until = (chrono::Utc::now() + chrono::Duration::seconds(30)).to_rfc3339();
                        if let Err(error) = sqlx::query("UPDATE agent_chat_topic_rotation SET lease_until = ? WHERE owner_token = ?")
                            .bind(until).bind(&renew_owner).execute(renew_db.pool()).await {
                            tracing::warn!(%error, "topic rotation lease renewal failed");
                            break;
                        }
                    }
                }
            }
        });
        let result = self.complete(chat_id, &owner, row).await;
        stop.cancel();
        let _ = heartbeat.await;
        sqlx::query("UPDATE agent_chat_topic_rotation SET owner_token = NULL, lease_until = NULL WHERE chat_id = ? AND owner_token = ?")
            .bind(chat_id).bind(owner).execute(self.db.pool()).await?;
        result
    }
}

impl TopicRotationCoordinator {
    async fn complete(
        &self,
        chat_id: &str,
        owner: &str,
        row: sqlx::sqlite::SqliteRow,
    ) -> Result<bool> {
        let intent: String = row.try_get("id")?;
        let label: String = row.try_get("label")?;
        let summary: Option<String> = row.try_get("requested_summary")?;
        let now: String = row.try_get("created_at")?;
        let chat = self
            .db
            .get_agent_chat(chat_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_chat", chat_id))?;
        let source_id: Option<String> = row.try_get("source_session_id")?;
        let source_id = if let Some(id) = source_id {
            id
        } else {
            let existing: Option<String> = sqlx::query_scalar("SELECT s.id FROM agent_session s JOIN agent_context_scope c ON c.id = s.context_scope_id WHERE c.scope_type = 'agent_chat' AND c.scope_id = ? AND s.status IN ('starting','ready','running','degraded') ORDER BY s.created_at DESC LIMIT 1")
                .bind(chat_id).fetch_optional(self.db.pool()).await?;
            if let Some(id) = existing {
                id
            } else {
                let binding: Option<(String, String)> = if let Some(account) = &chat.account_id {
                    sqlx::query_as("SELECT identity_id, profile_id FROM account_main_agent_binding WHERE account_id = ? AND state = 'active'")
                        .bind(account).fetch_optional(self.db.pool()).await?
                } else {
                    sqlx::query_as("SELECT identity_id, profile_id FROM project_agent_binding WHERE project_id = ? AND state = 'active'")
                        .bind(&chat.project_id).fetch_optional(self.db.pool()).await?
                };
                let (identity_id, profile_id) = binding.ok_or_else(|| {
                    ServiceError::invalid_operation("topic rotation requires an Agent binding")
                })?;
                let identity = AgentRepo::get_by_id(&*self.db, &identity_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("agent", &identity_id))?;
                self.embedded
                    .create_or_resume_session(crate::embedded_agent_service::CreateScopedSession {
                        actor_user_id: identity.owner_id.ok_or_else(|| {
                            ServiceError::invalid_operation("topic Agent has no owner")
                        })?,
                        identity_id,
                        profile_id: Some(profile_id),
                        scope: crate::embedded_agent_service::RequestedCanonicalScope::AgentChat {
                            chat_id: chat_id.to_owned(),
                        },
                    })
                    .await?
                    .id
            }
        };
        let source = self
            .db
            .get_agent_session(&source_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_session", &source_id))?;
        let profile = self
            .db
            .get_profile(&source.profile_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_profile", &source.profile_id))?;
        let successor_forge: String = row.try_get("successor_session_id")?;
        let successor_runtime: String = row.try_get("successor_runtime_id")?;
        if profile.backend_kind == "native" {
            let mut tx = db::begin_immediate(self.db.pool()).await?;
            sqlx::query("UPDATE agent_chat_topic_rotation SET source_session_id = ? WHERE id = ? AND owner_token = ?")
                .bind(&source_id).bind(&intent).bind(owner).execute(&mut *tx).await?;
            sqlx::query("INSERT OR IGNORE INTO agent_session (id, identity_id, profile_id, context_scope_id, backend_kind, runtime_session_id, status, capabilities_json, connection_status, predecessor_session_id, created_at, updated_at) SELECT ?, identity_id, profile_id, context_scope_id, backend_kind, ?, 'suspended', capabilities_json, connection_status, id, ?, ? FROM agent_session WHERE id = ?")
                .bind(&successor_forge).bind(&successor_runtime).bind(&now).bind(&now).bind(&source_id).execute(&mut *tx).await?;
            tx.commit().await?;
            let config: crate::agent_chat_turn_worker::NativeProfileConfig =
                serde_json::from_str(&profile.config_json).map_err(|_| {
                    ServiceError::invalid_operation("topic rotation Profile config is invalid")
                })?;
            let identity = AgentRepo::get_by_id(&*self.db, &source.identity_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("agent", &source.identity_id))?;
            let provider = profile
                .provider
                .ok_or_else(|| ServiceError::invalid_operation("topic Profile has no provider"))?;
            let (context_tokens, max_input_tokens, max_output_tokens) =
                crate::embedded_agent_service::effective_native_limits(
                    &provider,
                    config.context_tokens,
                    config.max_input_tokens,
                    config.max_output_tokens,
                );
            let scope_row = sqlx::query(
                "SELECT workspace_access, workspace_path FROM agent_context_scope WHERE id = ?",
            )
            .bind(&source.context_scope_id)
            .fetch_one(self.db.pool())
            .await?;
            let workspace_path: Option<String> = scope_row.try_get("workspace_path")?;
            let access: String = scope_row.try_get("workspace_access")?;
            let workspace_access = match access.as_str() {
                "account_scratch" => WorkspaceAccess::AccountScratch,
                "project_verify" => WorkspaceAccess::ProjectVerify,
                _ => WorkspaceAccess::Deny,
            };
            let history = sqlx::query("SELECT author_type, content FROM agent_chat_message WHERE chat_id = ? AND status = 'complete' AND sequence >= COALESCE((SELECT MAX(starting_message_sequence) FROM agent_chat_topic WHERE chat_id = ?), 0) ORDER BY sequence ASC")
                .bind(chat_id).bind(chat_id).fetch_all(self.db.pool()).await?.into_iter().filter_map(|r| {
                    let role: String = r.get("author_type");
                    match role.as_str() {
                        "user" => Some(forge_agent_host::Message::text(forge_agent_host::Role::User, r.get::<String, _>("content"))),
                        "agent" => Some(forge_agent_host::Message::text(forge_agent_host::Role::Assistant, r.get::<String, _>("content"))),
                        _ => None,
                    }
                }).collect();
            let provider_account_id = match profile.credential_ref.as_deref() {
                Some(id) => db::CredentialHandleRepo::get_credential_handle(&*self.db, id)
                    .await?
                    .as_ref()
                    .and_then(crate::embedded_agent_service::entry_provider_account_id),
                None => None,
            };
            self.embedded
                .native_backend()
                .fork_topic(
                    AgentTurnRequest {
                        forge_session_id: source.id,
                        runtime_session_id: source.runtime_session_id.ok_or_else(|| {
                            ServiceError::invalid_operation("topic source has no runtime id")
                        })?,
                        scope: CanonicalScope {
                            scope_type: CanonicalScopeType::AgentChat,
                            scope_id: chat_id.to_owned(),
                            workspace_access,
                        },
                        workspace_path,
                        provider: NativeProviderConfig {
                            provider,
                            base_url: config.base_url,
                            model: profile.model.ok_or_else(|| {
                                ServiceError::invalid_operation("topic Profile has no model")
                            })?,
                            reasoning_effort: profile.reasoning_effort,
                            credential_handle_id: profile.credential_ref.ok_or_else(|| {
                                ServiceError::invalid_operation("topic Profile has no credential")
                            })?,
                            owner_user_id: identity.owner_id.ok_or_else(|| {
                                ServiceError::invalid_operation("topic Agent has no owner")
                            })?,
                            provider_account_id,
                            context_tokens,
                            max_input_tokens,
                            max_output_tokens,
                        },
                        system_prompt: None,
                        history,
                        input: String::new(),
                        server_state_card: Some(String::new()),
                        command_allowlist: Some(
                            self.embedded
                                .effective_command_allowlist(chat.project_id.as_deref())
                                .await,
                        ),
                        environment: Default::default(),
                        cancellation: tokio_util::sync::CancellationToken::new(),
                    },
                    &intent,
                    &successor_runtime,
                )
                .await
                .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
        }
        let result = self
            .db
            .rotate_agent_chat_topic(db::RotateAgentChatTopic {
                runtime_session_id: (profile.backend_kind == "native").then_some(successor_runtime),
                rotation_owner: Some(owner.to_owned()),
                topic: db::CreateAgentChatTopic {
                    id: intent.clone(),
                    chat_id: chat_id.to_owned(),
                    label: label.clone(),
                    summary,
                    principal_type: "system".to_owned(),
                    principal_id: None,
                    created_at: now.clone(),
                },
                divider_message: db::topic_divider_message(
                    format!("topic-divider:{intent}"),
                    chat_id.to_owned(),
                    &label,
                    format!("topic-rotation:{intent}"),
                    now,
                ),
            })
            .await?;
        Ok(result.is_ok())
    }
}
