//! Durable topic rotation at the idle boundary before turn admission.
//!
//! A rotation intent (`agent_chat_topic_rotation`) is executed off the turn
//! claim path, by the Agent Chat worker's rotation pass or synchronously by
//! the REST route when no turn is live. Each execution is one leased
//! attempt. A failed attempt backs off; the third failure abandons the
//! intent: the chat keeps its current topic and session, a visible notice and
//! an `agent_chat.topic.rotation_failed` event record why, and queued turns
//! are admitted again. A chat therefore never waits on a rotation that
//! cannot complete.
use crate::{EmbeddedAgentService, Result, ServiceError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use db::{
    AgentChatRepo, AgentChatTopicTransactionRepo, AgentProfileRepo, AgentRepo, AgentSessionRepo,
    SqliteDb,
};
use forge_agent_host::{
    AgentHostError, AgentTurnRequest, CanonicalScope, CanonicalScopeType, NativeProviderConfig,
    WorkspaceAccess,
};
use sqlx::Row;
use std::sync::Arc;

/// Attempts one rotation intent gets before it is abandoned.
pub const MAX_TOPIC_ROTATION_ATTEMPTS: i64 = 3;
/// Back-off before the second and third attempts.
const ROTATION_BACKOFF_SECONDS: [i64; 2] = [5, 30];
const ROTATION_LEASE_SECONDS: i64 = 30;
/// Intents examined per worker pass.
const ROTATION_PASS_LIMIT: i64 = 8;

/// SQL predicate: the chat's responder (account Main binding or Project
/// binding -> the identity's selected Profile) runs on the native backend.
/// The same rule gates the automatic rotation triggers in
/// `V202610050233__topic_working_sets.sql`.
pub(crate) const NATIVE_RESPONDER_SQL: &str = "SELECT EXISTS (
    SELECT 1 FROM agent_chat c
    JOIN agent_identity i ON i.id = COALESCE(
        (SELECT b.identity_id FROM account_main_agent_binding b
         WHERE b.account_id = c.account_id AND b.state = 'active'),
        (SELECT b.identity_id FROM project_agent_binding b
         WHERE b.project_id = c.project_id AND b.state = 'active'))
    JOIN agent_profile p ON p.id = i.selected_profile_id
    WHERE c.id = ? AND p.backend_kind = 'native')";

pub(crate) async fn responder_is_native(db: &SqliteDb, chat_id: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(NATIVE_RESPONDER_SQL)
        .bind(chat_id)
        .fetch_one(db.pool())
        .await?)
}

#[async_trait]
pub trait TopicRotator: Send + Sync {
    /// One attempt at the chat's pending rotation intent, if it is due.
    /// `Ok(true)` when the new topic committed; `Ok(false)` when nothing was
    /// done (no intent, not due, a live turn, or the intent was abandoned).
    async fn rotate_pending(&self, chat_id: &str) -> Result<bool> {
        self.rotate_pending_at(chat_id, Utc::now()).await
    }

    /// [`Self::rotate_pending`] at an explicit clock instant (back-off due
    /// times are compared and computed against it).
    async fn rotate_pending_at(&self, chat_id: &str, now: DateTime<Utc>) -> Result<bool>;

    /// Whether the chat's responder is native, so its topics are runtime
    /// topics rotated through an intent.
    async fn is_native_chat(&self, chat_id: &str) -> Result<bool>;
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

/// Chats with a due, unleased intent that no live turn blocks, fresh
/// intents first so a repeatedly failing one cannot starve newer ones.
pub(crate) async fn due_topic_rotations(db: &SqliteDb, now: DateTime<Utc>) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT r.chat_id FROM agent_chat_topic_rotation r
         WHERE (r.owner_token IS NULL OR julianday(r.lease_until) <= julianday(?1))
           AND (r.next_attempt_at IS NULL OR julianday(r.next_attempt_at) <= julianday(?1))
           AND NOT EXISTS (SELECT 1 FROM agent_chat_turn_job j
                           WHERE j.chat_id = r.chat_id AND j.status = 'leased')
           AND NOT EXISTS (SELECT 1 FROM agent_chat_turn_job j
                           WHERE j.id = r.origin_turn_id
                             AND j.status IN ('queued', 'leased', 'retry_wait', 'awaiting_input'))
         ORDER BY r.attempt_count ASC, r.created_at ASC
         LIMIT ?2",
    )
    .bind(now.to_rfc3339())
    .bind(ROTATION_PASS_LIMIT)
    .fetch_all(db.pool())
    .await?)
}

/// One bounded worker pass, off the turn-claim path: attempt every due
/// rotation concurrently, then drain rotation-summary usage the rotations
/// could not settle themselves. Returns the number of committed rotations.
pub(crate) async fn run_topic_rotation_pass(
    db: &SqliteDb,
    runner: &Arc<dyn crate::agent_chat_turn_worker::AgentChatTurnRunner>,
    now: DateTime<Utc>,
) -> Result<usize> {
    let mut attempts = tokio::task::JoinSet::new();
    for chat_id in due_topic_rotations(db, now).await? {
        let runner = Arc::clone(runner);
        attempts.spawn(async move {
            let result = runner.rotate_pending_topic(&chat_id, now).await;
            (chat_id, result)
        });
    }
    let mut rotated = 0;
    while let Some(joined) = attempts.join_next().await {
        match joined {
            Ok((_, Ok(true))) => rotated += 1,
            Ok((_, Ok(false))) => {}
            Ok((chat_id, Err(error))) => {
                tracing::warn!(%chat_id, %error, "topic rotation attempt failed");
            }
            Err(error) => tracing::warn!(%error, "topic rotation attempt stopped unexpectedly"),
        }
    }
    crate::chat_usage::settle_unsettled_topic_summary_usage(db).await?;
    Ok(rotated)
}

/// Why an attempt failed, as a stable, redaction-safe kind.
struct RotationFailure {
    kind: &'static str,
    error: ServiceError,
}

impl From<ServiceError> for RotationFailure {
    fn from(error: ServiceError) -> Self {
        let kind = match &error {
            ServiceError::NotFound { .. } => "not_found",
            ServiceError::Db(_) => "persistence",
            ServiceError::InvalidOperation { .. } => "invalid_operation",
            _ => "unknown",
        };
        Self { kind, error }
    }
}

impl From<db::DbError> for RotationFailure {
    fn from(error: db::DbError) -> Self {
        ServiceError::from(error).into()
    }
}

impl From<sqlx::Error> for RotationFailure {
    fn from(error: sqlx::Error) -> Self {
        ServiceError::from(error).into()
    }
}

fn host_failure(error: AgentHostError) -> RotationFailure {
    let kind = match &error {
        AgentHostError::Configuration(_) => "configuration",
        AgentHostError::Authority(_) => "authority",
        AgentHostError::AgentPaused { .. } | AgentHostError::ProjectPaused { .. } => "paused",
        AgentHostError::SessionNotFound => "session_not_found",
        AgentHostError::CredentialNotFound => "credential_not_found",
        AgentHostError::VersionConflict => "conflict",
        AgentHostError::ProtectedPersistence => "persistence",
        AgentHostError::Runtime(_)
        | AgentHostError::RuntimeWithUsage { .. }
        | AgentHostError::TurnLimitReached { .. } => "runtime",
        _ => "unknown",
    };
    RotationFailure {
        kind,
        error: ServiceError::invalid_operation(error.to_string()),
    }
}

#[async_trait]
impl TopicRotator for TopicRotationCoordinator {
    async fn is_native_chat(&self, chat_id: &str) -> Result<bool> {
        responder_is_native(&self.db, chat_id).await
    }

    async fn rotate_pending_at(&self, chat_id: &str, now: DateTime<Utc>) -> Result<bool> {
        let owner = db::new_uuid_v4();
        let lease_until = (now + chrono::Duration::seconds(ROTATION_LEASE_SECONDS)).to_rfc3339();
        // Leasing an attempt counts it; a crash mid-attempt still counts.
        let row = sqlx::query(
            "UPDATE agent_chat_topic_rotation
             SET owner_token = ?1, lease_until = ?2, attempt_count = attempt_count + 1
             WHERE chat_id = ?3
               AND (owner_token IS NULL OR julianday(lease_until) <= julianday(?4))
               AND (next_attempt_at IS NULL OR julianday(next_attempt_at) <= julianday(?4))
               AND NOT EXISTS (SELECT 1 FROM agent_chat_turn_job
                               WHERE chat_id = ?3 AND status = 'leased')
               AND NOT EXISTS (SELECT 1 FROM agent_chat_turn_job
                               WHERE id = origin_turn_id
                                 AND status IN ('queued', 'leased', 'retry_wait', 'awaiting_input'))
             RETURNING *",
        )
        .bind(&owner)
        .bind(lease_until)
        .bind(chat_id)
        .bind(now.to_rfc3339())
        .fetch_optional(self.db.pool())
        .await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let intent: String = row.try_get("id")?;
        let attempts: i64 = row.try_get("attempt_count")?;
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
                        let until = (Utc::now() + chrono::Duration::seconds(ROTATION_LEASE_SECONDS)).to_rfc3339();
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
        match result {
            // The committing transaction deleted the intent.
            Ok(true) => {
                if let Err(error) = crate::chat_usage::settle_topic_summary_usage(
                    &self.db,
                    &forge_agent_host::topic_summary_usage_id(&intent),
                )
                .await
                {
                    tracing::warn!(%chat_id, %error, "topic summary usage settlement deferred");
                }
                Ok(true)
            }
            // Nothing was attempted (a turn was admitted meanwhile): refund.
            Ok(false) => {
                sqlx::query(
                    "UPDATE agent_chat_topic_rotation
                     SET owner_token = NULL, lease_until = NULL,
                         attempt_count = MAX(attempt_count - 1, 0)
                     WHERE chat_id = ? AND owner_token = ?",
                )
                .bind(chat_id)
                .bind(&owner)
                .execute(self.db.pool())
                .await?;
                Ok(false)
            }
            Err(failure) if attempts >= MAX_TOPIC_ROTATION_ATTEMPTS => {
                tracing::warn!(%chat_id, intent_id = %intent, kind = failure.kind, error = %failure.error, attempts, "topic rotation abandoned");
                self.abandon(chat_id, &intent, &owner, &failure, attempts, now)
                    .await?;
                Err(failure.error)
            }
            Err(failure) => {
                let index = usize::try_from(attempts - 1)
                    .unwrap_or(0)
                    .min(ROTATION_BACKOFF_SECONDS.len() - 1);
                let next = now + chrono::Duration::seconds(ROTATION_BACKOFF_SECONDS[index]);
                tracing::warn!(%chat_id, intent_id = %intent, kind = failure.kind, error = %failure.error, attempts, "topic rotation attempt failed; backing off");
                sqlx::query(
                    "UPDATE agent_chat_topic_rotation
                     SET owner_token = NULL, lease_until = NULL,
                         next_attempt_at = ?, last_error_kind = ?
                     WHERE chat_id = ? AND owner_token = ?",
                )
                .bind(next.to_rfc3339())
                .bind(failure.kind)
                .bind(chat_id)
                .bind(&owner)
                .execute(self.db.pool())
                .await?;
                Err(failure.error)
            }
        }
    }
}

impl TopicRotationCoordinator {
    async fn complete(
        &self,
        chat_id: &str,
        owner: &str,
        row: sqlx::sqlite::SqliteRow,
    ) -> std::result::Result<bool, RotationFailure> {
        let intent: String = row.try_get("id")?;
        let label: String = row.try_get("label")?;
        let summary: Option<String> = row.try_get("requested_summary")?;
        let cause: String = row.try_get("cause")?;
        let now: String = row.try_get("created_at")?;
        let chat = self
            .db
            .get_agent_chat(chat_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_chat", chat_id))?;
        let native = responder_is_native(&self.db, chat_id).await?;
        if !native && cause != "rest" {
            // Automatic rotations exist only for native chats; a CLI chat
            // keeps its conversation exactly as before.
            sqlx::query(
                "DELETE FROM agent_chat_topic_rotation WHERE chat_id = ? AND owner_token = ?",
            )
            .bind(chat_id)
            .bind(owner)
            .execute(self.db.pool())
            .await?;
            return Ok(false);
        }
        let successor_runtime: String = row.try_get("successor_runtime_id")?;
        if native {
            self.fork_native_topic(chat_id, owner, &chat, &row, &intent)
                .await?;
        }
        let result = self
            .db
            .rotate_agent_chat_topic(db::RotateAgentChatTopic {
                runtime_session_id: native.then_some(successor_runtime),
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

    /// Forks the source runtime session onto the reserved successor with a
    /// summary seed and a new timeline. Replays are exactly-once: the seed is
    /// sealed before the fork and the runtime fork is idempotent.
    async fn fork_native_topic(
        &self,
        chat_id: &str,
        owner: &str,
        chat: &db::AgentChat,
        row: &sqlx::sqlite::SqliteRow,
        intent: &str,
    ) -> std::result::Result<(), RotationFailure> {
        let request = self.fork_request(chat_id, owner, chat, row, intent).await?;
        let successor_runtime: String = row.try_get("successor_runtime_id")?;
        self.embedded
            .native_backend()
            .fork_topic(request, intent, &successor_runtime)
            .await
            .map_err(host_failure)
    }

    /// The source session's turn request for this intent, reserving the
    /// successor Forge session (suspended) on first use.
    async fn fork_request(
        &self,
        chat_id: &str,
        owner: &str,
        chat: &db::AgentChat,
        row: &sqlx::sqlite::SqliteRow,
        intent: &str,
    ) -> std::result::Result<AgentTurnRequest, RotationFailure> {
        let now: String = row.try_get("created_at")?;
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
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        sqlx::query("UPDATE agent_chat_topic_rotation SET source_session_id = ? WHERE id = ? AND owner_token = ?")
            .bind(&source_id).bind(intent).bind(owner).execute(&mut *tx).await?;
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
        Ok(AgentTurnRequest {
            forge_session_id: source.id,
            runtime_session_id: source
                .runtime_session_id
                .ok_or_else(|| ServiceError::invalid_operation("topic source has no runtime id"))?,
            scope: CanonicalScope {
                scope_type: CanonicalScopeType::AgentChat,
                scope_id: chat_id.to_owned(),
                workspace_access,
            },
            workspace_path,
            provider: NativeProviderConfig {
                provider,
                base_url: config.base_url,
                model: profile
                    .model
                    .ok_or_else(|| ServiceError::invalid_operation("topic Profile has no model"))?,
                reasoning_effort: profile.reasoning_effort,
                credential_handle_id: profile.credential_ref.ok_or_else(|| {
                    ServiceError::invalid_operation("topic Profile has no credential")
                })?,
                owner_user_id: identity
                    .owner_id
                    .ok_or_else(|| ServiceError::invalid_operation("topic Agent has no owner"))?,
                provider_account_id,
                context_tokens,
                max_input_tokens,
                max_output_tokens,
            },
            system_prompt: None,
            history,
            input: String::new(),
            // Same component set as the chat's turns; the fork plans
            // no provider request, so the empty card is never sent.
            server_state_card: Some(String::new()),
            command_allowlist: Some(
                self.embedded
                    .effective_command_allowlist(chat.project_id.as_deref())
                    .await,
            ),
            environment: Default::default(),
            cancellation: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// Abandons an intent whose attempts are exhausted. A fork that never
    /// saved its successor leaves the source session usable (any pending
    /// fork intent is aborted), so the chat stays on its current topic and
    /// session. A fork that did save its successor has superseded the
    /// source, so the chat continues on the successor: only the topic record
    /// is missing.
    async fn abandon(
        &self,
        chat_id: &str,
        intent: &str,
        owner: &str,
        failure: &RotationFailure,
        attempts: i64,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let row =
            sqlx::query("SELECT * FROM agent_chat_topic_rotation WHERE id = ? AND owner_token = ?")
                .bind(intent)
                .bind(owner)
                .fetch_optional(self.db.pool())
                .await?;
        let Some(row) = row else {
            return Ok(());
        };
        let label: String = row.try_get("label")?;
        let successor_forge: String = row.try_get("successor_session_id")?;
        let hand_over: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM protected_agent_session_state
                           WHERE session_id = ?
                             AND (snapshot_ciphertext IS NOT NULL
                                  OR checkpoint_ciphertext IS NOT NULL))",
        )
        .bind(&successor_forge)
        .fetch_one(self.db.pool())
        .await?;
        if !hand_over && responder_is_native(&self.db, chat_id).await? {
            let aborted = match self.db.get_agent_chat(chat_id).await? {
                Some(chat) => match self.fork_request(chat_id, owner, &chat, &row, intent).await {
                    Ok(request) => self
                        .embedded
                        .native_backend()
                        .abort_topic_fork(request)
                        .await
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(error.error.to_string()),
                },
                None => Ok(()),
            };
            if let Err(error) = aborted {
                tracing::warn!(%chat_id, intent_id = %intent, %error, "could not abort the abandoned topic fork");
            }
        }
        let mut notice = db::topic_divider_message(
            format!("topic-rotation-failed:{intent}"),
            chat_id.to_owned(),
            &label,
            format!("topic-rotation:{intent}"),
            now.to_rfc3339(),
        );
        notice.content = if hand_over {
            db::topic_rotation_unrecorded_message_body(&label)
        } else {
            db::topic_rotation_failed_message_body(&label)
        };
        notice.outcome = Some("topic_rotation_failed".to_owned());
        self.db
            .abandon_agent_chat_topic_rotation(db::AbandonAgentChatTopicRotation {
                chat_id: chat_id.to_owned(),
                intent_id: intent.to_owned(),
                owner_token: owner.to_owned(),
                error_kind: failure.kind.to_owned(),
                attempts,
                hand_over,
                notice_message: notice,
            })
            .await?;
        Ok(())
    }
}
