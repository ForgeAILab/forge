//! Binding, admission, and handoff policy for singular Agent Chats.
//!
//! The implementation is generic over the DB adapter so the service can be
//! exercised with a repository fake while the SQLite adapter is being rolled
//! out. The repository methods are the authority boundary; callers never
//! select a responder by putting an identity in message content.

use std::sync::Arc;

use db::{
    new_uuid_v4, now_rfc3339, AccountMainAgentBinding, AccountMainAgentBindingRepo,
    AdmitAgentHandoff, Agent, AgentChat, AgentChatMessage, AgentChatMessageAuthorType,
    AgentChatMessageRepo, AgentChatMessageStatus, AgentChatRepo, AgentChatTransactionRepo,
    AgentChatTurnJob, AgentChatTurnJobRepo, AgentChatTurnState, AgentHandoff, AgentHandoffRepo,
    AgentRepo, CancelAgentChatTurn, CompleteAgentChatTurn, CreateAccountMainAgentBinding,
    CreateAgentChat, CreateAgentChatMessage, CreateAgentChatTurnJob, CreateAgentHandoff,
    CreateProjectAgentBinding, FailAgentChatTurn, ProjectAgentBinding, ProjectAgentBindingRepo,
    ProjectMemberRepo, ReplaceAccountMainAgentBinding, ReplaceProjectAgentBinding, UpdateAgentChat,
};
use serde_json::json;

use crate::{
    agent_chat_policy::{guard_agent_chat_content, AgentChatOperation, AgentChatScope},
    agent_chat_turn_policy::{bounded_error, failure_after_claim},
    agent_turn_admission::{
        content_digest, handoff_content_digest_with_sources, AgentResponderStore,
        AgentTurnAdmissionService, AgentTurnAdmitInput, AgentTurnPrepareInput, AgentTurnTrigger,
    },
    Result, ServiceError,
};

const MAIN_CHAT_KIND: &str = "account_main";
const PROJECT_CHAT_KIND: &str = "project";
const READY_CHAT_STATUS: &str = "ready";
const ACTIVE_BINDING_STATE: &str = "active";
const DEFAULT_MAX_ATTEMPTS: i64 = 3;
const MAX_HANDOFF_CONTENT_CHARS: usize = 16_384;
const MAX_TURN_CANCELLATION_KEY_CHARS: usize = 256;

#[derive(Debug, Clone)]
pub struct SetMainAgentBindingInput {
    pub actor_user_id: String,
    pub account_id: String,
    pub identity_id: String,
    pub autonomy_policy_json: String,
    pub tool_policy_revision: String,
    pub expected_version: Option<i64>,
    pub replacement_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SetProjectAgentBindingInput {
    pub actor_user_id: String,
    pub project_id: String,
    pub identity_id: Option<String>,
    pub state: String,
    pub autonomy_policy_json: String,
    pub permission_ceiling_json: String,
    pub subscriptions_json: String,
    pub wake_budget: i64,
    pub expected_version: Option<i64>,
    pub replacement_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SendAgentChatMessageInput {
    pub actor_user_id: String,
    pub chat_id: String,
    pub content: String,
    pub dedupe_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CancelAgentChatTurnInput {
    pub actor_user_id: String,
    pub chat_id: String,
    pub turn_job_id: String,
    pub expected_version: i64,
    pub idempotency_key: String,
}

#[derive(Debug, Clone)]
pub struct CreateAgentHandoffInput {
    pub actor_user_id: String,
    pub source_chat_id: String,
    pub source_message_id: Option<String>,
    pub source_turn_job_id: Option<String>,
    pub target_project_id: String,
    pub content: String,
    pub source_revisions_json: String,
    pub dedupe_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmittedAgentChatMessage {
    pub message: AgentChatMessage,
    pub turn_job: AgentChatTurnJob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedAgentChatResponse {
    pub message: AgentChatMessage,
    pub turn_job: AgentChatTurnJob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendAgentChatSuccessInput {
    pub content: String,
    pub model: Option<String>,
    pub session_id: Option<String>,
    pub context_manifest_id: Option<String>,
    pub token_usage_json: Option<String>,
    pub duration_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentChatHandoffOutcome {
    pub handoff: AgentHandoff,
    pub target_message: AgentChatMessage,
    pub target_turn_job: AgentChatTurnJob,
}

#[derive(Clone)]
pub struct AgentChatService<D> {
    db: Arc<D>,
}

impl<D> std::fmt::Debug for AgentChatService<D> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentChatService")
            .finish_non_exhaustive()
    }
}

impl<D> AgentChatService<D> {
    pub fn new(db: Arc<D>) -> Self {
        Self { db }
    }
}

impl<D> AgentChatService<D>
where
    D: AccountMainAgentBindingRepo
        + ProjectAgentBindingRepo
        + AgentChatRepo
        + AgentChatMessageRepo
        + AgentChatTurnJobRepo
        + AgentHandoffRepo
        + AgentChatTransactionRepo
        + AgentRepo
        + AgentResponderStore
        + ProjectMemberRepo,
{
    pub async fn get_authorized_chat(
        &self,
        actor_user_id: &str,
        chat_id: &str,
    ) -> Result<AgentChat> {
        // Main Chat creation is idempotent and deliberately precedes lookup:
        // accounts created after the backfill still receive their canonical
        // timeline even before an Agent binding has been selected.
        self.ensure_main_chat(actor_user_id).await?;
        let chat = AgentChatRepo::get_agent_chat(&*self.db, chat_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_chat", chat_id.to_owned()))?;
        self.authorize_chat_scope(actor_user_id, &chat).await?;
        Ok(chat)
    }

    pub async fn list_authorized_chats(&self, actor_user_id: &str) -> Result<Vec<AgentChat>> {
        self.ensure_main_chat(actor_user_id).await?;
        Ok(AgentChatRepo::list_agent_chats(&*self.db, actor_user_id).await?)
    }

    /// Return the one account Main Chat, creating its setup-required row when
    /// this account predates the migration (or was created concurrently).
    /// The partial unique index is the race arbiter: a losing creator rereads
    /// the winner instead of exposing a duplicate or a transient 409.
    pub async fn ensure_main_chat(&self, account_id: &str) -> Result<AgentChat> {
        if let Some(chat) = AgentChatRepo::get_main_chat(&*self.db, account_id).await? {
            return Ok(chat);
        }
        let now = now_rfc3339();
        let status = if AccountMainAgentBindingRepo::get_active_main_binding(&*self.db, account_id)
            .await?
            .is_some()
        {
            READY_CHAT_STATUS
        } else {
            "agent_setup_required"
        };
        let input = CreateAgentChat {
            id: new_uuid_v4(),
            kind: MAIN_CHAT_KIND.to_owned(),
            account_id: Some(account_id.to_owned()),
            project_id: None,
            status: status.to_owned(),
            instruction_revision: 0,
            created_at: now.clone(),
            updated_at: now,
        };
        match AgentChatRepo::create_agent_chat(&*self.db, input).await {
            Ok(chat) => Ok(chat),
            Err(error) => {
                // SQLite maps the partial-unique race to a check error. If a
                // different write failed, preserve that actual error.
                if matches!(&error, db::DbError::Check(_)) {
                    AgentChatRepo::get_main_chat(&*self.db, account_id)
                        .await?
                        .ok_or(ServiceError::Db(error))
                } else {
                    Err(error.into())
                }
            }
        }
    }

    /// Return the one Project Chat, creating a setup-required row when a
    /// newly-created Project has not yet been bound to an Agent profile.
    pub async fn ensure_project_chat(&self, project_id: &str) -> Result<AgentChat> {
        if let Some(chat) = AgentChatRepo::get_project_chat(&*self.db, project_id).await? {
            return Ok(chat);
        }
        let now = now_rfc3339();
        let input = CreateAgentChat {
            id: new_uuid_v4(),
            kind: PROJECT_CHAT_KIND.to_owned(),
            account_id: None,
            project_id: Some(project_id.to_owned()),
            status: "agent_setup_required".to_owned(),
            instruction_revision: 0,
            created_at: now.clone(),
            updated_at: now,
        };
        match AgentChatRepo::create_agent_chat(&*self.db, input).await {
            Ok(chat) => Ok(chat),
            Err(error) => {
                if matches!(&error, db::DbError::Check(_)) {
                    AgentChatRepo::get_project_chat(&*self.db, project_id)
                        .await?
                        .ok_or(ServiceError::Db(error))
                } else {
                    Err(error.into())
                }
            }
        }
    }

    pub async fn set_main_binding(
        &self,
        input: SetMainAgentBindingInput,
    ) -> Result<AccountMainAgentBinding> {
        if input.actor_user_id != input.account_id {
            return Err(ServiceError::not_found("account", input.account_id));
        }
        // The binding names the agent; its profile column is a bind-time
        // snapshot of the agent's current settings, re-resolved on every turn.
        let identity = self
            .require_owned_identity(&input.actor_user_id, &input.identity_id)
            .await?;
        self.ensure_main_chat(&input.account_id).await?;
        let account_id = input.account_id.clone();
        let now = now_rfc3339();
        let replacement = CreateAccountMainAgentBinding {
            id: new_uuid_v4(),
            account_id: account_id.clone(),
            identity_id: input.identity_id,
            profile_id: identity.profile_id,
            autonomy_policy_json: input.autonomy_policy_json,
            tool_policy_revision: input.tool_policy_revision,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        let binding = match (
            AccountMainAgentBindingRepo::get_active_main_binding(&*self.db, &input.account_id)
                .await?,
            input.expected_version,
        ) {
            (Some(current), Some(expected)) if current.version == expected => {
                Ok(AccountMainAgentBindingRepo::replace_main_binding(
                    &*self.db,
                    ReplaceAccountMainAgentBinding {
                        account_id: account_id.clone(),
                        expected_version: expected,
                        replacement,
                        replacement_reason: input.replacement_reason,
                    },
                )
                .await?)
            }
            (Some(_current), Some(_)) => Err(ServiceError::Db(db::DbError::VersionConflict)),
            (Some(_), None) => Err(ServiceError::Conflict(
                "Main Agent binding already exists; expected_version is required for replacement"
                    .to_owned(),
            )),
            (None, None) => Ok(AccountMainAgentBindingRepo::create_main_binding(
                &*self.db,
                replacement,
            )
            .await?),
            (None, Some(_)) => Err(ServiceError::Db(db::DbError::VersionConflict)),
        }?;
        self.mark_main_chat_ready(&account_id).await?;
        Ok(binding)
    }

    pub async fn set_project_binding(
        &self,
        input: SetProjectAgentBindingInput,
    ) -> Result<ProjectAgentBinding> {
        self.require_project_member(&input.actor_user_id, &input.project_id)
            .await?;
        self.ensure_project_chat(&input.project_id).await?;
        let project_id = input.project_id.clone();
        let activate = input.state == ACTIVE_BINDING_STATE;
        // Snapshot of the bound agent's current settings; turns re-resolve
        // the agent's live profile rather than trusting this column.
        let mut profile_snapshot = None;
        if input.state == ACTIVE_BINDING_STATE {
            let Some(identity_id) = input.identity_id.as_deref() else {
                return Err(ServiceError::invalid_operation(
                    "active Project Agent binding requires an identity",
                ));
            };
            let identity = self
                .require_owned_identity(&input.actor_user_id, identity_id)
                .await?;
            profile_snapshot = Some(identity.profile_id);
        }
        let now = now_rfc3339();
        let replacement = CreateProjectAgentBinding {
            id: new_uuid_v4(),
            project_id: project_id.clone(),
            identity_id: input.identity_id,
            profile_id: profile_snapshot,
            state: input.state,
            autonomy_policy_json: input.autonomy_policy_json,
            permission_ceiling_json: input.permission_ceiling_json,
            subscriptions_json: input.subscriptions_json,
            wake_budget: input.wake_budget,
            created_at: now.clone(),
            updated_at: now,
        };
        let binding = match (
            ProjectAgentBindingRepo::get_active_project_binding(&*self.db, &input.project_id)
                .await?,
            input.expected_version,
        ) {
            (Some(current), Some(expected)) if current.version == expected => {
                Ok(ProjectAgentBindingRepo::replace_project_binding(
                    &*self.db,
                    ReplaceProjectAgentBinding {
                        project_id: project_id.clone(),
                        expected_version: expected,
                        replacement,
                        replacement_reason: input.replacement_reason,
                    },
                )
                .await?)
            }
            (Some(_), Some(_)) => Err(ServiceError::Db(db::DbError::VersionConflict)),
            (Some(_), None) => Err(ServiceError::Conflict(
                "Project Agent binding already exists; expected_version is required for replacement"
                    .to_owned(),
            )),
            (None, None) => Ok(ProjectAgentBindingRepo::create_project_binding(
                &*self.db,
                replacement,
            )
            .await?),
            (None, Some(_)) => Err(ServiceError::Db(db::DbError::VersionConflict)),
        }?;
        if activate {
            self.mark_project_chat_ready(&project_id).await?;
        }
        Ok(binding)
    }

    pub async fn send_message(
        &self,
        input: SendAgentChatMessageInput,
    ) -> Result<AdmittedAgentChatMessage> {
        let chat = self
            .get_authorized_chat(&input.actor_user_id, &input.chat_id)
            .await?;
        let guarded = guard_agent_chat_content(&input.content)?;
        let dedupe_key = input
            .dedupe_key
            .filter(|key| !key.trim().is_empty())
            .unwrap_or_else(new_uuid_v4);
        let trigger_content_digest = content_digest(&guarded.content)?;
        let now = now_rfc3339();
        let message_id = new_uuid_v4();
        let correlation_id = new_uuid_v4();
        let turn_id = new_uuid_v4();
        let turn = CreateAgentChatTurnJob {
            id: turn_id,
            chat_id: chat.id.clone(),
            triggering_message_id: message_id.clone(),
            // The shared admission service overwrites all authority fields
            // from its current binding/Profile snapshot.
            responder_identity_id: String::new(),
            profile_id: String::new(),
            responder_binding_id: None,
            responder_binding_version: None,
            responder_identity_version: None,
            profile_version: None,
            operating_skill_revision_id: None,
            policy_revision: None,
            policy_digest: None,
            permission_policy_digest: None,
            tool_policy_digest: None,
            admission_digest: None,
            canonical_scope_provenance_json: None,
            canonical_scope_type: String::new(),
            canonical_scope_id: String::new(),
            dedupe_key: String::new(),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            correlation_id: correlation_id.clone(),
            causation_id: None,
            causation_depth: 0,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        let admitted = AgentTurnAdmissionService::new(Arc::clone(&self.db))
            .admit(AgentTurnAdmitInput {
                preparation: AgentTurnPrepareInput {
                    chat: &chat,
                    trigger: AgentTurnTrigger::UserMessage,
                    dedupe_key: &dedupe_key,
                    content_digest: &trigger_content_digest,
                    causation_id: None,
                    causation_depth: 0,
                    source_responder: None,
                },
                message: CreateAgentChatMessage {
                    id: message_id.clone(),
                    chat_id: chat.id.clone(),
                    // The SQLite composite allocates the sequence while holding
                    // the transaction. Fakes may use this as their initial hint.
                    sequence: chat.message_count,
                    author_type: AgentChatMessageAuthorType::User,
                    author_id: Some(input.actor_user_id),
                    content: guarded.content,
                    content_guard_json: guarded.guard_json,
                    sensitivity: guarded.sensitivity,
                    status: AgentChatMessageStatus::Complete,
                    outcome: None,
                    model: None,
                    profile_id: None,
                    session_id: None,
                    context_manifest_id: None,
                    token_usage_json: None,
                    duration_ms: None,
                    error: None,
                    correlation_id: correlation_id.clone(),
                    causation_id: None,
                    handoff_id: None,
                    // `native` is the persisted source discriminator for a
                    // direct Agent Chat message (the schema deliberately
                    // reserves `handoff` for delivered handoff messages).
                    source_type: "native".to_owned(),
                    source_id: None,
                    source_message_id: None,
                    source_room_id: None,
                    source_conversation_id: None,
                    source_sequence: None,
                    source_metadata_json: "{}".to_owned(),
                    created_at: now.clone(),
                },
                turn,
            })
            .await?;
        Ok(AdmittedAgentChatMessage {
            message: admitted.message,
            turn_job: admitted.turn,
        })
    }

    /// Cancel a visible Agent Chat turn while retaining the worker's
    /// optimistic lease/version boundary.  The repository appends the
    /// cancellation event in the same transaction and uses its dedupe key to
    /// make an identical retry return the already-cancelled job.
    pub async fn cancel_turn(&self, input: CancelAgentChatTurnInput) -> Result<AgentChatTurnJob> {
        let chat = self
            .get_authorized_chat(&input.actor_user_id, &input.chat_id)
            .await?;
        let job = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, &input.turn_job_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_chat_turn", input.turn_job_id.clone()))?;
        if job.chat_id != chat.id {
            return Err(ServiceError::not_found(
                "agent_chat_turn",
                input.turn_job_id,
            ));
        }
        let idempotency_key = input.idempotency_key.trim();
        if idempotency_key.is_empty() {
            return Err(ServiceError::invalid_operation(
                "turn cancellation idempotency_key is required",
            ));
        }
        if idempotency_key.chars().count() > MAX_TURN_CANCELLATION_KEY_CHARS {
            return Err(ServiceError::invalid_operation(
                "turn cancellation idempotency_key exceeds the bounded limit",
            ));
        }
        if matches!(
            job.status,
            AgentChatTurnState::Succeeded | AgentChatTurnState::Failed
        ) {
            return Err(ServiceError::Conflict(
                "Agent Chat turn is already terminal".to_owned(),
            ));
        }
        AgentChatTransactionRepo::cancel_agent_chat_turn(
            &*self.db,
            CancelAgentChatTurn {
                turn_job_id: job.id,
                expected_version: input.expected_version,
                actor_user_id: input.actor_user_id,
                idempotency_key: idempotency_key.to_owned(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn append_success(
        &self,
        job: &AgentChatTurnJob,
        lease_owner: &str,
        response: AppendAgentChatSuccessInput,
    ) -> Result<CommittedAgentChatResponse> {
        if job.status != AgentChatTurnState::Leased
            || job.lease_owner.as_deref() != Some(lease_owner)
        {
            return Err(ServiceError::Conflict(
                "Agent Chat turn lease is no longer active".to_owned(),
            ));
        }
        let AppendAgentChatSuccessInput {
            content,
            model,
            session_id,
            context_manifest_id,
            token_usage_json,
            duration_ms,
        } = response;
        let guarded = guard_agent_chat_content(&content)?;
        let now = now_rfc3339();
        let message_id = new_uuid_v4();
        let completed = AgentChatTransactionRepo::complete_agent_chat_turn(
            &*self.db,
            CompleteAgentChatTurn {
                turn_job_id: job.id.clone(),
                expected_version: job.version,
                lease_owner: lease_owner.to_owned(),
                response: CreateAgentChatMessage {
                    id: message_id,
                    chat_id: job.chat_id.clone(),
                    // The SQLite composite allocates the response sequence in
                    // the same transaction as the terminal job update.
                    sequence: 0,
                    author_type: AgentChatMessageAuthorType::Agent,
                    author_id: job.responder_identity_id.clone(),
                    content: guarded.content,
                    content_guard_json: guarded.guard_json,
                    sensitivity: guarded.sensitivity,
                    status: AgentChatMessageStatus::Complete,
                    outcome: Some("completed".to_owned()),
                    model,
                    profile_id: job.profile_id.clone(),
                    session_id,
                    context_manifest_id,
                    token_usage_json,
                    duration_ms,
                    error: None,
                    correlation_id: job.correlation_id.clone(),
                    causation_id: job.causation_id.clone(),
                    handoff_id: None,
                    source_type: "native".to_owned(),
                    source_id: Some(job.id.clone()),
                    source_message_id: Some(job.triggering_message_id.clone()),
                    source_room_id: None,
                    source_conversation_id: None,
                    source_sequence: None,
                    source_metadata_json: "{}".to_owned(),
                    created_at: now.clone(),
                },
                updated_at: now,
            },
        )
        .await?;
        Ok(CommittedAgentChatResponse {
            message: completed.response,
            turn_job: completed.turn,
        })
    }

    pub async fn append_awaiting_input(
        &self,
        job: &AgentChatTurnJob,
        lease_owner: &str,
        pending_interaction_id: &str,
    ) -> Result<AgentChatTurnJob> {
        if job.status != AgentChatTurnState::Leased
            || job.lease_owner.as_deref() != Some(lease_owner)
        {
            return Err(ServiceError::Conflict(
                "Agent Chat turn lease is no longer active".to_owned(),
            ));
        }
        let now = now_rfc3339();
        AgentChatTransactionRepo::park_agent_chat_turn(
            &*self.db,
            db::ParkAgentChatTurn {
                turn_job_id: job.id.clone(),
                expected_version: job.version,
                lease_owner: lease_owner.to_owned(),
                pending_interaction_id: pending_interaction_id.to_owned(),
                updated_at: now,
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn append_failure(
        &self,
        job: &AgentChatTurnJob,
        lease_owner: &str,
        error_code: &str,
        error_message: &str,
    ) -> Result<AgentChatTurnJob> {
        if job.status != AgentChatTurnState::Leased
            || job.lease_owner.as_deref() != Some(lease_owner)
        {
            return Err(ServiceError::Conflict(
                "Agent Chat turn lease is no longer active".to_owned(),
            ));
        }
        let now = chrono::Utc::now();
        let decision = failure_after_claim(job.attempt_count, job.max_attempts, now, error_message);
        AgentChatTransactionRepo::fail_agent_chat_turn(
            &*self.db,
            FailAgentChatTurn {
                turn_job_id: job.id.clone(),
                expected_version: job.version,
                lease_owner: lease_owner.to_owned(),
                status: match decision.status {
                    api_types::AgentChatTurnStatus::RetryWait => AgentChatTurnState::RetryWait,
                    api_types::AgentChatTurnStatus::Failed => AgentChatTurnState::Failed,
                    _ => AgentChatTurnState::Failed,
                },
                attempt_count: decision.attempt_count,
                next_attempt_at: decision.next_attempt_at.map(|at| at.to_rfc3339()),
                error_code: bounded_error(error_code),
                error_message: decision.error,
                updated_at: now.to_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn create_handoff(
        &self,
        input: CreateAgentHandoffInput,
    ) -> Result<AgentChatHandoffOutcome> {
        let source = self
            .get_authorized_chat(&input.actor_user_id, &input.source_chat_id)
            .await?;
        if source.kind != MAIN_CHAT_KIND {
            return Err(ServiceError::invalid_operation(
                "only the Main Agent Chat can publish a Project handoff",
            ));
        }
        let source_account_id = source
            .account_id
            .as_deref()
            .ok_or_else(|| ServiceError::not_found("agent_chat", source.id.clone()))?;
        AgentChatScope::main(source_account_id)
            .authorize(
                AgentChatOperation::HandoffPublish,
                Some(&input.target_project_id),
            )
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let source_binding =
            AccountMainAgentBindingRepo::get_active_main_binding(&*self.db, source_account_id)
                .await?
                .filter(|binding| binding.state == ACTIVE_BINDING_STATE)
                .ok_or_else(|| {
                    ServiceError::Conflict("Main Agent binding is not configured".into())
                })?;
        let target = AgentChatRepo::get_project_chat(&*self.db, &input.target_project_id)
            .await?
            .ok_or_else(|| {
                ServiceError::not_found("agent_chat", input.target_project_id.clone())
            })?;
        if target.kind != PROJECT_CHAT_KIND {
            return Err(ServiceError::not_found("agent_chat", target.id.clone()));
        }
        let source_responder = self.responder_for_chat(&source).await?;
        if source_responder.identity_id()? != source_binding.identity_id {
            return Err(ServiceError::Conflict(
                "Main Agent binding changed while preparing the handoff".to_owned(),
            ));
        }
        let guarded = guard_agent_chat_content(&input.content)?;
        if guarded.content.chars().count() > MAX_HANDOFF_CONTENT_CHARS {
            return Err(ServiceError::invalid_operation(
                "handoff content exceeds the bounded publication limit",
            ));
        }
        let now = now_rfc3339();
        let correlation_id = new_uuid_v4();
        let handoff_id = new_uuid_v4();
        let target_message_id = new_uuid_v4();
        let target_turn_id = new_uuid_v4();
        let bounded_source_revisions = bounded_json(&input.source_revisions_json)?;
        let handoff_content_digest = handoff_content_digest_with_sources(
            &guarded.content,
            &bounded_source_revisions,
            input.source_message_id.as_deref(),
            input.source_turn_job_id.as_deref(),
        )?;
        let source_causation_depth =
            if let Some(source_turn_job_id) = input.source_turn_job_id.as_deref() {
                AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, source_turn_job_id)
                    .await?
                    .map(|job| job.causation_depth)
                    .unwrap_or(0)
            } else {
                0
            };
        let causation_depth = source_causation_depth.saturating_add(1);
        let target_dedupe_key = format!("handoff:{}", input.dedupe_key);
        let prepared = AgentTurnAdmissionService::new(Arc::clone(&self.db))
            .prepare(AgentTurnPrepareInput {
                chat: &target,
                trigger: AgentTurnTrigger::MainProjectHandoff,
                dedupe_key: &target_dedupe_key,
                content_digest: &handoff_content_digest,
                // `handoff_id` is a server-minted outcome identifier and is
                // regenerated on a response-loss retry.  The admission
                // digest must remain stable for the same caller/source
                // trigger; the persisted turn still records the concrete
                // handoff id as its causation edge below.
                causation_id: None,
                causation_depth,
                source_responder: Some(&source_responder),
            })
            .await?;
        let handoff = CreateAgentHandoff {
            id: handoff_id.clone(),
            source_chat_id: source.id,
            target_chat_id: target.id.clone(),
            source_message_id: input.source_message_id.clone(),
            source_turn_job_id: input.source_turn_job_id.clone(),
            // The source Main binding is the author. The target binding only
            // supplies the responder for the newly queued target turn.
            author_identity_id: Some(source_binding.identity_id.clone()),
            content: guarded.content.clone(),
            content_guard_json: guarded.guard_json.clone(),
            source_revisions_json: bounded_source_revisions,
            correlation_id: correlation_id.clone(),
            causation_id: None,
            dedupe_key: input.dedupe_key.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        let target_message = CreateAgentChatMessage {
            id: target_message_id.clone(),
            chat_id: target.id.clone(),
            sequence: target.message_count,
            author_type: AgentChatMessageAuthorType::Handoff,
            author_id: handoff.author_identity_id.clone(),
            content: guarded.content,
            content_guard_json: guarded.guard_json,
            sensitivity: guarded.sensitivity,
            status: AgentChatMessageStatus::Complete,
            outcome: Some("handoff_delivered".to_owned()),
            model: None,
            // Preserve source attribution on the delivered message.
            profile_id: Some(source_responder.profile_id()?.to_owned()),
            session_id: None,
            context_manifest_id: None,
            token_usage_json: None,
            duration_ms: None,
            error: None,
            correlation_id: correlation_id.clone(),
            causation_id: handoff.causation_id.clone(),
            handoff_id: Some(handoff_id.clone()),
            source_type: "handoff".to_owned(),
            source_id: Some(handoff_id.clone()),
            source_message_id: input.source_message_id,
            source_room_id: None,
            source_conversation_id: None,
            source_sequence: None,
            source_metadata_json: json!({"source_chat_id": handoff.source_chat_id}).to_string(),
            created_at: now.clone(),
        };
        let mut target_turn = prepared.apply_to_turn(CreateAgentChatTurnJob {
            id: target_turn_id,
            chat_id: target.id.clone(),
            triggering_message_id: target_message_id,
            // PreparedAgentTurnAdmission is the sole mapping authority for
            // responder/profile/policy provenance. These placeholders are
            // overwritten before the atomic handoff transaction.
            responder_identity_id: String::new(),
            profile_id: String::new(),
            responder_binding_id: None,
            responder_binding_version: None,
            responder_identity_version: None,
            profile_version: None,
            operating_skill_revision_id: None,
            policy_revision: None,
            policy_digest: None,
            permission_policy_digest: None,
            tool_policy_digest: None,
            admission_digest: None,
            canonical_scope_provenance_json: None,
            canonical_scope_type: String::new(),
            canonical_scope_id: String::new(),
            dedupe_key: String::new(),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            correlation_id,
            causation_id: None,
            causation_depth: 0,
            created_at: now.clone(),
            updated_at: now,
        })?;
        // The concrete handoff id is a persisted causation edge, not input
        // identity. It is intentionally excluded from the prepared digest so
        // response-loss replay remains idempotent.
        target_turn.causation_id = Some(handoff_id);
        let admitted = AgentChatTransactionRepo::admit_agent_handoff(
            &*self.db,
            AdmitAgentHandoff {
                handoff,
                target_message,
                target_turn,
                source_responder_provenance_json: Some(source_responder.provenance_json()?),
            },
        )
        .await?;
        Ok(AgentChatHandoffOutcome {
            handoff: admitted.handoff,
            target_message: admitted.message,
            target_turn_job: admitted.turn,
        })
    }

    async fn mark_main_chat_ready(&self, account_id: &str) -> Result<()> {
        for _ in 0..3 {
            let chat = self.ensure_main_chat(account_id).await?;
            if chat.status == READY_CHAT_STATUS {
                return Ok(());
            }
            match AgentChatRepo::update_agent_chat(
                &*self.db,
                UpdateAgentChat {
                    id: chat.id,
                    expected_version: chat.version,
                    status: Some(READY_CHAT_STATUS.to_owned()),
                    instruction_revision: None,
                    updated_at: now_rfc3339(),
                },
            )
            .await
            {
                Ok(_) => return Ok(()),
                Err(db::DbError::VersionConflict) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(ServiceError::Conflict(
            "Main Agent Chat changed while binding was configured".to_owned(),
        ))
    }

    async fn mark_project_chat_ready(&self, project_id: &str) -> Result<()> {
        for _ in 0..3 {
            let chat = self.ensure_project_chat(project_id).await?;
            if chat.status == READY_CHAT_STATUS {
                return Ok(());
            }
            match AgentChatRepo::update_agent_chat(
                &*self.db,
                UpdateAgentChat {
                    id: chat.id,
                    expected_version: chat.version,
                    status: Some(READY_CHAT_STATUS.to_owned()),
                    instruction_revision: None,
                    updated_at: now_rfc3339(),
                },
            )
            .await
            {
                Ok(_) => return Ok(()),
                Err(db::DbError::VersionConflict) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(ServiceError::Conflict(
            "Project Agent Chat changed while binding was configured".to_owned(),
        ))
    }

    async fn authorize_chat_scope(&self, actor_user_id: &str, chat: &AgentChat) -> Result<()> {
        match chat.kind.as_str() {
            MAIN_CHAT_KIND => {
                if chat.account_id.as_deref() != Some(actor_user_id) {
                    return Err(ServiceError::not_found("agent_chat", chat.id.clone()));
                }
            }
            PROJECT_CHAT_KIND => {
                let project_id = chat
                    .project_id
                    .as_deref()
                    .ok_or_else(|| ServiceError::not_found("agent_chat", chat.id.clone()))?;
                self.require_project_member(actor_user_id, project_id)
                    .await?;
            }
            _ => return Err(ServiceError::not_found("agent_chat", chat.id.clone())),
        }
        Ok(())
    }

    async fn responder_for_chat(
        &self,
        chat: &AgentChat,
    ) -> Result<crate::agent_turn_admission::ResolvedAgentResponder> {
        let resolver = AgentTurnAdmissionService::new(Arc::clone(&self.db));
        let responder = resolver.resolve(chat).await?;
        responder.require_frozen()?;
        Ok(responder)
    }

    async fn require_owned_identity(
        &self,
        actor_user_id: &str,
        identity_id: &str,
    ) -> Result<Agent> {
        AgentRepo::get_by_id(&*self.db, identity_id)
            .await?
            .filter(|identity| {
                identity.owner_id.as_deref() == Some(actor_user_id) && !identity.paused
            })
            .ok_or_else(|| ServiceError::not_found("agent_identity", identity_id.to_owned()))
    }

    async fn require_project_member(&self, actor_user_id: &str, project_id: &str) -> Result<()> {
        ProjectMemberRepo::get_member(&*self.db, project_id, actor_user_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        Ok(())
    }
}

fn bounded_json(value: &str) -> Result<String> {
    if value.chars().count() > MAX_HANDOFF_CONTENT_CHARS {
        return Err(ServiceError::invalid_operation(
            "handoff source revisions exceed the bounded limit",
        ));
    }
    let parsed: serde_json::Value = serde_json::from_str(value)
        .map_err(|_| ServiceError::invalid_operation("handoff source revisions must be JSON"))?;
    Ok(parsed.to_string())
}

/// Append one durable system message to an Agent Chat timeline, outside any
/// turn. The caller-chosen `message_id` is the idempotency key: replaying the
/// same id returns the stored message instead of appending a duplicate, so
/// lifecycle hooks can call this on every (re)execution.
pub async fn append_system_chat_message(
    db: &db::SqliteDb,
    chat_id: &str,
    message_id: &str,
    content: &str,
) -> Result<AgentChatMessage> {
    Ok(AgentChatMessageRepo::append_agent_chat_message(
        db,
        CreateAgentChatMessage {
            id: message_id.to_owned(),
            chat_id: chat_id.to_owned(),
            // The store allocates the real sequence.
            sequence: 0,
            author_type: AgentChatMessageAuthorType::System,
            author_id: None,
            content: content.to_owned(),
            content_guard_json: "{}".to_owned(),
            sensitivity: "internal".to_owned(),
            status: AgentChatMessageStatus::Complete,
            outcome: None,
            model: None,
            profile_id: None,
            session_id: None,
            context_manifest_id: None,
            token_usage_json: None,
            duration_ms: None,
            error: None,
            correlation_id: new_uuid_v4(),
            causation_id: None,
            handoff_id: None,
            source_type: "native".to_owned(),
            source_id: None,
            source_message_id: None,
            source_room_id: None,
            source_conversation_id: None,
            source_sequence: None,
            source_metadata_json: "{}".to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await?)
}
