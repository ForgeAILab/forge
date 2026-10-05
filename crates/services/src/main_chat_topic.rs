//! Main Chat topic boundary service (design D21, live-acceptance finding
//! F18: "the singular Main Chat has no fresh-topic boundary").
//!
//! A topic is a durable, user-owned context epoch *inside* the one account
//! Main Chat -- this service never creates a second chat, binding, or
//! authority scope. It only ever: lists a chat's topics, reads the current
//! one, and rotates to a new one subject to the D21 denial rule (a live Main
//! turn, or a Product Genesis session that still needs an explicit
//! finish-or-cancel decision).
//!
//! The denial rule itself is enforced atomically inside the same database
//! transaction that performs the rotation (`AgentChatTopicTransactionRepo`);
//! this service's own precondition read below is a best-effort early exit so
//! a doomed request fails fast with a specific reason, not the sole guard.

use std::sync::Arc;

use db::{
    new_uuid_v4, now_rfc3339, AccountMainAgentBindingRepo, AgentChat, AgentChatMessageRepo,
    AgentChatRepo, AgentChatTopic, AgentChatTopicRepo, AgentChatTopicTransactionRepo,
    AgentChatTransactionRepo, AgentChatTurnJobRepo, AgentHandoffRepo, AgentRepo,
    CreateAgentChatTopic, ProjectAgentBindingRepo, ProjectMemberRepo, RotateAgentChatTopic,
};

use crate::{
    agent_chat_service::AgentChatService, agent_turn_admission::AgentResponderStore, Result,
    ServiceError,
};

const MAIN_CHAT_KIND: &str = "account_main";
const USER_PRINCIPAL: &str = "user";
const MAX_LABEL_CHARS: usize = 200;
const MAX_SUMMARY_CHARS: usize = 2000;
const DEFAULT_LABEL: &str = "New topic";

#[derive(Debug, Clone)]
pub struct StartMainChatTopicInput {
    pub actor_user_id: String,
    pub chat_id: String,
    pub label: Option<String>,
    pub summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MainChatTopicRotation {
    pub topic: Option<AgentChatTopic>,
    pub divider_message: Option<db::AgentChatMessage>,
    pub rotation_pending: bool,
}

#[derive(Clone)]
pub struct MainChatTopicService<D> {
    db: Arc<D>,
    chat_service: Arc<AgentChatService<D>>,
    rotator: Option<Arc<dyn crate::TopicRotator>>,
}

impl<D> std::fmt::Debug for MainChatTopicService<D> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MainChatTopicService")
            .finish_non_exhaustive()
    }
}

impl<D> MainChatTopicService<D>
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
        + ProjectMemberRepo
        + AgentChatTopicRepo
        + AgentChatTopicTransactionRepo,
{
    pub fn new(db: Arc<D>, chat_service: Arc<AgentChatService<D>>) -> Self {
        Self {
            db,
            chat_service,
            rotator: None,
        }
    }

    pub fn with_rotator(mut self, rotator: Arc<dyn crate::TopicRotator>) -> Self {
        self.rotator = Some(rotator);
        self
    }

    /// List every topic for the caller's own Main Chat, oldest first.
    /// Earlier topics remain inspectable/searchable even though a new Main
    /// turn's context is bounded to the current one (D21).
    pub async fn list_topics(
        &self,
        actor_user_id: &str,
        chat_id: &str,
    ) -> Result<Vec<AgentChatTopic>> {
        let chat = self.authorized_main_chat(actor_user_id, chat_id).await?;
        Ok(AgentChatTopicRepo::list_agent_chat_topics(&*self.db, &chat.id).await?)
    }

    /// The Main Chat's current topic. `None` only before any topic exists
    /// for this chat (a state the V103 backfill and `ensure_main_chat`
    /// startup path together should make unreachable in practice).
    pub async fn current_topic(
        &self,
        actor_user_id: &str,
        chat_id: &str,
    ) -> Result<Option<AgentChatTopic>> {
        let chat = self.authorized_main_chat(actor_user_id, chat_id).await?;
        Ok(AgentChatTopicRepo::get_current_agent_chat_topic(&*self.db, &chat.id).await?)
    }

    /// Start a fresh topic: rotates the episodic runtime context and appends
    /// a visible timeline divider, without creating a second Main Chat,
    /// binding, identity, or authority scope (D21). Denied while a Main turn
    /// is live or a Genesis session/approval needs an explicit
    /// finish-or-cancel decision.
    pub async fn start_topic(
        &self,
        input: StartMainChatTopicInput,
    ) -> Result<MainChatTopicRotation> {
        let chat = self
            .authorized_main_chat(&input.actor_user_id, &input.chat_id)
            .await?;

        let label = normalize_label(input.label.as_deref())?;
        let summary = normalize_summary(input.summary.as_deref())?;
        let now = now_rfc3339();
        let topic_id = new_uuid_v4();
        let divider_message = db::topic_divider_message(
            new_uuid_v4(),
            chat.id.clone(),
            &label,
            new_uuid_v4(),
            now.clone(),
        );

        let intent = AgentChatTopicTransactionRepo::request_agent_chat_topic(
            &*self.db,
            RotateAgentChatTopic {
                runtime_session_id: None,
                rotation_owner: None,
                topic: CreateAgentChatTopic {
                    id: topic_id,
                    chat_id: chat.id.clone(),
                    label,
                    summary,
                    principal_type: USER_PRINCIPAL.to_owned(),
                    principal_id: Some(input.actor_user_id),
                    created_at: now,
                },
                divider_message,
            },
        )
        .await?;

        let live = AgentChatTurnJobRepo::list_agent_chat_turn_jobs(&*self.db, &chat.id)
            .await?
            .iter()
            .any(|turn| {
                matches!(
                    turn.status,
                    db::AgentChatTurnState::Queued
                        | db::AgentChatTurnState::Leased
                        | db::AgentChatTurnState::RetryWait
                        | db::AgentChatTurnState::AwaitingInput
                )
            });
        if !live {
            if let Some(rotator) = &self.rotator {
                rotator.rotate_pending(&chat.id).await?;
            }
        }

        let topic = AgentChatTopicRepo::get_agent_chat_topic(&*self.db, &intent).await?;
        let divider_message = match topic
            .as_ref()
            .and_then(|topic| topic.starting_message_id.as_deref())
        {
            Some(id) => AgentChatMessageRepo::get_agent_chat_message(&*self.db, id).await?,
            None => None,
        };
        Ok(MainChatTopicRotation {
            rotation_pending: topic.is_none(),
            topic,
            divider_message,
        })
    }

    async fn authorized_main_chat(&self, actor_user_id: &str, chat_id: &str) -> Result<AgentChat> {
        let chat = self
            .chat_service
            .get_authorized_chat(actor_user_id, chat_id)
            .await?;
        if chat.kind != MAIN_CHAT_KIND {
            // Topics exist only inside the one account Main Chat (D21); a
            // Project Chat id can never reach this service.
            return Err(ServiceError::not_found("agent_chat", chat.id));
        }
        Ok(chat)
    }
}

fn normalize_label(label: Option<&str>) -> Result<String> {
    let trimmed = label.map(str::trim).filter(|value| !value.is_empty());
    let label = trimmed.unwrap_or(DEFAULT_LABEL);
    if label.chars().count() > MAX_LABEL_CHARS {
        return Err(ServiceError::invalid_operation(
            "topic label exceeds the bounded limit",
        ));
    }
    Ok(label.to_owned())
}

fn normalize_summary(summary: Option<&str>) -> Result<Option<String>> {
    let Some(summary) = summary.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if summary.chars().count() > MAX_SUMMARY_CHARS {
        return Err(ServiceError::invalid_operation(
            "topic summary exceeds the bounded limit",
        ));
    }
    Ok(Some(summary.to_owned()))
}
