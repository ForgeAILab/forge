//! Read and cancel surface for Main Agent inquiries.
//!
//! Dispatching one lives in [`crate::agent_inquiry_runner`]; this service is
//! only what the REST layer needs to show a run and stop it.
//!
//! Authorization reuses [`AgentChatService::get_authorized_chat`] rather than
//! inventing a second ownership check: an inquiry is reachable only through
//! the chat that dispatched it (a Main Chat authorizes by `account_id`
//! match, a Project Chat by Project membership). A caller who cannot see
//! that chat gets exactly the not-found response a caller pointing at a
//! nonexistent inquiry id gets -- existence is never leaked through a
//! different status code.

use std::sync::Arc;

use db::{
    AccountMainAgentBindingRepo, AgentChatMessageRepo, AgentChatRepo, AgentChatTransactionRepo,
    AgentChatTurnJobRepo, AgentHandoffRepo, AgentInquiry, AgentInquiryRepo, AgentInquiryStatus,
    AgentRepo, Page, ProjectAgentBindingRepo, ProjectMemberRepo,
};

use crate::{
    agent_chat_service::AgentChatService, agent_inquiry_runner::InquiryRunner,
    agent_turn_admission::AgentResponderStore, Result, ServiceError,
};

#[derive(Clone)]
pub struct AgentInquiryService<D> {
    db: Arc<D>,
    chat_service: Arc<AgentChatService<D>>,
    /// Attached after construction, like the other runtime-reaching handles
    /// in this crate: the runner needs the embedded agent service, which is
    /// built later. Without it a cancel would only mark the record and leave
    /// the sub-agent talking to its provider.
    runner: Arc<std::sync::RwLock<Option<Arc<dyn InquiryRunner>>>>,
}

impl<D> std::fmt::Debug for AgentInquiryService<D> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentInquiryService")
            .finish_non_exhaustive()
    }
}

impl<D> AgentInquiryService<D>
where
    D: AgentInquiryRepo
        + AccountMainAgentBindingRepo
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
    pub fn new(db: Arc<D>, chat_service: Arc<AgentChatService<D>>) -> Self {
        Self {
            db,
            chat_service,
            runner: Arc::new(std::sync::RwLock::new(None)),
        }
    }

    pub fn set_runner(&self, runner: Arc<dyn InquiryRunner>) {
        if let Ok(mut slot) = self.runner.write() {
            *slot = Some(runner);
        }
    }

    fn runner(&self) -> Option<Arc<dyn InquiryRunner>> {
        self.runner.read().ok().and_then(|slot| slot.clone())
    }

    /// List one chat's inquiries, newest first. The caller must already be
    /// authorized for `chat_id` (Main Chat ownership or Project
    /// membership) -- an unauthorized or unknown chat id is not-found, same
    /// as every other chat-scoped list in this crate.
    pub async fn list_for_chat(
        &self,
        actor_user_id: &str,
        chat_id: &str,
        limit: i64,
        cursor: Option<&str>,
    ) -> Result<Page<AgentInquiry>> {
        self.chat_service
            .get_authorized_chat(actor_user_id, chat_id)
            .await?;
        Ok(AgentInquiryRepo::list_agent_inquiries(&*self.db, chat_id, limit, cursor).await?)
    }

    /// Fetch one inquiry by id, authorized through the chat that dispatched
    /// it.
    pub async fn get(&self, actor_user_id: &str, inquiry_id: &str) -> Result<AgentInquiry> {
        self.authorized_inquiry(actor_user_id, inquiry_id).await
    }

    /// Cancel a running inquiry.
    ///
    /// An inquiry that already reached a terminal status
    /// (`succeeded`/`failed`/`cancelled`) is left untouched: this returns a
    /// conflict rather than reporting success, because the caller asked to
    /// stop a run and none was stopped. This mirrors
    /// `AgentChatService::cancel_turn`'s terminal-state guard for the
    /// analogous Agent Chat turn cancellation.
    pub async fn cancel(
        &self,
        actor_user_id: &str,
        inquiry_id: &str,
        expected_version: i64,
    ) -> Result<AgentInquiry> {
        let inquiry = self.authorized_inquiry(actor_user_id, inquiry_id).await?;
        if inquiry.status != AgentInquiryStatus::Running {
            return Err(ServiceError::conflict(
                "Agent Inquiry is already terminal and cannot be cancelled",
            ));
        }
        let cancelled =
            AgentInquiryRepo::cancel_agent_inquiry(&*self.db, &inquiry.id, expected_version)
                .await?;
        // Stop the provider call itself. The record is already terminal, so a
        // runner that has nothing to signal (the run finished in between, or
        // this process did not host it) is a normal outcome, not a failure.
        if let Some(runner) = self.runner() {
            runner.cancel_inquiry(&cancelled.id).await;
        }
        Ok(cancelled)
    }

    async fn authorized_inquiry(
        &self,
        actor_user_id: &str,
        inquiry_id: &str,
    ) -> Result<AgentInquiry> {
        let inquiry = AgentInquiryRepo::get_agent_inquiry(&*self.db, inquiry_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_inquiry", inquiry_id.to_owned()))?;
        match self
            .chat_service
            .get_authorized_chat(actor_user_id, &inquiry.chat_id)
            .await
        {
            Ok(_) => Ok(inquiry),
            // The owning chat exists but the caller isn't authorized for it,
            // or it doesn't exist at all -- either way, report the same
            // not-found as an unknown inquiry id rather than distinguishing
            // "forbidden" from "missing".
            Err(ServiceError::NotFound { .. }) => Err(ServiceError::not_found(
                "agent_inquiry",
                inquiry_id.to_owned(),
            )),
            Err(other) => Err(other),
        }
    }
}
