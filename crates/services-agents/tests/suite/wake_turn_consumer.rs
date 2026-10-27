//! Wake turn admission, delivery, and blocker recovery tests.
use crate::test_services as services;
use std::sync::Arc;

use async_trait::async_trait;
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentChatRepo,
    AgentChatTurnJobRepo, AgentChatTurnState, AgentProfileRepo, AgentRepo, AgentStatus,
    AttentionRepo, CreateAgentIdentity, CreateAgentProfile, CreateDomainEvent, CreateProject,
    DomainEventRepo, ProjectRepo, SelectAgentProfile, SqliteDb, UpdateAgentChatTurnJob, User,
    UserRepo,
};
use services::project_escalation::EscalationAuthority::Agent;
use services::{
    wake_attention_incident_digest, AgentChatService, AgentChatTurnRunner, AgentChatTurnWorker,
    AttentionService, CompletedAgentChatTurn, CreateAgentHandoffInput, SendAgentChatMessageInput,
    ServiceError, SetMainAgentBindingInput, SetProjectAgentBindingInput, WakeTurnConsumer,
};
use tokio_util::sync::CancellationToken;

#[path = "wake_turn_consumer/helpers.rs"]
mod helpers;
use helpers::*;

#[path = "wake_turn_consumer/blocker_audits.rs"]
mod blocker_audits;
#[path = "wake_turn_consumer/blocker_recovery.rs"]
mod blocker_recovery;
#[path = "wake_turn_consumer/delivery_turns.rs"]
mod delivery_turns;
#[path = "wake_turn_consumer/responder_admission.rs"]
mod responder_admission;
#[path = "wake_turn_consumer/wake_policy.rs"]
mod wake_policy;
