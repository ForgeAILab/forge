//! Tool exchanges on a persisted, LCM-backed Agent Chat timeline.
//!
//! Two properties, both of which a worker or Project Agent timeline — mostly
//! `[assistant tool call, tool result]` pairs — depends on: a reused
//! provider tool-call id must not poison the durable history, and pressure
//! must see what those exchanges actually cost.
//!
//! Providers do not guarantee tool-call ids are unique across a conversation
//! — z.ai answered one tool with `call_130845` and, much later in the same
//! session, a different tool with the same id. Forge's canonical history is
//! durable (`agent_lcm_entry` is append-only and immutable), so both
//! exchanges stay on the timeline forever, and the context planner requires
//! exactly one call and one result fragment per id. From the second exchange
//! on, every later turn on that timeline failed `invalid_pairing` and no
//! retry could recover: the poisoned history was replayed each attempt.
//!
//! The runtime now re-ids a colliding call at ingest, before dispatch or
//! history. These cases hold the Forge half of that contract: the collision
//! is detected against the history Forge restored — including across a fresh
//! backend process, where the only source of the earlier id is the protected
//! session store — and the durable timeline keeps exactly one call and one
//! result per id.
//!
//! The usage-ledger cases for the same persisted session (every provider
//! call is recorded once, by the turn that made it) live in `usage_ledger.rs`.

use crate::test_services as services;
use std::collections::BTreeMap;
use std::sync::Arc;

use agent_runtime::core::content::ContentPart;
use agent_runtime::core::provider::{Capabilities, FinishReason, ProviderStreamEvent};
use agent_runtime::provider::fake::{usage_event, FakeProvider, ScriptedStream};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    CreateAgentIdentity, CreateAgentProfile, CreateProject, ProjectRepo, SqliteDb, User, UserRepo,
};
use forge_agent_host::{
    AgentSessionBackend, AgentTurnRequest, CanonicalScope, CanonicalScopeType, Message,
    NativeAgentRuntimeBackend, NativeProviderConfig, Secret, TurnEventSink, WorkspaceAccess,
};
use services::embedded_agent_service::{CreateScopedSession, RequestedCanonicalScope};
use services::{AgentChatService, EmbeddedAgentService, SetMainAgentBindingInput};
use tokio_util::sync::CancellationToken;

#[path = "native_chat_tool_exchanges/helpers.rs"]
mod helpers;
#[path = "native_chat_tool_exchanges/journal_failures.rs"]
mod journal_failures;
#[path = "native_chat_tool_exchanges/provider_failures.rs"]
mod provider_failures;
#[path = "native_chat_tool_exchanges/request_context.rs"]
mod request_context;
#[path = "native_chat_tool_exchanges/retirement.rs"]
mod retirement;
#[path = "native_chat_tool_exchanges/rotation.rs"]
mod rotation;
#[path = "native_chat_tool_exchanges/tool_history.rs"]
mod tool_history;
#[path = "native_chat_tool_exchanges/usage_ledger.rs"]
mod usage_ledger;

use helpers::*;
