//! File-backed migration test modules.

use db::{
    create_sqlite_pool, run_migrations, run_migrations_from, AgentActionPolicyResult,
    AgentActionRepo, AgentActionStatus, AgentChatRepo, AgentCommitmentRepo, AgentCommitmentStatus,
    AgentInboxKind, AgentInboxRepo, AgentInboxStatus, AgentRepo, AgentSessionRepo, AgentStatus,
    CreateAgent, CreateAgentAction, CreateAgentCommitment, CreateAgentInboxItem,
    CreateAgentSession, CreateContextManifest, CreateContextManifestSource, CreateDomainEvent,
    CreateForgeMemorySourceBinding, CreateTask, DomainEventRepo, MemoryItem,
    ScopedMemoryRepository, SqliteDb, TaskRepo, User, UserRepo,
};
use sqlx::Row;
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use helpers::*;

#[path = "file_backed_migration/bundle_and_runner.rs"]
mod bundle_and_runner;
#[path = "file_backed_migration/chat_threads.rs"]
mod chat_threads;
#[path = "file_backed_migration/chat_usage.rs"]
mod chat_usage;
#[path = "file_backed_migration/checks_and_doctrine.rs"]
mod checks_and_doctrine;
#[path = "file_backed_migration/execution_usage.rs"]
mod execution_usage;
#[path = "file_backed_migration/helpers.rs"]
mod helpers;
#[path = "file_backed_migration/identity_and_scope.rs"]
mod identity_and_scope;
#[path = "file_backed_migration/wake_delivery.rs"]
mod wake_delivery;
#[path = "file_backed_migration/workspace_and_steps.rs"]
mod workspace_and_steps;
