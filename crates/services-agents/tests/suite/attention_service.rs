//! Attention projection, wake admission, and restart tests.
use crate::test_services as services;
use std::sync::Arc;

use chrono::{Duration, Utc};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    AttentionRepo, CreateAgentIdentity, CreateAgentProfile, CreateAttentionProjection,
    CreateDomainEvent, CreateExecution, CreateProject, CreateTask, DomainEventRepo, ExecutionRepo,
    ExecutionStatus, ProjectRepo, ResumePolicy, SqliteDb, TaskRepo, UpdateAttentionLifecycle,
};
use services::{
    workflow::{default_autonomous_workflow, default_workflow},
    AttentionService, WakeAdmissionRequest, WakeAdmissionResult, WakeSuppressionReason,
};
use tokio::sync::watch;

#[path = "attention_service/helpers.rs"]
mod helpers;
use helpers::*;

#[path = "attention_service/audit_health.rs"]
mod audit_health;
#[path = "attention_service/projection_worker.rs"]
mod projection_worker;
#[path = "attention_service/restart_upgrades.rs"]
mod restart_upgrades;
#[path = "attention_service/task_incidents.rs"]
mod task_incidents;
#[path = "attention_service/wake_admission.rs"]
mod wake_admission;
