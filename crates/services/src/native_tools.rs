//! Forge service adapter for the host's scope-derived native tools.
//!
//! This adapter exposes read projections and policy-checked proposal commands.
//! Query operations use the read boundary; direct `task.propose`,
//! `task.adaptive`, Main Charter, and bounded Project orchestration commands
//! call their shared command services, while approval-required mutations
//! retain an `AgentAction` envelope.

mod registered_reads;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Read,
    net::{IpAddr, Ipv6Addr},
    path::{Component, Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};

use sha2::{Digest, Sha256};

use api_types::{
    ApprovalTarget, CanonicalScopeRef as OutcomeScopeRef, CurrentVersionOrRevision, DeniedBy,
    OrchestrationOutcome, OutcomeCode, OutcomeScopeType, OutcomeStatus, RetryAction,
    RetryInstruction, SetupRequirement,
};
use async_trait::async_trait;
use chrono::Utc;
use config::PublicSearchConfig;
use db::{
    AgentAction, AgentActionListQuery, AgentActionPolicyResult, AgentActionRepo,
    AgentCommitmentListQuery, AgentCommitmentRepo, AgentInboxListQuery, AgentInboxRepo,
    CommandReceiptRepo, MemoryScopeGrant, SqliteDb, TaskBoardRepo,
};
use forge_agent_host::{
    contains_adaptive_authority_override, contains_authority_override, operation_contract,
    operation_descriptor, operation_permission, AgentHostError, CanonicalScope, CanonicalScopeType,
    CommandObservation, ForgeToolProvider, OperationClassification, PublicSearchScope,
    WorkspaceAccess, MAIN_CHARTER_DRAFT_OPERATION, MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
    MAIN_GENESIS_START_OPERATION, MAIN_PROJECT_CREATE_OPERATION,
    PROJECT_CHARTER_ADOPTION_OPERATION, PROJECT_CURRENT_STATE_OPERATION,
    PROJECT_DECISION_OPERATION, PROJECT_DOCUMENT_OPERATION, PROJECT_ESCALATE_OPERATION,
    PROJECT_EVIDENCE_OPERATION, PROJECT_MILESTONE_OPERATION, PROJECT_OBSERVATIONS_OPERATION,
    PROJECT_READINESS_OPERATION, PROJECT_RELEASE_OPERATION, PROJECT_VALIDATION_OPERATION,
    TASK_ACTION_OPERATION, TASK_ADAPTIVE_OPERATION, TASK_DEPENDENCY_OPERATION,
    TASK_EVIDENCE_OPERATION, TASK_PLAN_OPERATION, TASK_PROPOSE_OPERATION, TASK_WORKLOG_OPERATION,
};
use reqwest::header::ACCEPT;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    agent_chat_policy::guard_agent_chat_content,
    agent_inquiry_runner::{InquiryRequest, InquiryRunner},
    coordination_service::{AgentActionService, ProposeActionInput},
    memory::{MemoryAccessContext, MemoryService},
    project_agent_actions::ExecuteDirectProjectCommandInput,
    project_runtime::{
        load_effective_project_state, load_project_adoption_charter, ProjectCurrentStateResponse,
    },
    task_service::{
        AdaptiveTaskChild, AdaptiveTaskCommand, AdaptiveTaskCommandResult, AdaptiveTaskOperation,
        DirectTaskProposalInput, TaskDependencyAction, TaskProposalCommandResult,
        TaskProposalPayload,
    },
    MainGenesisCharterDraftRequest, MainGenesisCommandService, MainGenesisDraftCommandInput,
    MainGenesisDraftPrincipal, MainGenesisProjectAgentSelectCommandInput,
    MainGenesisProjectAgentSelectRequest, MainGenesisStartCommandInput, MainGenesisStartPrincipal,
    MainGenesisStartRequest, MainOrchestrationQueryService, OrchestrationAuthorizationService,
    ProjectOrchestrationActionService, TaskService,
};

/// Closed native payload for the bounded adaptive Task command.  The adapter
/// owns only transport decoding; Project, actor, permission, governance, and
/// fixed-boundary values are filled by the server and never accepted here.
#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum AdaptiveTaskPayload {
    Split {
        source_task_id: String,
        expected_task_version: i64,
        expected_board_revision: i64,
        rationale: String,
        items: Vec<AdaptiveTaskChildPayload>,
    },
    Sequence {
        source_task_id: String,
        expected_task_version: i64,
        expected_board_revision: i64,
        rationale: String,
        ordered_task_ids: Vec<String>,
    },
    Replace {
        source_task_id: String,
        expected_task_version: i64,
        expected_board_revision: i64,
        rationale: String,
        title: String,
        description: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdaptiveTaskChildPayload {
    title: String,
    description: Option<String>,
    assignee_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum TaskDependencyPayload {
    Add {
        task_id: String,
        depends_on_task_id: String,
        rationale: String,
    },
    Remove {
        task_id: String,
        depends_on_task_id: String,
        rationale: String,
    },
}

impl TaskDependencyPayload {
    fn into_parts(self) -> (String, String, TaskDependencyAction, String) {
        match self {
            Self::Add {
                task_id,
                depends_on_task_id,
                rationale,
            } => (
                task_id,
                depends_on_task_id,
                TaskDependencyAction::Add,
                rationale,
            ),
            Self::Remove {
                task_id,
                depends_on_task_id,
                rationale,
            } => (
                task_id,
                depends_on_task_id,
                TaskDependencyAction::Remove,
                rationale,
            ),
        }
    }
}

impl AdaptiveTaskPayload {
    fn into_command_parts(self) -> (String, i64, i64, AdaptiveTaskOperation, String) {
        match self {
            Self::Split {
                source_task_id,
                expected_task_version,
                expected_board_revision,
                rationale,
                items,
            } => (
                source_task_id,
                expected_task_version,
                expected_board_revision,
                AdaptiveTaskOperation::Split {
                    items: items
                        .into_iter()
                        .map(|item| AdaptiveTaskChild {
                            title: item.title,
                            description: item.description,
                            assignee_id: item.assignee_id,
                        })
                        .collect(),
                },
                rationale,
            ),
            Self::Sequence {
                source_task_id,
                expected_task_version,
                expected_board_revision,
                rationale,
                ordered_task_ids,
            } => (
                source_task_id,
                expected_task_version,
                expected_board_revision,
                AdaptiveTaskOperation::Sequence { ordered_task_ids },
                rationale,
            ),
            Self::Replace {
                source_task_id,
                expected_task_version,
                expected_board_revision,
                rationale,
                title,
                description,
            } => (
                source_task_id,
                expected_task_version,
                expected_board_revision,
                AdaptiveTaskOperation::Replace { title, description },
                rationale,
            ),
        }
    }
}

type SessionDenialRows = BTreeMap<(String, String), Vec<db::ChatSessionDenial>>;

/// Forge-owned provider injected into native Agent Runtime compositions.
#[derive(Clone)]
pub struct CoordinationToolProvider {
    db: Arc<SqliteDb>,
    actions: AgentActionService,
    authorization: OrchestrationAuthorizationService,
    memory: MemoryService,
    main_queries: MainOrchestrationQueryService,
    project_actions: ProjectOrchestrationActionService,
    public_search: Arc<RwLock<Option<PublicSearchConfig>>>,
    /// Reminders seen by the resolved turn's state-card read. Success only
    /// deletes a durable reminder when that read actually found one.
    read_session_denials: Arc<RwLock<SessionDenialRows>>,
    /// Shared TaskService used to execute directly admitted `task.propose` and
    /// `task.adaptive` commands inline, so native proposals materialize
    /// through the durable command receipt path without a separate caller.
    task_service: Arc<RwLock<Option<Arc<TaskService>>>>,
    /// Root of the media store. Evidence capture writes the bytes it was given
    /// here before recording the Task media row that references them.
    media_root: Arc<RwLock<Option<PathBuf>>>,
    /// Dispatches ephemeral inquiry sub-agents. Attached after construction
    /// for the same reason `task_service` is: the runner reaches the native
    /// backend, which holds this provider, so wiring it at construction would
    /// close a cycle.
    inquiry_runner: Arc<RwLock<Option<Arc<dyn InquiryRunner>>>>,
}

impl std::fmt::Debug for CoordinationToolProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CoordinationToolProvider")
            .finish_non_exhaustive()
    }
}

impl CoordinationToolProvider {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
            actions: AgentActionService::new(Arc::clone(&db)),
            authorization: OrchestrationAuthorizationService::new(Arc::clone(&db)),
            memory: MemoryService::new(Arc::clone(&db)),
            main_queries: MainOrchestrationQueryService::new(Arc::clone(&db)),
            project_actions: ProjectOrchestrationActionService::new(Arc::clone(&db)),
            public_search: Arc::new(RwLock::new(None)),
            read_session_denials: Arc::new(RwLock::new(BTreeMap::new())),
            task_service: Arc::new(RwLock::new(None)),
            media_root: Arc::new(RwLock::new(None)),
            inquiry_runner: Arc::new(RwLock::new(None)),
            db,
        }
    }

    /// Recheck stored causes through the same policy used by native calls.
    /// Reminders are advisory: a read, recheck or deletion failure cannot fail
    /// turn admission or hide the agent's normal state card.
    pub async fn chat_session_denials(
        &self,
        identity_id: &str,
        profile_id: &str,
        chat_id: &str,
        session_id: Option<&str>,
    ) -> Vec<(String, DeniedBy)> {
        let key = (identity_id.to_owned(), chat_id.to_owned());
        if session_id.is_some() {
            if let Ok(mut read) = self.read_session_denials.write() {
                read.remove(&key);
            }
        }
        match self
            .recheck_session_denials(identity_id, profile_id, chat_id, session_id)
            .await
        {
            Ok((unavailable, rows)) => {
                if session_id.is_some() {
                    if let Ok(mut read) = self.read_session_denials.write() {
                        read.insert(key, rows);
                    }
                }
                unavailable
            }
            Err(error) => {
                tracing::warn!(identity_id, chat_id, error = %error, "could not read native denial reminders");
                Vec::new()
            }
        }
    }

    async fn recheck_session_denials(
        &self,
        identity_id: &str,
        profile_id: &str,
        chat_id: &str,
        session_id: Option<&str>,
    ) -> crate::Result<(Vec<(String, DeniedBy)>, Vec<db::ChatSessionDenial>)> {
        use db::ChatSessionDenialRepo;
        let rows = self
            .db
            .chat_session_denials(identity_id, profile_id, chat_id, session_id)
            .await?;
        let mut unavailable = Vec::new();
        let mut holding_rows = Vec::new();
        for row in rows {
            let mut cause = row.denied_by.parse::<DeniedBy>().ok();
            let holds = match cause.as_ref() {
                Some(DeniedBy::OperationNotInScope) => true,
                Some(DeniedBy::ProjectPaused(_)) => {
                    let scope = CanonicalScope {
                        scope_type: CanonicalScopeType::AgentChat,
                        scope_id: chat_id.to_owned(),
                        workspace_access: WorkspaceAccess::Deny,
                    };
                    let project_id = self
                        .authorization
                        .project_orchestration_target(identity_id, &scope)
                        .await?;
                    let pause = sqlx::query_scalar::<_, Option<String>>(
                        "SELECT system_pause_reason FROM project WHERE id = ? AND paused_at IS NOT NULL",
                    ).bind(project_id).fetch_optional(self.db.pool()).await?;
                    if let Some(reason) = pause {
                        cause = Some(db::project_pause_denial(reason.as_deref()));
                        true
                    } else {
                        false
                    }
                }
                Some(cause) if cause.withdraws_operation() && cause.clears() => {
                    if let Some(permission) =
                        operation_permission(CanonicalScopeType::AgentChat, &row.operation)
                    {
                        let (result, reason) = self
                            .actions
                            .evaluate_direct_command_policy(
                                identity_id,
                                "agent_chat",
                                chat_id,
                                permission,
                                &row.operation,
                                None,
                            )
                            .await?;
                        result == AgentActionPolicyResult::Denied
                            && reason
                                .as_deref()
                                .map(|reason| native_denial_cause(reason, Some(permission)))
                                .as_ref()
                                == Some(cause)
                    } else {
                        false
                    }
                }
                _ => false,
            };
            if holds {
                let entry = (
                    row.operation.clone(),
                    cause.expect("holding cause is parsed"),
                );
                if !unavailable.contains(&entry) {
                    unavailable.push(entry);
                }
                holding_rows.push(row);
            } else {
                self.db.delete_chat_session_denial(&row).await?;
            }
        }
        Ok((unavailable, holding_rows))
    }

    /// Attach the shared TaskService so admitted Task commands execute inline
    /// through the shared receipt-backed command paths.
    /// Attach the media storage root so Task sessions can capture evidence.
    pub fn set_media_root(&self, media_root: PathBuf) {
        if let Ok(mut slot) = self.media_root.write() {
            *slot = Some(media_root);
        }
    }

    fn media_root_handle(&self) -> Option<PathBuf> {
        self.media_root.read().ok().and_then(|slot| slot.clone())
    }

    pub fn set_task_service(&self, task_service: Arc<TaskService>) {
        if let Ok(mut slot) = self.task_service.write() {
            *slot = Some(task_service);
        }
    }

    fn task_service_handle(&self) -> Option<Arc<TaskService>> {
        self.task_service.read().ok().and_then(|slot| slot.clone())
    }

    pub fn set_inquiry_runner(&self, runner: Arc<dyn InquiryRunner>) {
        if let Ok(mut slot) = self.inquiry_runner.write() {
            *slot = Some(runner);
        }
    }

    fn inquiry_runner_handle(&self) -> Option<Arc<dyn InquiryRunner>> {
        self.inquiry_runner
            .read()
            .ok()
            .and_then(|slot| slot.clone())
    }

    /// Check the immutable command identity before evaluating mutable policy.
    /// An exact adaptive retry must reach the shared receipt replay path even
    /// if the Project's current governance has changed since the original
    /// commit.  The command service still performs the digest-aware lookup and
    /// returns an idempotency conflict for a changed payload.
    async fn adaptive_receipt_exists(
        &self,
        actor_identity_id: &str,
        project_id: &str,
        idempotency_key: &str,
    ) -> Result<bool, AgentHostError> {
        CommandReceiptRepo::get_command_receipt_by_identity(
            &*self.db,
            "agent",
            actor_identity_id,
            "project",
            project_id,
            TASK_ADAPTIVE_OPERATION,
            idempotency_key,
        )
        .await
        .map(|receipt| receipt.is_some())
        .map_err(|_| AgentHostError::ProtectedPersistence)
    }

    /// Configure the optional public search endpoint used by native Main and
    /// Project Agent Chat turns.  This is a runtime setting, not a credential
    /// store; the provider never accepts authentication headers or cookies.
    pub fn set_public_search_config(&self, config: Option<PublicSearchConfig>) {
        if let Ok(mut slot) = self.public_search.write() {
            *slot = config;
        }
    }

    fn public_search_config(&self) -> Option<PublicSearchConfig> {
        self.public_search.read().ok().and_then(|slot| slot.clone())
    }

    async fn summary(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
    ) -> Result<Value, AgentHostError> {
        let (query, bind_id) = match scope.scope_type {
            CanonicalScopeType::Account => (
                "SELECT id, name, status, paused, visibility FROM agent_identity WHERE id = ?",
                actor_identity_id,
            ),
            CanonicalScopeType::Project => (
                "SELECT id, name FROM project WHERE id = ?",
                scope.scope_id.as_str(),
            ),
            CanonicalScopeType::AgentChat => (
                "SELECT id, kind, status, kind AS scope_type, id AS scope_id FROM agent_chat WHERE id = ?",
                scope.scope_id.as_str(),
            ),
            CanonicalScopeType::Task => (
                "SELECT id, project_id, title, status, priority FROM task WHERE id = ?",
                scope.scope_id.as_str(),
            ),
        };
        let row = sqlx::query(query)
            .bind(bind_id)
            .fetch_optional(self.db.pool())
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?
            .ok_or_else(|| {
                AgentHostError::Authority("current Forge scope is unavailable".into())
            })?;
        let mut result = serde_json::Map::new();
        for column in [
            "id",
            "name",
            "title",
            "status",
            "paused",
            "visibility",
            "scope_type",
            "scope_id",
            "project_id",
            "priority",
        ] {
            if let Ok(value) = row.try_get::<String, _>(column) {
                result.insert(column.to_owned(), Value::String(value));
            } else if let Ok(value) = row.try_get::<i64, _>(column) {
                result.insert(column.to_owned(), Value::Number(value.into()));
            }
        }
        if scope.scope_type == CanonicalScopeType::Task {
            if let Some(service) = self.task_service_handle() {
                let offers = service
                    .task_action_offers(
                        &scope.scope_id,
                        &api_types::Actor::agent(actor_identity_id),
                    )
                    .await
                    .map_err(service_error)?;
                result.insert(
                    "available_actions".to_owned(),
                    json!(offers.available_actions),
                );
                result.insert("version".to_owned(), json!(offers.version));
            }
        }
        result.insert(
            "canonical_scope".to_owned(),
            json!({
                "type": scope_type_name(scope.scope_type),
                "id": scope.scope_id,
                "workspace_access": workspace_access_name(scope.workspace_access),
            }),
        );
        Ok(Value::Object(result))
    }

    async fn memory_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
        decision_only: bool,
    ) -> Result<Value, AgentHostError> {
        let query = arguments
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .chars()
            .take(512)
            .collect::<String>();
        let limit = arguments
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(10)
            .clamp(1, 20) as u32;
        let visibility = match scope.scope_type {
            CanonicalScopeType::Account => vec!["account".to_owned(), "private".to_owned()],
            CanonicalScopeType::Project => vec!["project".to_owned(), "private".to_owned()],
            CanonicalScopeType::AgentChat => vec![
                "chat".to_owned(),
                "project".to_owned(),
                "private".to_owned(),
            ],
            CanonicalScopeType::Task => vec![
                "task".to_owned(),
                "project".to_owned(),
                "private".to_owned(),
            ],
        };
        // Agent Chat history is owned by the chat. The chat repository
        // performs the binding check before this provider is composed.
        let access = MemoryAccessContext {
            identity_id: Some(actor_identity_id.to_owned()),
            grants: vec![MemoryScopeGrant {
                scope_type: scope_type_name(scope.scope_type).to_owned(),
                scope_id: scope.scope_id.clone(),
                visibility,
                identity_id: Some(actor_identity_id.to_owned()),
            }],
        };
        let (items, has_more, cursor) = self
            .memory
            .search_scoped(
                &access,
                query,
                Some(2),
                if decision_only {
                    limit.saturating_mul(5).min(100)
                } else {
                    limit
                },
                None,
            )
            .await
            .map_err(service_error)?;
        let items = items
            .into_iter()
            .filter(|item| !decision_only || item.kind == db::MemoryKind::Decision)
            .take(limit as usize)
            .map(|item| {
                json!({
                    "id": item.id.to_string(),
                    "kind": item.kind.to_string(),
                    "title": item.title,
                    "summary": item.summary,
                    "source_type": item.source_type.to_string(),
                    "created_at": item.created_at,
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({"items": items, "has_more": has_more, "next_cursor": cursor}))
    }

    async fn scoped_rows(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        let limit = arguments
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(20)
            .clamp(1, 50) as i64;
        match operation {
            "work.read" => self.read_work(actor_identity_id, scope, limit).await,
            "events.read" => self.read_events(scope, limit).await,
            "inbox.read" => self.read_inbox(actor_identity_id, scope, limit).await,
            "commitments.read" => self.read_commitments(actor_identity_id, scope, limit).await,
            "delivery.read" => self.read_delivery(actor_identity_id, scope, limit).await,
            _ => Err(AgentHostError::Unsupported(
                "Forge scoped read operation is not implemented".to_owned(),
            )),
        }
    }

    async fn discovery_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        input: operation_registry::main_reads::BoundedListQuery,
    ) -> Result<Value, AgentHostError> {
        let account_id = self
            .authorization
            .main_account_id(actor_identity_id, scope)
            .await
            .map_err(native_scope_error)?;
        let limit = input.limit.unwrap_or(10).clamp(1, 20) as i64;
        let rows = sqlx::query(
            "SELECT id, maturity, lifecycle, project_id, handoff_id, version,
                    created_at, updated_at
             FROM product_genesis_session
             WHERE account_id = ?
             ORDER BY updated_at DESC, id DESC LIMIT ?",
        )
        .bind(account_id)
        .bind(limit)
        .fetch_all(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?;
        Ok(json!({
            "items": rows.into_iter().map(|row| json!({
                "id": row.try_get::<String, _>("id").unwrap_or_default(),
                "maturity": row.try_get::<String, _>("maturity").unwrap_or_default(),
                "lifecycle": row.try_get::<String, _>("lifecycle").unwrap_or_default(),
                "project_id": row.try_get::<Option<String>, _>("project_id").ok().flatten(),
                "handoff_id": row.try_get::<Option<String>, _>("handoff_id").ok().flatten(),
                "version": row.try_get::<i64, _>("version").unwrap_or_default(),
                "created_at": row.try_get::<String, _>("created_at").unwrap_or_default(),
                "updated_at": row.try_get::<String, _>("updated_at").unwrap_or_default(),
            })).collect::<Vec<_>>()
        }))
    }

    async fn portfolio_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        input: operation_registry::main_reads::BoundedListQuery,
    ) -> Result<Value, AgentHostError> {
        let account_id = self
            .authorization
            .main_account_id(actor_identity_id, scope)
            .await
            .map_err(native_scope_error)?;
        let limit = input.limit.unwrap_or(20).clamp(1, 20) as i64;
        let rows = sqlx::query(
            "SELECT id, name, paused_at, created_at, updated_at
             FROM project WHERE owner_id = ? ORDER BY updated_at DESC, id DESC LIMIT ?",
        )
        .bind(account_id)
        .bind(limit)
        .fetch_all(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?;
        Ok(json!({
            "items": rows.into_iter().map(|row| json!({
                "id": row.try_get::<String, _>("id").unwrap_or_default(),
                "name": row.try_get::<String, _>("name").unwrap_or_default(),
                "paused": row.try_get::<Option<String>, _>("paused_at").ok().flatten().is_some(),
                "created_at": row.try_get::<String, _>("created_at").unwrap_or_default(),
                "updated_at": row.try_get::<String, _>("updated_at").unwrap_or_default(),
            })).collect::<Vec<_>>()
        }))
    }

    /// Returns the bounded Project projection used by the Project Agent
    /// orchestration tool.  It intentionally contains no repository path,
    /// Workspace lease, credential, or cross-Project metadata.
    /// Capture one artifact this Task run produced as authoritative evidence.
    ///
    /// This is deliberately the only evidence-*producing* operation in the
    /// system, and it is deliberately Task-scoped. A Project Agent session has
    /// no workspace and no process by construction, so anything it "captured"
    /// would be authored rather than observed. A Task session has both, so the
    /// artifact it registers here is a real product of a real run.
    /// Append one entry to the Task worklog the next role will read.
    ///
    /// Provenance is server-derived on purpose: the Task comes from the bound
    /// scope, the execution and role from the run that is actually holding the
    /// workspace, and the identity from the session. An agent can write what it
    /// did; it cannot assert who it was or which run it belonged to.
    async fn execute_task_worklog_append(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: &Value,
        payload: &Value,
    ) -> Result<Value, AgentHostError> {
        if scope.scope_type != CanonicalScopeType::Task {
            return Err(AgentHostError::Authority(
                "a worklog entry belongs to a Task session".to_owned(),
            ));
        }
        let task_id = scope.scope_id.clone();
        let kind = payload
            .get("kind")
            .and_then(Value::as_str)
            .filter(|value| matches!(*value, "progress" | "decision" | "validation" | "blocker"))
            .ok_or_else(|| {
                invalid_arguments(
                    "kind must be progress, decision, validation, or blocker".to_owned(),
                )
            })?
            .to_owned();
        let summary = payload
            .get("summary")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid_arguments("summary is required".to_owned()))?;
        if summary.chars().count() > MAX_WORKLOG_SUMMARY_CHARS {
            return Err(invalid_arguments(format!(
                "summary exceeds the {MAX_WORKLOG_SUMMARY_CHARS} character worklog limit"
            )));
        }

        // The run holding the workspace is the one that authored this entry.
        let execution: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT id, role FROM execution
             WHERE task_id = ? AND agent_id = ? AND status = 'running'
             ORDER BY created_at DESC LIMIT 1",
        )
        .bind(&task_id)
        .bind(actor_identity_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        let (execution_id, role) = match execution {
            Some((id, role)) => (Some(id), role),
            None => (None, None),
        };

        let idempotency_key = correlation_id(arguments, TASK_WORKLOG_OPERATION, scope);
        let now = db::now_rfc3339();
        let created = db::TaskCommentRepo::create_comment(
            &*self.db,
            db::CreateTaskComment {
                id: db::new_uuid_v4(),
                task_id: task_id.clone(),
                author_type: db::CommentAuthorType::Agent,
                author_id: Some(actor_identity_id.to_owned()),
                author_name: role.clone().unwrap_or_else(|| "agent".to_owned()),
                content: summary.to_owned(),
                execution_id,
                role: role.clone(),
                worklog_kind: Some(kind.clone()),
                idempotency_key: Some(idempotency_key),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .map_err(|error| AgentHostError::Runtime(error.to_string()))?;

        Ok(json!({
            "operation": TASK_WORKLOG_OPERATION,
            "task_id": task_id,
            "comment_id": created.id,
            "kind": kind,
            "execution_id": created.execution_id,
            "role": created.role,
            "domain_committed": true,
        }))
    }

    async fn execute_task_evidence_capture(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        payload: &Value,
    ) -> Result<Value, AgentHostError> {
        if scope.scope_type != CanonicalScopeType::Task {
            return Err(AgentHostError::Authority(
                "evidence capture requires a Task scope with a workspace".to_owned(),
            ));
        }
        let task_id = scope.scope_id.clone();
        let (kind, caption) = evidence_kind_and_caption(payload).map_err(invalid_arguments)?;
        let path = payload
            .get("path")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let content = payload
            .get("content")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        // One source of bytes, never both: a caller supplying each would leave
        // the stored artifact ambiguous about what was actually observed.
        let (bytes, default_name, content_type) = match (path, content) {
            (Some(path), None) => {
                let workspace = self.task_workspace(&task_id).await?;
                let root = workspace
                    .embedded_path()
                    .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
                let resolved =
                    resolve_workspace_artifact(&root, path).map_err(invalid_arguments)?;
                let relative = resolved
                    .strip_prefix(root.canonicalize().map_err(|error| {
                        AgentHostError::Runtime(format!("Task workspace is unavailable: {error}"))
                    })?)
                    .map_err(|error| AgentHostError::Authority(error.to_string()))?
                    .to_string_lossy()
                    .into_owned();
                let bytes = workspace
                    .backend
                    .read(
                        &workspace.placement,
                        &relative,
                        MAX_CAPTURED_EVIDENCE_BYTES as u64,
                    )
                    .await
                    .map_err(|error| {
                        AgentHostError::Runtime(format!("captured artifact is unreadable: {error}"))
                    })?;
                let name = resolved
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("artifact")
                    .to_owned();
                let content_type = content_type_for(&name, kind);
                (bytes, name, content_type)
            }
            (None, Some(content)) => (
                content.as_bytes().to_vec(),
                format!("{kind}.txt"),
                "text/plain".to_owned(),
            ),
            (Some(_), Some(_)) => {
                return Err(invalid_arguments(
                    "supply either path or content, not both".to_owned(),
                ));
            }
            (None, None) => {
                return Err(invalid_arguments(
                    "evidence capture requires either a workspace path or inline content"
                        .to_owned(),
                ));
            }
        };
        if bytes.is_empty() {
            return Err(invalid_arguments("captured artifact is empty".to_owned()));
        }
        if bytes.len() as i64 > MAX_CAPTURED_EVIDENCE_BYTES {
            return Err(invalid_arguments(format!(
                "captured artifact exceeds the {MAX_CAPTURED_EVIDENCE_BYTES} byte capture limit"
            )));
        }
        let filename = payload
            .get("filename")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty() && !value.contains('/') && !value.contains('\\'))
            .map_or(default_name, str::to_owned);
        let filename = match payload.get("kind").and_then(Value::as_str) {
            Some(original_kind) if original_kind != kind => {
                format!("[{}] {filename}", original_kind.replace(['/', '\\'], "_"))
            }
            _ => filename,
        };

        let stored = self
            .store_task_evidence(actor_identity_id, &task_id, &filename, content_type, &bytes)
            .await?;

        Ok(json!({
            "operation": TASK_EVIDENCE_OPERATION,
            "task_id": task_id,
            "media_id": stored.media_id,
            "asset_id": stored.asset_id,
            "checksum": stored.checksum,
            "byte_size": stored.byte_size,
            "kind": kind,
            "caption": caption,
            "domain_committed": true,
        }))
    }

    async fn execute_task_plan_write(
        &self,
        actor_identity_id: &str,
        runtime_session_id: &str,
        scope: &CanonicalScope,
        payload: &Value,
    ) -> Result<Value, AgentHostError> {
        if scope.scope_type != CanonicalScopeType::Task {
            return Err(AgentHostError::Authority(
                "a plan candidate belongs to a Task session".to_owned(),
            ));
        }
        if payload.get("action").and_then(Value::as_str) != Some("write") {
            return Err(invalid_arguments(
                "task.plan action must be write".to_owned(),
            ));
        }
        let content = payload
            .get("content")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| invalid_arguments("plan content is required".to_owned()))?;
        let executions: Vec<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT e.id, e.role, e.workspace_id
             FROM agent_session AS s
             JOIN agent_context_scope AS scope ON scope.id = s.context_scope_id
             JOIN execution AS e
               ON e.task_id = scope.task_id AND e.agent_id = s.identity_id
             WHERE s.runtime_session_id = ? AND s.identity_id = ?
               AND scope.scope_type = 'task' AND scope.scope_id = ?
               AND scope.task_id = ? AND e.status = 'running'
               AND (
                    (scope.task_role = 'planner' AND e.role = 'planner')
                    OR (scope.task_role = 'worker' AND e.role IN ('worker', 'coder', 'executor'))
               )
             ORDER BY e.created_at DESC, e.id DESC LIMIT 2",
        )
        .bind(runtime_session_id)
        .bind(actor_identity_id)
        .bind(&scope.scope_id)
        .bind(&scope.scope_id)
        .fetch_all(self.db.pool())
        .await
        .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        let [(execution_id, role, workspace_id)] = executions.as_slice() else {
            return Err(AgentHostError::Authority(
                "task.plan requires one exact running execution for this native session".to_owned(),
            ));
        };
        let execution_id = execution_id.clone();
        let role = role.clone();
        let workspace_id = workspace_id.clone();
        if !executors::task_role_can_write_plan(Some(&role)) {
            return Err(AgentHostError::Authority(
                "task.plan is available only to planner and implementation roles".to_owned(),
            ));
        }
        let workspace_id = workspace_id.ok_or_else(|| {
            AgentHostError::Authority(
                "task.plan requires an execution-bound Task workspace".to_owned(),
            )
        })?;
        let worktree_path: Option<String> = sqlx::query_scalar(
            "SELECT worktree_path FROM workspace WHERE id = ? AND status = 'ready'",
        )
        .bind(workspace_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        let worktree_path = worktree_path.ok_or_else(|| {
            AgentHostError::Runtime(
                "this execution has no active workspace for its plan candidate".to_owned(),
            )
        })?;
        match crate::plan_artifact::write_execution_outbox_plan(
            Path::new(&worktree_path),
            &execution_id,
            content,
        ) {
            Ok(()) => {}
            Err(
                crate::plan_artifact::PlanArtifactError::MissingChecklist { .. }
                | crate::plan_artifact::PlanArtifactError::FileTooLarge { .. },
            ) => {
                return Err(invalid_arguments(
                    "plan content must be a Markdown checklist no larger than 1 MiB".to_owned(),
                ));
            }
            Err(_) => {
                return Err(AgentHostError::Runtime(
                    "Forge could not store this execution's plan candidate".to_owned(),
                ));
            }
        }
        let checklist_items = crate::plan_artifact::parse_plan_markdown(content)
            .items
            .len();
        Ok(json!({
            "operation": TASK_PLAN_OPERATION,
            "task_id": scope.scope_id,
            "execution_id": execution_id,
            "role": role,
            "checklist_items": checklist_items,
            "domain_committed": true,
        }))
    }

    /// Store one captured artifact as Task media with the checksum milestone
    /// evidence attachment compares against.
    async fn store_task_evidence(
        &self,
        actor_identity_id: &str,
        task_id: &str,
        filename: &str,
        content_type: String,
        bytes: &[u8],
    ) -> Result<StoredEvidence, AgentHostError> {
        self.store_task_evidence_with_id(
            actor_identity_id,
            task_id,
            filename,
            content_type,
            bytes,
            db::new_uuid_v4(),
        )
        .await
    }

    /// Store Task evidence under a caller-selected durable identity.
    ///
    /// CLI outbox ingestion derives this identity from the execution and record
    /// position. If the process stops after the media row commits but before its
    /// caption comment is appended, replay resolves the same media instead of
    /// creating another asset.
    async fn store_task_evidence_with_id(
        &self,
        actor_identity_id: &str,
        task_id: &str,
        filename: &str,
        content_type: String,
        bytes: &[u8],
        media_id: String,
    ) -> Result<StoredEvidence, AgentHostError> {
        let media_root = self.media_root_handle().ok_or_else(|| {
            AgentHostError::Configuration("media storage is not configured".to_owned())
        })?;
        let storage_key = format!("{task_id}/{media_id}__{filename}");
        let byte_size = bytes.len() as i64;
        let checksum = hex::encode(Sha256::digest(bytes));

        if let Some(existing) = db::TaskMediaRepo::get_media_by_id(&*self.db, &media_id, true)
            .await
            .map_err(|error| AgentHostError::Runtime(error.to_string()))?
        {
            return self
                .resolve_task_evidence_receipt(
                    existing,
                    actor_identity_id,
                    task_id,
                    filename,
                    &content_type,
                    &storage_key,
                    byte_size,
                    &checksum,
                )
                .await;
        }

        let destination = safe_media_destination(&media_root, &storage_key)?;
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                AgentHostError::Runtime(format!("media storage is unavailable: {error}"))
            })?;
        }
        std::fs::write(&destination, bytes).map_err(|error| {
            AgentHostError::Runtime(format!("captured artifact could not be stored: {error}"))
        })?;

        let created = db::TaskMediaRepo::create_media(
            &*self.db,
            db::CreateTaskMedia {
                id: media_id.clone(),
                task_id: task_id.to_owned(),
                display_filename: filename.to_owned(),
                content_type: content_type.clone(),
                byte_size,
                storage_key: storage_key.clone(),
                author_type: db::CommentAuthorType::Agent,
                author_id: Some(actor_identity_id.to_owned()),
                author_name: "Agent".to_owned(),
                created_at: db::now_rfc3339(),
            },
        )
        .await;
        let created = match created {
            Ok(created) => created,
            Err(error) => {
                // A concurrent replay may have won the deterministic media
                // insert after the lookup above. Resolve that exact receipt;
                // do not turn its unique-key win into a second artifact.
                match db::TaskMediaRepo::get_media_by_id(&*self.db, &media_id, true).await {
                    Ok(Some(existing)) => existing,
                    Ok(None) => {
                        let _ = std::fs::remove_file(&destination);
                        return Err(AgentHostError::Runtime(error.to_string()));
                    }
                    Err(lookup_error) => {
                        return Err(AgentHostError::Runtime(format!(
                            "{error}; evidence receipt lookup failed: {lookup_error}"
                        )));
                    }
                }
            }
        };

        self.resolve_task_evidence_receipt(
            created,
            actor_identity_id,
            task_id,
            filename,
            &content_type,
            &storage_key,
            byte_size,
            &checksum,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn resolve_task_evidence_receipt(
        &self,
        media: db::TaskMedia,
        actor_identity_id: &str,
        task_id: &str,
        filename: &str,
        content_type: &str,
        storage_key: &str,
        byte_size: i64,
        checksum: &str,
    ) -> Result<StoredEvidence, AgentHostError> {
        if media.task_id != task_id
            || media.display_filename != filename
            || media.content_type != content_type
            || media.byte_size != byte_size
            || media.storage_key != storage_key
            || media.author_type != db::CommentAuthorType::Agent
            || media.author_id.as_deref() != Some(actor_identity_id)
            || media.deleted_at.is_some()
        {
            return Err(AgentHostError::Runtime(
                "outbox evidence receipt conflicts with the persisted artifact".to_owned(),
            ));
        }

        // Milestone evidence attachment compares the caller's checksum against
        // this column; a Task artifact without one can never back a check.
        db::SharedMediaRepo::set_media_asset_checksum(
            &*self.db,
            &media.id,
            byte_size,
            checksum,
            &db::now_rfc3339(),
        )
        .await
        .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        let asset = db::SharedMediaRepo::get_media_asset_for_task_media(&*self.db, &media.id)
            .await
            .map_err(|error| AgentHostError::Runtime(error.to_string()))?
            .ok_or_else(|| {
                AgentHostError::Runtime("captured artifact has no media asset".to_owned())
            })?;
        Ok(StoredEvidence {
            media_id: media.id,
            asset_id: asset.id,
            checksum: checksum.to_owned(),
            byte_size,
        })
    }

    /// Ingest the worklog and evidence a CLI harness wrote to its outbox.
    ///
    /// A CLI harness has no Forge tool channel, so this is its equivalent of
    /// `task.worklog` and `task.evidence`, applied when the execution ends.
    /// Every entry is validated exactly as the native tools validate it, and
    /// provenance still comes from the execution row rather than the files.
    /// A malformed entry is skipped and reported, never fatal: the run's real
    /// outcome must not hinge on its bookkeeping. An authorized plan candidate
    /// is frozen into a bounded host-owned stage before the outbox is removed;
    /// only the terminal-CAS winner may publish that exact snapshot.
    pub async fn ingest_execution_outbox(
        &self,
        input: &ExecutionOutboxInput<'_>,
    ) -> ExecutionOutboxReport {
        let mut report = ExecutionOutboxReport::default();
        let worktree = Path::new(input.worktree_path);
        let outbox = match executors::existing_execution_outbox(worktree, input.execution_id) {
            Ok(Some(outbox)) => outbox,
            Ok(None) => return report,
            Err(error) => {
                report
                    .rejected
                    .push(format!("execution outbox is unsafe: {error}"));
                return report;
            }
        };

        let author_name = input.role.unwrap_or("agent").to_owned();
        for (position, entry) in
            read_outbox_entries(&outbox, executors::OUTBOX_WORKLOG_FILE, &mut report)
        {
            let result = match entry {
                Ok(entry) => {
                    self.ingest_outbox_worklog(input, &author_name, &position, &entry)
                        .await
                }
                Err(reason) => Err(reason),
            };
            match result {
                Ok(()) => report.worklog_entries += 1,
                Err(reason) => report.rejected.push(format!(
                    "{}:{position}: {reason}",
                    executors::OUTBOX_WORKLOG_FILE
                )),
            }
        }
        for (position, entry) in
            read_outbox_entries(&outbox, executors::OUTBOX_EVIDENCE_FILE, &mut report)
        {
            let result = match entry {
                Ok(entry) => {
                    self.ingest_local_outbox_evidence(
                        input,
                        &author_name,
                        &outbox,
                        &position,
                        &entry,
                    )
                    .await
                }
                Err(reason) => Err(reason),
            };
            match result {
                Ok(()) => report.evidence_items += 1,
                Err(reason) => report.rejected.push(format!(
                    "{}:{position}: {reason}",
                    executors::OUTBOX_EVIDENCE_FILE
                )),
            }
        }

        if executors::task_role_can_write_plan(input.role) {
            match crate::plan_artifact::stage_execution_outbox_plan(
                &outbox,
                std::path::Path::new(input.worktree_path),
                input.execution_id,
            ) {
                Ok(true) => {
                    report.plan_candidate = true;
                }
                Ok(false) => {}
                Err(error) => {
                    report.plan_rejected = true;
                    report.rejected.push(format!("plan.md: {error}"));
                }
            }
        }

        // A staging failure may be transient (for example a process exited
        // during the prior no-replace install). Keep the source until the
        // terminal cascade either recovers it or settles the failure path.
        if !report.plan_rejected {
            if let Err(error) = executors::remove_execution_outbox(worktree, input.execution_id) {
                report.rejected.push(format!(
                    "outbox could not be removed after ingestion: {error}"
                ));
            }
        }
        report
    }

    async fn ingest_outbox_worklog(
        &self,
        input: &ExecutionOutboxInput<'_>,
        author_name: &str,
        position: &str,
        entry: &Value,
    ) -> Result<(), String> {
        let idempotency_key = format!("outbox:{}:worklog:{position}", input.execution_id);
        if self
            .outbox_worklog_receipt_exists(input.task_id, &idempotency_key)
            .await?
        {
            return Ok(());
        }
        let kind = entry
            .get("kind")
            .and_then(Value::as_str)
            .filter(|value| matches!(*value, "progress" | "decision" | "validation" | "blocker"))
            .ok_or("kind must be progress, decision, validation, or blocker")?;
        let summary = entry
            .get("summary")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or("summary is required")?;
        if summary.chars().count() > MAX_WORKLOG_SUMMARY_CHARS {
            return Err(format!(
                "summary exceeds the {MAX_WORKLOG_SUMMARY_CHARS} character worklog limit"
            ));
        }
        self.append_outbox_worklog(input, author_name, kind, summary, idempotency_key)
            .await
    }

    async fn ingest_local_outbox_evidence(
        &self,
        input: &ExecutionOutboxInput<'_>,
        author_name: &str,
        outbox: &Path,
        position: &str,
        entry: &Value,
    ) -> Result<(), String> {
        let idempotency_key = format!("outbox:{}:evidence:{position}", input.execution_id);
        if self
            .outbox_worklog_receipt_exists(input.task_id, &idempotency_key)
            .await?
        {
            return Ok(());
        }

        let (kind, caption) = evidence_kind_and_caption(entry)?;
        let path = entry
            .get("path")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let content = entry
            .get("content")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        let (bytes, filename, content_type) = match (path, content) {
            (Some(path), None) => {
                let (resolved, allowed_root, require_single_link) =
                    resolve_outbox_artifact(Path::new(input.worktree_path), outbox, path)?;
                let bytes = read_bounded_regular_file(
                    &resolved,
                    &allowed_root,
                    MAX_CAPTURED_EVIDENCE_BYTES as u64,
                    require_single_link,
                )?
                .ok_or("captured artifact does not exist")?;
                let name = resolved
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("artifact")
                    .to_owned();
                let content_type = content_type_for(&name, kind);
                (bytes, name, content_type)
            }
            (None, Some(content)) => (
                content.as_bytes().to_vec(),
                format!("{kind}.txt"),
                "text/plain".to_owned(),
            ),
            (Some(_), Some(_)) => return Err("supply either path or content, not both".to_owned()),
            (None, None) => {
                return Err("evidence requires either a path or inline content".to_owned());
            }
        };
        if bytes.is_empty() {
            return Err("captured artifact is empty".to_owned());
        }
        if bytes.len() as i64 > MAX_CAPTURED_EVIDENCE_BYTES {
            return Err(format!(
                "captured artifact exceeds the {MAX_CAPTURED_EVIDENCE_BYTES} byte capture limit"
            ));
        }
        self.store_task_evidence_with_id(
            input.agent_id,
            input.task_id,
            &filename,
            content_type,
            &bytes,
            outbox_evidence_media_id(input.execution_id, position),
        )
        .await
        .map_err(|error| error.to_string())?;
        // The file has no response channel to report the stored asset back
        // through, so the caption becomes the worklog line naming it.
        self.append_outbox_worklog(
            input,
            author_name,
            "validation",
            &format!("Captured {kind} evidence `{filename}`: {caption}"),
            idempotency_key,
        )
        .await
    }

    /// Apply owner-harvested entries without opening any owner-local paths.
    pub async fn ingest_execution_outbox_entries(
        &self,
        input: &ExecutionOutboxInput<'_>,
        entries: Vec<api_types::ExecutionOutboxEntry>,
    ) -> ExecutionOutboxReport {
        use api_types::{ExecutionOutboxEntry, ExecutionOutboxWorklogKind};
        let mut report = ExecutionOutboxReport::default();
        let author_name = input.role.unwrap_or("agent");
        let mut worklog_count = 0;
        let mut evidence_count = 0;
        let mut evidence_bytes = 0_u64;
        for entry in entries {
            let (file, line_no, result) = match entry {
                ExecutionOutboxEntry::Worklog {
                    position,
                    kind,
                    summary,
                } => {
                    worklog_count += 1;
                    let kind = match kind {
                        ExecutionOutboxWorklogKind::Progress => "progress",
                        ExecutionOutboxWorklogKind::Decision => "decision",
                        ExecutionOutboxWorklogKind::Validation => "validation",
                        ExecutionOutboxWorklogKind::Blocker => "blocker",
                    };
                    let result = if worklog_count > api_types::MAX_EXECUTION_OUTBOX_ENTRIES_PER_KIND
                    {
                        Err("execution outbox has too many worklog entries".to_owned())
                    } else if position.is_empty() {
                        Err("line number must be positive".to_owned())
                    } else if summary.trim().is_empty() {
                        Err("summary is required".to_owned())
                    } else if summary.trim().chars().count() > MAX_WORKLOG_SUMMARY_CHARS {
                        Err(format!("summary exceeds the {MAX_WORKLOG_SUMMARY_CHARS} character worklog limit"))
                    } else {
                        self.append_outbox_worklog(
                            input,
                            author_name,
                            kind,
                            summary.trim(),
                            format!("outbox:{}:worklog:{position}", input.execution_id),
                        )
                        .await
                    };
                    if result.is_ok() {
                        report.worklog_entries += 1;
                    }
                    (executors::OUTBOX_WORKLOG_FILE, position, result)
                }
                ExecutionOutboxEntry::Evidence {
                    position,
                    kind,
                    caption,
                    path,
                    content,
                    artifact,
                } => {
                    evidence_count += 1;
                    let size = artifact
                        .as_ref()
                        .map(|artifact| artifact.bytes.len())
                        .or_else(|| content.as_ref().map(String::len))
                        .unwrap_or(0) as u64;
                    evidence_bytes = evidence_bytes.saturating_add(size);
                    let result =
                        if evidence_count > api_types::MAX_EXECUTION_OUTBOX_ENTRIES_PER_KIND {
                            Err("execution outbox has too many evidence entries".to_owned())
                        } else if evidence_bytes > api_types::MAX_EXECUTION_OUTBOX_EVIDENCE_BYTES {
                            Err("execution outbox evidence exceeds size budget".to_owned())
                        } else {
                            self.ingest_outbox_evidence(
                                input,
                                author_name,
                                ExecutionOutboxEntry::Evidence {
                                    position: position.clone(),
                                    kind,
                                    caption,
                                    path,
                                    content,
                                    artifact,
                                },
                            )
                            .await
                        };
                    if result.is_ok() {
                        report.evidence_items += 1;
                    }
                    (executors::OUTBOX_EVIDENCE_FILE, position, result)
                }
            };
            if let Err(reason) = result {
                report.rejected.push(format!("{file}:{line_no}: {reason}"));
            }
        }
        report
    }

    async fn ingest_outbox_evidence(
        &self,
        input: &ExecutionOutboxInput<'_>,
        author_name: &str,
        entry: api_types::ExecutionOutboxEntry,
    ) -> Result<(), String> {
        use api_types::{ExecutionOutboxEntry, ExecutionOutboxEvidenceKind};
        let ExecutionOutboxEntry::Evidence {
            position: line_no,
            kind,
            caption,
            path,
            content,
            artifact,
        } = entry
        else {
            return Err("expected an evidence entry".to_owned());
        };
        let kind = match kind {
            ExecutionOutboxEvidenceKind::Screenshot => "screenshot",
            ExecutionOutboxEvidenceKind::WalkthroughVideo => "walkthrough_video",
            ExecutionOutboxEvidenceKind::Log => "log",
            ExecutionOutboxEvidenceKind::Report => "report",
            ExecutionOutboxEvidenceKind::Other => "other",
        };
        if line_no.is_empty() {
            return Err("line number must be positive".to_owned());
        }
        let caption = caption.trim();
        if caption.is_empty() {
            return Err("caption describing the artifact is required".to_owned());
        }
        let path = path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let content = content.as_deref().filter(|value| !value.trim().is_empty());
        let (bytes, filename, content_type) = match (path, content, artifact) {
            (Some(_), None, Some(artifact)) => {
                if artifact.filename.is_empty()
                    || artifact.filename.contains('/')
                    || artifact.filename.contains('\\')
                {
                    return Err("captured artifact filename is invalid".to_owned());
                }
                (artifact.bytes, artifact.filename, artifact.content_type)
            }
            (None, Some(content), None) => (
                content.as_bytes().to_vec(),
                format!("{kind}.txt"),
                "text/plain".to_owned(),
            ),
            (Some(_), Some(_), _) => {
                return Err("supply either path or content, not both".to_owned())
            }
            (Some(_), None, None) => {
                return Err("owner did not supply captured artifact bytes".to_owned())
            }
            _ => {
                return Err(
                    "evidence requires either a path with captured bytes or inline content"
                        .to_owned(),
                )
            }
        };
        if bytes.is_empty() {
            return Err("captured artifact is empty".to_owned());
        }
        if bytes.len() as i64 > MAX_CAPTURED_EVIDENCE_BYTES {
            return Err(format!(
                "captured artifact exceeds the {MAX_CAPTURED_EVIDENCE_BYTES} byte capture limit"
            ));
        }
        let idempotency_key = format!("outbox:{}:evidence:{line_no}", input.execution_id);
        let already_ingested = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM task_comment WHERE task_id = ? AND idempotency_key = ? LIMIT 1",
        )
        .bind(input.task_id)
        .bind(&idempotency_key)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|error| error.to_string())?
        .is_some();
        if already_ingested {
            return Ok(());
        }
        self.store_task_evidence_with_id(
            input.agent_id,
            input.task_id,
            &filename,
            content_type,
            &bytes,
            outbox_evidence_media_id(input.execution_id, &line_no),
        )
        .await
        .map_err(|error| error.to_string())?;
        self.append_outbox_worklog(
            input,
            author_name,
            "validation",
            &format!("Captured {kind} evidence `{filename}`: {caption}"),
            idempotency_key,
        )
        .await
    }

    async fn outbox_worklog_receipt_exists(
        &self,
        task_id: &str,
        idempotency_key: &str,
    ) -> Result<bool, String> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                 SELECT 1 FROM task_comment
                 WHERE task_id = ? AND idempotency_key = ?
             )",
        )
        .bind(task_id)
        .bind(idempotency_key)
        .fetch_one(self.db.pool())
        .await
        .map_err(|error| error.to_string())
    }

    async fn append_outbox_worklog(
        &self,
        input: &ExecutionOutboxInput<'_>,
        author_name: &str,
        kind: &str,
        summary: &str,
        idempotency_key: String,
    ) -> Result<(), String> {
        let now = db::now_rfc3339();
        let result = db::TaskCommentRepo::create_comment(
            &*self.db,
            db::CreateTaskComment {
                id: db::new_uuid_v4(),
                task_id: input.task_id.to_owned(),
                author_type: db::CommentAuthorType::Agent,
                author_id: Some(input.agent_id.to_owned()),
                author_name: author_name.to_owned(),
                content: summary.to_owned(),
                execution_id: Some(input.execution_id.to_owned()),
                role: input.role.map(str::to_owned),
                worklog_kind: Some(kind.to_owned()),
                idempotency_key: Some(idempotency_key.clone()),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(error) => {
                // A concurrent replay can win the insert after the repository's
                // lookup. Its durable receipt makes this append successful too.
                if self
                    .outbox_worklog_receipt_exists(input.task_id, &idempotency_key)
                    .await?
                {
                    Ok(())
                } else {
                    Err(error.to_string())
                }
            }
        }
    }

    async fn task_workspace(
        &self,
        task_id: &str,
    ) -> Result<crate::workspace_backend::ResolvedWorkspace, AgentHostError> {
        let workspace = db::WorkspaceRepo::get_by_task_id(&*self.db, task_id)
            .await
            .map_err(|error| AgentHostError::Runtime(error.to_string()))?
            .filter(|workspace| workspace.status != db::WorkspaceStatus::Cleaned)
            .ok_or_else(|| {
                AgentHostError::Runtime(
                    "this Task has no active workspace to capture an artifact from".to_owned(),
                )
            })?;
        let router = self
            .task_service_handle()
            .map(|service| service.workspace_backend_router())
            .ok_or_else(|| {
                AgentHostError::Runtime("workspace router is not configured".to_owned())
            })?;
        router
            .resolve(&self.db, &workspace)
            .await
            .map_err(|error| AgentHostError::Runtime(error.to_string()))
    }

    /// The Project Agent's own verification workspace root (`forge/` plus the
    /// disposable `checkout/`), as persisted on its `project_verify` scope.
    async fn project_verify_workspace_root(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
    ) -> Result<PathBuf, AgentHostError> {
        let path: Option<String> = sqlx::query_scalar(
            "SELECT workspace_path FROM agent_context_scope
             WHERE identity_id = ? AND scope_type = ? AND scope_id = ?
               AND workspace_access = 'project_verify'
             ORDER BY updated_at DESC LIMIT 1",
        )
        .bind(actor_identity_id)
        .bind(scope_type_name(scope.scope_type))
        .bind(&scope.scope_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        path.filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| {
                AgentHostError::Runtime(
                    "this session has no Project verification workspace to capture an artifact \
                     from; pass the artifact text inline as `content` instead"
                        .to_owned(),
                )
            })
    }

    /// Store an artifact the Project Agent's own verification run produced as
    /// a Project media asset, and rewrite the `capture` payload into the
    /// equivalent bounded `attach`. This is the Project-scope sibling of
    /// `task.evidence`: a Task run captures what its own execution did, while
    /// this path captures what the Agent itself observed in its workspace.
    async fn capture_project_evidence_asset(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        payload: Value,
        arguments: &Value,
    ) -> Result<Value, AgentHostError> {
        let project_id = self
            .authorization
            .project_orchestration_target(actor_identity_id, scope)
            .await
            .map_err(native_scope_error)?;
        let (kind, caption) = evidence_kind_and_caption(&payload).map_err(invalid_arguments)?;
        if payload
            .get("asset_id")
            .is_some_and(|value| !value.is_null())
            || payload
                .get("checksum")
                .is_some_and(|value| !value.is_null())
        {
            return Err(invalid_arguments(
                "capture creates the asset itself; asset_id and checksum belong to attach"
                    .to_owned(),
            ));
        }
        let path = payload
            .get("path")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let content = payload
            .get("content")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        // One source of bytes, never both: a caller supplying each would leave
        // the stored artifact ambiguous about what was actually observed.
        let (bytes, default_name, content_type) = match (path, content) {
            (Some(path), None) => {
                let root = self
                    .project_verify_workspace_root(actor_identity_id, scope)
                    .await?;
                let resolved =
                    resolve_workspace_artifact(&root, path).map_err(invalid_arguments)?;
                let bytes = read_bounded_regular_file(
                    &resolved,
                    &root,
                    MAX_CAPTURED_EVIDENCE_BYTES as u64,
                    false,
                )
                .map_err(|error| {
                    AgentHostError::Runtime(format!("captured artifact is unreadable: {error}"))
                })?
                .ok_or_else(|| {
                    AgentHostError::Runtime("captured artifact does not exist".to_owned())
                })?;
                let name = resolved
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("artifact")
                    .to_owned();
                // The Project media store accepts a closed content-type set;
                // structured-text and unknown captures are stored as plain
                // text rather than refused after the bytes were read.
                let content_type = match content_type_for(&name, kind).as_str() {
                    "application/json" | "application/octet-stream" => "text/plain".to_owned(),
                    other => other.to_owned(),
                };
                (bytes, name, content_type)
            }
            (None, Some(content)) => (
                content.as_bytes().to_vec(),
                format!("{kind}.txt"),
                "text/plain".to_owned(),
            ),
            (Some(_), Some(_)) => {
                return Err(invalid_arguments(
                    "supply either path or content, not both".to_owned(),
                ));
            }
            (None, None) => {
                return Err(invalid_arguments(
                    "evidence capture requires either a workspace path or inline content"
                        .to_owned(),
                ));
            }
        };
        if bytes.is_empty() {
            return Err(invalid_arguments("captured artifact is empty".to_owned()));
        }
        if bytes.len() as i64 > MAX_CAPTURED_EVIDENCE_BYTES {
            return Err(invalid_arguments(format!(
                "captured artifact exceeds the {MAX_CAPTURED_EVIDENCE_BYTES} byte capture limit"
            )));
        }
        let filename = payload
            .get("filename")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty() && !value.contains('/') && !value.contains('\\'))
            .map_or(default_name, str::to_owned);
        let media_root = self.media_root_handle().ok_or_else(|| {
            AgentHostError::Configuration("media storage is not configured".to_owned())
        })?;
        let project_version: Option<i64> =
            sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
                .bind(&project_id)
                .fetch_optional(self.db.pool())
                .await
                .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        let project_version = project_version.ok_or_else(|| {
            AgentHostError::Runtime("the bound Project no longer exists".to_owned())
        })?;
        let dedupe_key = required_argument(arguments, "dedupe_key")?;
        let correlation_id = required_argument(arguments, "correlation_id")?;
        let byte_size = bytes.len() as i64;
        let checksum = hex::encode(Sha256::digest(&bytes));
        let asset_id = db::new_uuid_v4();
        // The storage key matches the user upload route so one GC/tombstone
        // path covers every Project media asset.
        let storage_key = format!("projects/{project_id}/{asset_id}__{filename}");
        let destination = safe_media_destination(&media_root, &storage_key)?;
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                AgentHostError::Runtime(format!("media storage is unavailable: {error}"))
            })?;
        }
        std::fs::write(&destination, &bytes).map_err(|error| {
            AgentHostError::Runtime(format!("captured artifact could not be stored: {error}"))
        })?;
        let mutation_fingerprint = hex::encode(Sha256::digest(
            json!({
                "operation": "project.evidence.capture",
                "project_id": project_id,
                "filename": filename,
                "byte_size": byte_size,
                "checksum": checksum,
            })
            .to_string()
            .as_bytes(),
        ));
        let created = db::SharedMediaRepo::create_project_media_asset(
            &*self.db,
            db::CreateProjectMediaAsset {
                id: asset_id.clone(),
                project_id: project_id.clone(),
                display_filename: filename,
                content_type,
                byte_size,
                storage_key,
                checksum: checksum.clone(),
                idempotency_key: format!("agent-evidence-capture:{dedupe_key}"),
                mutation_fingerprint,
                expected_project_version: project_version,
                actor_type: "agent".to_owned(),
                actor_id: Some(actor_identity_id.to_owned()),
                authorization_event_id: correlation_id,
                created_at: db::now_rfc3339(),
            },
        )
        .await;
        let created = match created {
            Ok(asset) => asset,
            Err(error) => {
                let _ = std::fs::remove_file(&destination);
                return Err(AgentHostError::Runtime(error.to_string()));
            }
        };
        if created.id != asset_id {
            // Idempotent replay returned the already-stored asset; the bytes
            // written above belong to no row.
            let _ = std::fs::remove_file(&destination);
        }
        // Creation leaves the asset quarantined until its bytes are in place
        // (the user upload flow stages first). Capture wrote the final bytes
        // above, so promote to available now — the attach command refuses a
        // quarantined asset. Finalize is idempotent on replay.
        db::SharedMediaRepo::finalize_project_media_upload(
            &*self.db,
            &project_id,
            &created.id,
            &db::now_rfc3339(),
        )
        .await
        .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        let mut object = payload.as_object().cloned().unwrap_or_default();
        object.insert("action".to_owned(), json!("attach"));
        object.insert("kind".to_owned(), json!(kind));
        object.insert("caption".to_owned(), json!(caption));
        object.insert("asset_id".to_owned(), json!(created.id));
        object.insert("checksum".to_owned(), json!(checksum));
        object.remove("content");
        object.remove("path");
        object.remove("filename");
        Ok(Value::Object(object))
    }

    /// Return what Task runs actually reported: worklog entries with the
    /// execution and role that wrote them, and the artifacts those runs
    /// captured.
    ///
    /// Captured text comes back inline. That is the point of the operation: an
    /// Agent that cannot run anything still has to be able to read what the run
    /// found before it cites that run as authority, and before it decides the
    /// outcome needs a corrective Task.
    /// Whether this caller may dispatch an inquiry: the runner, the owning
    /// account and the chat the run record hangs off.
    ///
    /// `main_account_id` is what confines this to a Main Chat: it rejects a
    /// Project Chat outright and requires an Account scope's id to be the
    /// caller's own, so an inquiry can only ever be run against the account
    /// that dispatched it.
    async fn inquiry_admission(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
    ) -> Result<(Arc<dyn InquiryRunner>, String, String), AgentHostError> {
        let runner = self.inquiry_runner_handle().ok_or_else(|| {
            AgentHostError::Unsupported("inquiries are not available on this server".to_owned())
        })?;
        let account_id = self
            .authorization
            .main_account_id(actor_identity_id, scope)
            .await
            .map_err(native_scope_error)?;
        // The run record hangs off the conversation the user is watching, so
        // an inquiry is only dispatchable from a chat, never from a bare
        // Account session (which is what an inquiry sub-agent itself holds).
        match scope.scope_type {
            CanonicalScopeType::AgentChat => Ok((runner, account_id, scope.scope_id.clone())),
            _ => Err(AgentHostError::Authority(
                "inquiries are dispatched from a Main Chat".to_owned(),
            )),
        }
    }

    /// Dispatch one ephemeral inquiry sub-agent and block on its findings.
    async fn inquiry_run(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        input: operation_registry::main_reads::InquiryQuery,
    ) -> Result<Value, AgentHostError> {
        let (runner, account_id, chat_id) =
            self.inquiry_admission(actor_identity_id, scope).await?;
        let title = input.title.trim().to_owned();
        let question = input.question.trim().to_owned();
        // Preserve semantic whitespace checks and trimming in the handler.
        if title.is_empty() {
            return Err(AgentHostError::Unsupported(
                "an inquiry needs a title".to_owned(),
            ));
        }
        if question.is_empty() {
            return Err(AgentHostError::Unsupported(
                "an inquiry needs a question".to_owned(),
            ));
        }
        let context = input
            .context
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);

        let chat_id_for_log = chat_id.clone();
        let outcome = runner
            .dispatch(
                InquiryRequest {
                    chat_id,
                    // The turn job is not addressable from inside a tool
                    // call; the chat binding is what the run record needs.
                    turn_job_id: None,
                    identity_id: actor_identity_id.to_owned(),
                    account_id,
                    title,
                    question,
                    context,
                },
                tokio_util::sync::CancellationToken::new(),
            )
            .await
            .inspect_err(|error| {
                // A bare `internal_failure` with nothing in the log is not
                // debuggable, and the model can only see the safe message.
                tracing::warn!(%error, chat_id = %chat_id_for_log, "inquiry dispatch failed");
            })
            .map_err(service_error)?;

        Ok(json!({
            "inquiry_id": outcome.inquiry_id,
            "status": outcome.status.to_string(),
            "findings": outcome.findings,
            "findings_path": outcome.findings_path,
            "duration_ms": outcome.duration_ms,
            // Disjoint counters: context size is input + cache_read +
            // cache_write. They are reported separately so neither the model
            // nor the analytics rollup can double-count a cached prefix.
            "token_usage": {
                "input_tokens": outcome.input_tokens,
                "output_tokens": outcome.output_tokens,
                "cache_read_tokens": outcome.cache_read_tokens,
                "cache_write_tokens": outcome.cache_write_tokens,
            },
        }))
    }

    async fn project_observations_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        let project_id = self
            .authorization
            .project_orchestration_target(actor_identity_id, scope)
            .await
            .map_err(native_scope_error)?;
        let limit = arguments
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(20)
            .clamp(1, 50) as i64;
        let task_filter = arguments
            .get("task_id")
            .and_then(Value::as_str)
            .map(str::to_owned);

        let worklog = sqlx::query(
            "SELECT c.id, c.task_id, c.worklog_kind, c.role, c.execution_id,
                    c.content, c.created_at, t.title
             FROM task_comment c
             JOIN task t ON t.id = c.task_id
             WHERE t.project_id = ? AND t.deleted_at IS NULL
               AND c.author_type = 'agent'
               AND (? IS NULL OR c.task_id = ?)
             ORDER BY c.created_at DESC, c.id DESC LIMIT ?",
        )
        .bind(&project_id)
        .bind(task_filter.as_deref())
        .bind(task_filter.as_deref())
        .bind(limit)
        .fetch_all(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?;

        let artifacts = sqlx::query(
            "SELECT a.id AS asset_id, m.task_id, a.display_filename, a.content_type,
                    a.byte_size, a.checksum, a.storage_key, m.created_at
             FROM task_media m
             JOIN media_asset a ON a.legacy_task_media_id = m.id
             JOIN task t ON t.id = m.task_id
             WHERE t.project_id = ? AND t.deleted_at IS NULL AND m.deleted_at IS NULL
               AND (? IS NULL OR m.task_id = ?)
             ORDER BY m.created_at DESC, m.id DESC LIMIT ?",
        )
        .bind(&project_id)
        .bind(task_filter.as_deref())
        .bind(task_filter.as_deref())
        .bind(limit)
        .fetch_all(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?;

        let media_root = self.media_root_handle();
        let artifacts = artifacts
            .into_iter()
            .map(|row| {
                let content_type: String = row.try_get("content_type").unwrap_or_default();
                let byte_size: i64 = row.try_get("byte_size").unwrap_or_default();
                let storage_key: String = row.try_get("storage_key").unwrap_or_default();
                // Text is what an Agent can actually evaluate; binary artifacts
                // are named so the user can open them, never inlined.
                let content = (content_type.starts_with("text/")
                    && byte_size <= MAX_INLINE_ARTIFACT_BYTES)
                    .then_some(media_root.as_ref())
                    .flatten()
                    .and_then(|root| safe_media_destination(root, &storage_key).ok())
                    .and_then(|path| std::fs::read_to_string(path).ok())
                    .map(|text| truncate(&text, MAX_INLINE_ARTIFACT_CHARS));
                json!({
                    "asset_id": row.try_get::<String, _>("asset_id").unwrap_or_default(),
                    "task_id": row.try_get::<String, _>("task_id").unwrap_or_default(),
                    "filename": row.try_get::<String, _>("display_filename").unwrap_or_default(),
                    "content_type": content_type,
                    "byte_size": byte_size,
                    "checksum": row.try_get::<Option<String>, _>("checksum").unwrap_or_default(),
                    "captured_at": row.try_get::<String, _>("created_at").unwrap_or_default(),
                    "content": content,
                })
            })
            .collect::<Vec<_>>();

        Ok(json!({
            "scope": {"type": "project", "id": project_id},
            "worklog": worklog
                .into_iter()
                .map(|row| json!({
                    "id": row.try_get::<String, _>("id").unwrap_or_default(),
                    "task_id": row.try_get::<String, _>("task_id").unwrap_or_default(),
                    "task_title": truncate(&row.try_get::<String, _>("title").unwrap_or_default(), 160),
                    "kind": row.try_get::<Option<String>, _>("worklog_kind").unwrap_or_default(),
                    "role": row.try_get::<Option<String>, _>("role").unwrap_or_default(),
                    "execution_id": row.try_get::<Option<String>, _>("execution_id").unwrap_or_default(),
                    "summary": truncate(&row.try_get::<String, _>("content").unwrap_or_default(), 2_000),
                    "created_at": row.try_get::<String, _>("created_at").unwrap_or_default(),
                }))
                .collect::<Vec<_>>(),
            "artifacts": artifacts,
        }))
    }

    async fn project_charter_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
    ) -> Result<Value, AgentHostError> {
        let project_id = self
            .authorization
            .project_orchestration_target(actor_identity_id, scope)
            .await
            .map_err(native_scope_error)?;
        let row = sqlx::query(
            "SELECT c.id AS charter_id, c.project_mode, c.version AS charter_version,
                    r.id AS revision_id, r.revision, r.content_digest, r.rendered_digest,
                    r.rendered_view, r.content_json
             FROM project_charter AS c
             JOIN project_charter_revision AS r
               ON r.id = c.current_approved_revision_id AND r.lifecycle = 'approved'
             WHERE c.project_id = ?",
        )
        .bind(&project_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?
        .ok_or_else(|| {
            AgentHostError::Authority("the bound Project has no approved Charter".to_owned())
        })?;
        let revision_id = row.try_get::<String, _>("revision_id").unwrap_or_default();
        // The rendered Charter is prose; `review_requirement_ids` on a Task
        // proposal are exact `<revision_id>:<JSON Pointer>` strings. Without
        // the catalog the Agent has to guess array indices, and every guess is
        // rejected as an unknown requirement ID — which blocks Task creation
        // outright now that the field is required. Universal requirements are
        // omitted: they already apply to every Task and naming one is an error.
        let selectable_requirements = row
            .try_get::<String, _>("content_json")
            .ok()
            .and_then(|content| {
                serde_json::from_str::<api_types::ProjectCharterContent>(&content).ok()
            })
            .and_then(|charter| {
                ::review::contract::charter_requirements(&revision_id, &charter).ok()
            })
            .map(|requirements| {
                requirements
                    .into_iter()
                    .filter(|requirement| !requirement.universal)
                    .map(|requirement| json!({"id": requirement.id, "text": requirement.text}))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(json!({
            "scope": {"type": "project", "id": project_id},
            "charter_id": row.try_get::<String, _>("charter_id").unwrap_or_default(),
            "revision_id": revision_id,
            "revision": row.try_get::<i64, _>("revision").unwrap_or_default(),
            "charter_version": row.try_get::<i64, _>("charter_version").unwrap_or_default(),
            "project_mode": row.try_get::<String, _>("project_mode").unwrap_or_default(),
            "content_digest": row.try_get::<String, _>("content_digest").unwrap_or_default(),
            "render_digest": row.try_get::<String, _>("rendered_digest").unwrap_or_default(),
            "rendered_markdown": row.try_get::<String, _>("rendered_view").unwrap_or_default(),
            "selectable_review_requirements": selectable_requirements,
        }))
    }

    async fn project_skill_section_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        input: operation_registry::project_reads::SectionArguments,
    ) -> Result<Value, AgentHostError> {
        // Doctrine text is static server-owned content, but the read still
        // authenticates the Project binding so the operation cannot become an
        // unauthorized liveness probe for foreign scopes.
        let _project_id = self
            .authorization
            .project_orchestration_target(actor_identity_id, scope)
            .await
            .map_err(native_scope_error)?;
        let section = input.section.as_str();
        let body = crate::operating_skills::project_skill_section(section).ok_or_else(|| {
            AgentHostError::Unsupported(format!(
                "unknown doctrine section `{section}`; sections: {}",
                crate::operating_skills::PROJECT_SKILL_SECTIONS
                    .iter()
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
        Ok(json!({
            "skill_key": crate::operating_skills::PROJECT_OPERATING_SKILL_KEY,
            "section": section,
            "content": body,
        }))
    }

    async fn project_current_state_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        let project_id = self
            .authorization
            .project_orchestration_target(actor_identity_id, scope)
            .await
            .map_err(native_scope_error)?;
        let limit = arguments
            .get("limit")
            .and_then(Value::as_i64)
            .map(|value| value.clamp(1, 64));
        let projection = load_effective_project_state(&self.db, &project_id, limit)
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        let execution_setup = crate::load_project_execution_setup(&self.db, &project_id)
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        let adoption_charter = load_project_adoption_charter(&self.db, &project_id)
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        serde_json::to_value(ProjectCurrentStateResponse {
            scope: "project".to_owned(),
            effective_state: projection,
            execution_setup: Some(execution_setup),
            adoption_charter,
        })
        .map_err(|_| AgentHostError::ProtectedPersistence)
    }

    async fn project_summary_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        let account_id = self
            .authorization
            .main_account_id(actor_identity_id, scope)
            .await
            .map_err(native_scope_error)?;
        let project_id = arguments
            .get("project_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| invalid_arguments("project_id is required".to_owned()))?;
        let row = sqlx::query(
            "SELECT p.id, p.name, p.paused_at, p.created_at, p.updated_at,
                    COUNT(t.id) AS task_count
             FROM project AS p
             LEFT JOIN task AS t ON t.project_id = p.id AND t.deleted_at IS NULL
             WHERE p.id = ? AND p.owner_id = ?
             GROUP BY p.id",
        )
        .bind(project_id)
        .bind(account_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?
        .ok_or_else(|| invalid_arguments("Project summary is unavailable".to_owned()))?;
        Ok(json!({
            "id": row.try_get::<String, _>("id").unwrap_or_default(),
            "name": row.try_get::<String, _>("name").unwrap_or_default(),
            "paused": row.try_get::<Option<String>, _>("paused_at").ok().flatten().is_some(),
            "task_count": row.try_get::<i64, _>("task_count").unwrap_or_default(),
            "created_at": row.try_get::<String, _>("created_at").unwrap_or_default(),
            "updated_at": row.try_get::<String, _>("updated_at").unwrap_or_default(),
        }))
    }

    async fn read_work(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        limit: i64,
    ) -> Result<Value, AgentHostError> {
        // A Project Agent works from its Chat scope, so resolve that binding to
        // the Project whose Tasks it proposes; reading them is how it avoids
        // proposing the same work twice.
        let project_scope_id = match scope.scope_type {
            CanonicalScopeType::Project => Some(scope.scope_id.clone()),
            CanonicalScopeType::AgentChat => Some(
                self.authorization
                    .project_orchestration_target(actor_identity_id, scope)
                    .await
                    .map_err(native_scope_error)?,
            ),
            _ => None,
        };
        let rows = match (scope.scope_type, project_scope_id.as_deref()) {
            (CanonicalScopeType::Project | CanonicalScopeType::AgentChat, Some(project_id)) => {
                sqlx::query(
                    "SELECT id, parent_task_id, subtask_order, version,
                            title, status, priority, assignee_type, assignee_id,
                            condition_json
                     FROM task WHERE project_id = ? AND deleted_at IS NULL
                     ORDER BY updated_at DESC, id DESC LIMIT ?",
                )
                .bind(project_id)
                .bind(limit)
                .fetch_all(self.db.pool())
                .await
                .map_err(|_| AgentHostError::ProtectedPersistence)?
            }
            (CanonicalScopeType::Task, _) => sqlx::query(
                "SELECT id, parent_task_id, subtask_order, version,
                        title, status, priority, assignee_type, assignee_id,
                        condition_json
                     FROM task WHERE id = ? AND deleted_at IS NULL LIMIT 1",
            )
            .bind(&scope.scope_id)
            .fetch_all(self.db.pool())
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?,
            _ => {
                return Err(AgentHostError::Authority(
                    "work is not available in this canonical scope".to_owned(),
                ));
            }
        };
        let ids = rows
            .iter()
            .filter_map(|row| row.try_get::<String, _>("id").ok())
            .collect::<Vec<_>>();
        // Dependencies are what distinguish a wired chain from stray Tasks, so
        // the reader that reconciles work has to see the edges, not just rows.
        let mut dependencies: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        if !ids.is_empty() {
            let placeholders = vec!["?"; ids.len()].join(", ");
            let sql = format!(
                "SELECT task_id, depends_on_id FROM task_dependency
                 WHERE task_id IN ({placeholders}) ORDER BY task_id, depends_on_id"
            );
            let mut query = sqlx::query(&sql);
            for id in &ids {
                query = query.bind(id);
            }
            for row in query
                .fetch_all(self.db.pool())
                .await
                .map_err(|_| AgentHostError::ProtectedPersistence)?
            {
                let task_id: String = row.try_get("task_id").unwrap_or_default();
                let depends_on: String = row.try_get("depends_on_id").unwrap_or_default();
                dependencies.entry(task_id).or_default().push(depends_on);
            }
        }
        let mut latest_executions: std::collections::HashMap<String, Value> =
            std::collections::HashMap::new();
        if !ids.is_empty() {
            let placeholders = vec!["?"; ids.len()].join(", ");
            let sql = format!(
                "SELECT task_id, id, status, role, agent_session_id, logs_path
                 FROM (
                     SELECT task_id, id, status, role, agent_session_id, logs_path,
                            ROW_NUMBER() OVER (
                                PARTITION BY task_id ORDER BY created_at DESC, id DESC
                            ) AS execution_rank
                     FROM execution
                     WHERE task_id IN ({placeholders})
                 )
                 WHERE execution_rank = 1"
            );
            let mut query = sqlx::query(&sql);
            for id in &ids {
                query = query.bind(id);
            }
            for row in query
                .fetch_all(self.db.pool())
                .await
                .map_err(|_| AgentHostError::ProtectedPersistence)?
            {
                let task_id: String = row.try_get("task_id").unwrap_or_default();
                let execution_id: String = row.try_get("id").unwrap_or_default();
                let status: String = row.try_get("status").unwrap_or_default();
                let role: String = row.try_get("role").unwrap_or_default();
                let agent_session_id = row
                    .try_get::<Option<String>, _>("agent_session_id")
                    .ok()
                    .flatten()
                    .map(|value| truncate(&value, 256));
                let logs_path = row
                    .try_get::<Option<String>, _>("logs_path")
                    .ok()
                    .flatten()
                    .map(|value| truncate(&value, 512));
                latest_executions.insert(
                    task_id,
                    json!({
                        "execution_id": execution_id,
                        "status": status,
                        "role": role,
                        "agent_session_id": agent_session_id,
                        "logs_path": logs_path,
                    }),
                );
            }
        }
        let mut offers_by_task = std::collections::HashMap::new();
        if let Some(service) = self.task_service_handle() {
            for id in &ids {
                let offers = service
                    .task_action_offers(id, &api_types::Actor::agent(actor_identity_id))
                    .await
                    .map_err(service_error)?;
                offers_by_task.insert(id.clone(), offers.available_actions);
            }
        }
        let items = rows
            .into_iter()
            .map(|row| {
                let id = row.try_get::<String, _>("id").unwrap_or_default();
                let depends_on = dependencies.remove(&id).unwrap_or_default();
                let condition = row.try_get::<String, _>("condition_json").ok()
                    .map(|raw| db::task_condition::decode_or_unknown(&raw).public());
                json!({
                    "id": id,
                    "parent_task_id": row.try_get::<Option<String>, _>("parent_task_id").ok().flatten(),
                    "subtask_order": row.try_get::<Option<i64>, _>("subtask_order").ok().flatten(),
                    "version": row.try_get::<i64, _>("version").unwrap_or_default(),
                    "title": row.try_get::<String, _>("title").unwrap_or_default(),
                    "status": row.try_get::<String, _>("status").unwrap_or_default(),
                    "priority": row.try_get::<i64, _>("priority").unwrap_or_default(),
                    "assignee_type": row.try_get::<Option<String>, _>("assignee_type").ok().flatten(),
                    "assignee_id": row.try_get::<Option<String>, _>("assignee_id").ok().flatten(),
                    "condition": condition,
                    "depends_on": depends_on,
                    "available_actions": offers_by_task.remove(&id).unwrap_or_default(),
                    "latest_execution": latest_executions.remove(&id).unwrap_or(Value::Null),
                })
            })
            .collect::<Vec<_>>();
        // `task.adaptive` requires `expected_board_revision`; this read is the
        // only place a Project Agent can learn it without a failed command.
        let board_revision = match project_scope_id.as_deref() {
            Some(project_id) => Some(
                TaskBoardRepo::board_revision(&*self.db, project_id)
                    .await
                    .map_err(|_| AgentHostError::ProtectedPersistence)?,
            ),
            None => None,
        };
        Ok(json!({"board_revision": board_revision, "items": items}))
    }

    async fn read_events(
        &self,
        scope: &CanonicalScope,
        limit: i64,
    ) -> Result<Value, AgentHostError> {
        let rows = sqlx::query(
            "SELECT sequence, id, event_type, entity_type, entity_id, actor_type,
                    correlation_id, causation_id, causation_depth, created_at
             FROM domain_event
             WHERE scope_type = ? AND scope_id = ?
             ORDER BY sequence DESC LIMIT ?",
        )
        .bind(scope_type_name(scope.scope_type))
        .bind(&scope.scope_id)
        .bind(limit)
        .fetch_all(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?;
        let items = rows
            .into_iter()
            .map(|row| {
                json!({
                    "sequence": row.try_get::<i64, _>("sequence").unwrap_or_default(),
                    "id": row.try_get::<String, _>("id").unwrap_or_default(),
                    "event_type": row.try_get::<String, _>("event_type").unwrap_or_default(),
                    "entity_type": row.try_get::<String, _>("entity_type").unwrap_or_default(),
                    "entity_id": row.try_get::<String, _>("entity_id").unwrap_or_default(),
                    "actor_type": row.try_get::<String, _>("actor_type").unwrap_or_default(),
                    "correlation_id": row.try_get::<String, _>("correlation_id").unwrap_or_default(),
                    "causation_id": row.try_get::<Option<String>, _>("causation_id").ok().flatten(),
                    "causation_depth": row.try_get::<i64, _>("causation_depth").unwrap_or_default(),
                    "created_at": row.try_get::<String, _>("created_at").unwrap_or_default(),
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({"items": items}))
    }

    async fn read_inbox(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        limit: i64,
    ) -> Result<Value, AgentHostError> {
        let items = AgentInboxRepo::list_inbox_items(
            &*self.db,
            AgentInboxListQuery {
                recipient_identity_id: actor_identity_id.to_owned(),
                status: None,
                scope_type: Some(scope_type_name(scope.scope_type).to_owned()),
                scope_id: Some(scope.scope_id.clone()),
                limit,
            },
        )
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?;
        Ok(json!({
            "items": items.into_iter().map(|item| json!({
                "id": item.id,
                "kind": item.kind.to_string(),
                "status": item.status.to_string(),
                "title": truncate(&item.title, 256),
                "source_type": item.source_type,
                "source_id": item.source_id,
                "correlation_id": item.correlation_id,
                "version": item.version,
                "created_at": item.created_at,
            })).collect::<Vec<_>>()
        }))
    }

    async fn read_commitments(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        limit: i64,
    ) -> Result<Value, AgentHostError> {
        let items = AgentCommitmentRepo::list_commitments(
            &*self.db,
            AgentCommitmentListQuery {
                owner_identity_id: Some(actor_identity_id.to_owned()),
                scope_type: Some(scope_type_name(scope.scope_type).to_owned()),
                scope_id: Some(scope.scope_id.clone()),
                status: None,
                limit,
            },
        )
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?;
        Ok(json!({
            "items": items.into_iter().map(|item| json!({
                "id": item.id,
                "title": truncate(&item.title, 256),
                "status": item.status.to_string(),
                "due_at": item.due_at,
                "originating_task_id": item.originating_task_id,
                "evidence_required": item.evidence_required,
                "blocked_reason": item.blocked_reason.map(|reason| truncate(&reason, 256)),
                "version": item.version,
            })).collect::<Vec<_>>()
        }))
    }

    async fn read_delivery(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        limit: i64,
    ) -> Result<Value, AgentHostError> {
        let inbox = AgentInboxRepo::list_inbox_items(
            &*self.db,
            AgentInboxListQuery {
                recipient_identity_id: actor_identity_id.to_owned(),
                status: None,
                scope_type: Some(scope_type_name(scope.scope_type).to_owned()),
                scope_id: Some(scope.scope_id.clone()),
                limit,
            },
        )
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?;
        let actions = AgentActionRepo::list_actions(
            &*self.db,
            AgentActionListQuery {
                actor_identity_id: Some(actor_identity_id.to_owned()),
                scope_type: Some(scope_type_name(scope.scope_type).to_owned()),
                scope_id: Some(scope.scope_id.clone()),
                status: None,
                limit,
            },
        )
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?;
        Ok(json!({
            "inbox": inbox.into_iter().filter(|item| matches!(&item.kind, db::AgentInboxKind::TaskOutcome | db::AgentInboxKind::ActionResult)).map(|item| json!({
                "id": item.id,
                "kind": item.kind.to_string(),
                "status": item.status.to_string(),
                "title": truncate(&item.title, 256),
                "source_id": item.source_id,
                "created_at": item.created_at,
            })).collect::<Vec<_>>(),
            "actions": actions.into_iter().map(|action| json!({
                "id": action.id,
                "operation": action.operation,
                "status": action.status.to_string(),
                "policy_result": action.policy_result.to_string(),
                "target_type": action.target_type,
                "target_id": action.target_id,
                "version": action.version,
                "created_at": action.created_at,
            })).collect::<Vec<_>>(),
        }))
    }

    async fn propose(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        runtime_session_id: &str,
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        let payload = arguments
            .get("payload")
            .filter(|value| value.is_object())
            .cloned()
            .ok_or_else(|| {
                // The schema requires `payload`, so a call that omits it
                // (and supplies no flat fields to lift into one) is refused
                // by schema validation before it reaches this code. The
                // property is still declared nullable (a cross-provider
                // compatibility choice), so an explicit `"payload": null`
                // does arrive here. Naming the expected shape is the only
                // thing that lets the model correct the call; the bare
                // message left the Agent with a non-retryable dead end.
                let operation_hint = arguments
                    .get("operation")
                    .and_then(Value::as_str)
                    .unwrap_or("<operation>");
                AgentHostError::Unsupported(format!(
                    "proposal payload must be an object: put this operation's own fields \
                     inside a `payload` object, as {{\"operation\": \"{operation_hint}\", \
                     \"payload\": {{ ...fields... }}, \"dedupe_key\": \"...\", \
                     \"correlation_id\": \"...\"}}. The `payload` property description \
                     lists the fields each operation takes."
                ))
            })?;
        let descriptor = operation_descriptor(scope.scope_type, operation, Some(&payload));
        let classification = descriptor.classification;
        match classification {
            OperationClassification::Query => {
                return Err(AgentHostError::Unsupported(
                    "read-only Forge operations execute through the query tool".to_owned(),
                ));
            }
            OperationClassification::Denied => {
                return Err(AgentHostError::StructuredOutcome(Box::new(
                    OrchestrationOutcome::terminal_denial(
                        operation,
                        outcome_scope(scope),
                        correlation_id(&arguments, operation, scope),
                        DeniedBy::OperationNotInScope,
                    ),
                )));
            }
            OperationClassification::DirectCommand
            | OperationClassification::ApprovalRequiredAction => {}
        }
        validate_proposal_payload(operation, &payload).map_err(|_| {
            AgentHostError::Unsupported(
                "proposal payload does not match the typed operation schema".to_owned(),
            )
        })?;
        if operation == MAIN_GENESIS_START_OPERATION {
            return self
                .execute_main_genesis_start(actor_identity_id, scope, arguments, payload)
                .await;
        }
        if operation == MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION {
            return self
                .execute_main_genesis_project_agent_select(
                    actor_identity_id,
                    scope,
                    arguments,
                    payload,
                )
                .await;
        }
        if operation == MAIN_CHARTER_DRAFT_OPERATION {
            return self
                .execute_main_genesis_charter_draft(actor_identity_id, scope, arguments, payload)
                .await;
        }
        if operation == TASK_WORKLOG_OPERATION {
            return self
                .execute_task_worklog_append(actor_identity_id, scope, &arguments, &payload)
                .await;
        }
        if operation == TASK_EVIDENCE_OPERATION {
            return self
                .execute_task_evidence_capture(actor_identity_id, scope, &payload)
                .await;
        }
        if operation == TASK_PLAN_OPERATION {
            return self
                .execute_task_plan_write(actor_identity_id, runtime_session_id, scope, &payload)
                .await;
        }
        // `project.evidence` capture stores the artifact the Project Agent's
        // own verification run produced as a Project media asset, then
        // continues as the bounded attach it now is. The asset exists before
        // the attach command runs; an attach failure leaves an unreferenced
        // Project asset for media GC, never a dangling attachment.
        let payload = if operation == PROJECT_EVIDENCE_OPERATION
            && payload.get("action").and_then(Value::as_str) == Some("capture")
        {
            self.capture_project_evidence_asset(actor_identity_id, scope, payload, &arguments)
                .await?
        } else {
            payload
        };
        if classification == OperationClassification::DirectCommand {
            let requested_permission = descriptor.required_permission.ok_or_else(|| {
                AgentHostError::Authority(
                    "direct command has no canonical permission descriptor".to_owned(),
                )
            })?;
            // Both halves are read off the catalog: a coordination command
            // admitted under this permission, or a Project-orchestration
            // operation. Re-listing the coordination names here was the
            // second place a new operation had to be registered, and missing
            // it failed with "no canonical target derivation" — a message
            // that names neither the operation nor the list.
            let target_id = if forge_agent_host::is_coordination_direct_command(
                operation,
                requested_permission,
            ) || forge_agent_host::is_project_orchestration_operation(operation)
            {
                Some(
                    self.authorization
                        .direct_project_target(scope)
                        .await
                        .map_err(native_scope_error)?,
                )
            } else {
                return Err(AgentHostError::Authority(
                    "direct command has no canonical target derivation".to_owned(),
                ));
            };
            let dedupe_key = required_argument(&arguments, "dedupe_key")?;
            let correlation_id = required_argument(&arguments, "correlation_id")?;
            let causation_id = arguments
                .get("causation_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let causation_depth = arguments
                .get("causation_depth")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            return self
                .execute_direct_command(
                    actor_identity_id,
                    scope,
                    operation,
                    payload,
                    requested_permission,
                    target_id,
                    dedupe_key,
                    correlation_id,
                    causation_id,
                    causation_depth,
                )
                .await;
        }
        let project_chat_target = if operation == TASK_PROPOSE_OPERATION
            && scope.scope_type == CanonicalScopeType::AgentChat
        {
            Some(
                self.project_chat_task_target(actor_identity_id, scope)
                    .await?,
            )
        } else {
            None
        };
        // Generic coordination mutations are not an alternate Main/account
        // authority path.  They are admitted only for a bound Project (or a
        // Task reviewer for review requests), and every Project mutation is
        // blocked until a user-approved Charter adoption is current.  The
        // setup exception is the bounded message channel plus the typed
        // adoption operation handled below.
        match operation {
            "message.propose" | "message.send" => {
                let _ = self
                    .authorization
                    .project_orchestration_target(actor_identity_id, scope)
                    .await
                    .map_err(native_scope_error)?;
            }
            "commitment.propose" | "commitment.update" | "memory.publish" | "memory.supersede"
            | "session.action" | "review.propose" | "review.request"
                if scope.scope_type != CanonicalScopeType::Task =>
            {
                let _ = self
                    .authorization
                    .project_orchestration_target(actor_identity_id, scope)
                    .await
                    .map_err(native_scope_error)?;
            }
            _ => {}
        }
        let requested_permission =
            operation_permission(scope.scope_type, operation).ok_or_else(|| {
                AgentHostError::Authority(
                    "proposal operation has no canonical permission descriptor".to_owned(),
                )
            })?;
        let (target_type, target_id) = match operation {
            MAIN_PROJECT_CREATE_OPERATION => {
                let account_id = self
                    .authorization
                    .main_account_id(actor_identity_id, scope)
                    .await
                    .map_err(native_scope_error)?;
                (Some("account".to_owned()), Some(account_id))
            }
            PROJECT_CHARTER_ADOPTION_OPERATION => {
                let project_id = self
                    .authorization
                    .project_orchestration_target(actor_identity_id, scope)
                    .await
                    .map_err(native_scope_error)?;
                (Some("project".to_owned()), Some(project_id))
            }
            PROJECT_DOCUMENT_OPERATION
            | PROJECT_MILESTONE_OPERATION
            | PROJECT_EVIDENCE_OPERATION
            | PROJECT_VALIDATION_OPERATION
            | PROJECT_READINESS_OPERATION
            | PROJECT_RELEASE_OPERATION => {
                let project_id = self
                    .authorization
                    .project_orchestration_target(actor_identity_id, scope)
                    .await
                    .map_err(native_scope_error)?;
                (Some("project".to_owned()), Some(project_id))
            }
            PROJECT_DECISION_OPERATION => {
                let project_id = self
                    .authorization
                    .project_orchestration_target(actor_identity_id, scope)
                    .await
                    .map_err(native_scope_error)?;
                (Some("project".to_owned()), Some(project_id))
            }
            "message.propose" | "message.send" => (
                Some(scope_type_name(scope.scope_type).to_owned()),
                Some(scope.scope_id.clone()),
            ),
            TASK_PROPOSE_OPERATION if scope.scope_type == CanonicalScopeType::Project => {
                (Some("project".to_owned()), Some(scope.scope_id.clone()))
            }
            TASK_PROPOSE_OPERATION if project_chat_target.is_some() => {
                let project_id = project_chat_target.as_deref().ok_or_else(|| {
                    AgentHostError::Authority("Project Agent Chat has no owning Project".to_owned())
                })?;
                let _ = project_id;
                (Some("project".to_owned()), project_chat_target)
            }
            "review.propose" | "review.request"
                if matches!(
                    scope.scope_type,
                    CanonicalScopeType::Project
                        | CanonicalScopeType::AgentChat
                        | CanonicalScopeType::Task
                ) && (scope.scope_type != CanonicalScopeType::Task
                    || scope.workspace_access == WorkspaceAccess::TaskRead) =>
            {
                (
                    Some(scope_type_name(scope.scope_type).to_owned()),
                    Some(scope.scope_id.clone()),
                )
            }
            "commitment.propose" | "commitment.update" => (
                Some(scope_type_name(scope.scope_type).to_owned()),
                Some(scope.scope_id.clone()),
            ),
            "memory.publish" | "memory.supersede" => (
                Some(scope_type_name(scope.scope_type).to_owned()),
                Some(scope.scope_id.clone()),
            ),
            "session.action"
                if matches!(
                    scope.scope_type,
                    CanonicalScopeType::Account
                        | CanonicalScopeType::Project
                        | CanonicalScopeType::AgentChat
                ) =>
            {
                (Some("scope".to_owned()), Some(scope.scope_id.clone()))
            }
            _ => {
                return Err(AgentHostError::Authority(
                    "proposal operation is not admitted for this scope".into(),
                ));
            }
        };
        let dedupe_key = required_argument(&arguments, "dedupe_key")?;
        let correlation_id = required_argument(&arguments, "correlation_id")?;
        let causation_id = arguments
            .get("causation_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let causation_depth = arguments
            .get("causation_depth")
            .and_then(Value::as_i64)
            .unwrap_or(0);

        let action = self
            .actions
            .propose(ProposeActionInput {
                id: None,
                actor_identity_id: actor_identity_id.to_owned(),
                scope_type: scope_type_name(scope.scope_type).to_owned(),
                scope_id: scope.scope_id.clone(),
                operation: operation.to_owned(),
                payload_json: payload.to_string(),
                dedupe_key,
                correlation_id,
                causation_id,
                causation_depth,
                requested_permission: requested_permission.to_owned(),
                policy_reason: None,
                target_type,
                target_id,
            })
            .await
            .map_err(service_error)?;
        if action.policy_result == AgentActionPolicyResult::Denied {
            return Err(AgentHostError::Authority(
                action
                    .policy_reason
                    .clone()
                    .unwrap_or_else(|| DeniedBy::Unspecified.to_string()),
            ));
        }
        let mut response = action_value(&action);
        if operation_contract(operation).is_some() {
            // A proposal row is not a domain success. Protected Main
            // Project creation and all Project-local operations remain
            // explicitly pending until their typed executor/user transaction
            // runs.
            if let Some(object) = response.as_object_mut() {
                object.insert("materialized".to_owned(), Value::Bool(false));
                object.insert("domain_committed".to_owned(), Value::Bool(false));
                object.insert("domain_result".to_owned(), Value::Null);
                object.insert(
                    "requires_user_authorization".to_owned(),
                    Value::Bool(operation == MAIN_PROJECT_CREATE_OPERATION),
                );
            }
        }
        Ok(response)
    }

    /// Route an operation that the canonical host catalog has already
    /// classified as a direct command.  The adapter only supplies the
    /// server-derived source scope and policy envelope; Task/Project command
    /// services own authorization, validation, receipt replay, and domain
    /// persistence.  No branch here creates an `AgentAction` row.
    #[allow(clippy::too_many_arguments)]
    async fn execute_direct_command(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
        payload: Value,
        requested_permission: &str,
        target_id: Option<String>,
        idempotency_key: String,
        correlation_id: String,
        causation_id: Option<String>,
        causation_depth: i64,
    ) -> Result<Value, AgentHostError> {
        if operation == PROJECT_ESCALATE_OPERATION {
            let project_id = target_id.ok_or_else(|| {
                AgentHostError::Authority("escalation requires a bound Project".to_owned())
            })?;
            let policy_payload = payload.to_string();
            let (policy, reason) = self
                .actions
                .evaluate_direct_command_policy(
                    actor_identity_id,
                    scope_type_name(scope.scope_type),
                    &scope.scope_id,
                    requested_permission,
                    operation,
                    Some(&policy_payload),
                )
                .await
                .map_err(service_error)?;
            if !matches!(policy, AgentActionPolicyResult::Allowed) {
                return Err(AgentHostError::Authority(
                    reason.unwrap_or_else(|| "escalation denied".to_owned()),
                ));
            }
            let request: api_types::ProjectEscalateRequest =
                serde_json::from_value(payload).map_err(|e| invalid_arguments(e.to_string()))?;
            let result =
                crate::project_escalation::ProjectEscalationService::new(Arc::clone(&self.db))
                    .escalate(
                        &project_id,
                        crate::project_escalation::EscalationAuthority::Agent(actor_identity_id),
                        request,
                        &idempotency_key,
                    )
                    .await
                    .map_err(service_error)?;
            return Ok(
                json!({"operation":operation,"status":"succeeded","materialized":true,"domain_committed":true,"domain_result":result,"correlation_id":correlation_id,"requires_user_authorization":false}),
            );
        }
        if operation == TASK_ACTION_OPERATION {
            let task_service = self.task_service_handle().ok_or_else(|| {
                AgentHostError::Configuration("Task actions are not wired".to_owned())
            })?;
            let project_id = target_id.ok_or_else(|| {
                AgentHostError::Authority("Task action has no canonical Project".to_owned())
            })?;
            let task_id = payload
                .get("task_id")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid_arguments("task_id is required".to_owned()))?;
            let task = db::TaskRepo::get_by_id(&*self.db, task_id, false)
                .await
                .map_err(|error| AgentHostError::Runtime(error.to_string()))?
                .filter(|task| task.project_id == project_id)
                .ok_or_else(|| {
                    invalid_arguments("task_id must name a Task in this Project".to_owned())
                })?;
            let request: api_types::TaskActionRequest = serde_json::from_value(
                json!({ "action": payload.get("action"), "version": payload.get("version") }),
            )
            .map_err(|error| invalid_arguments(error.to_string()))?;
            let (policy, reason) = self
                .actions
                .evaluate_direct_command_policy(
                    actor_identity_id,
                    scope_type_name(scope.scope_type),
                    &scope.scope_id,
                    requested_permission,
                    operation,
                    None,
                )
                .await
                .map_err(service_error)?;
            if !matches!(policy, AgentActionPolicyResult::Allowed) {
                return Err(AgentHostError::Authority(
                    reason.unwrap_or_else(|| DeniedBy::Unspecified.to_string()),
                ));
            }
            let unblocking = crate::project_escalation::is_unblocking_verb(request.action.verb());
            let result = task_service
                .perform_task_action_as(
                    task.id,
                    request.action,
                    request.version,
                    api_types::Actor::agent(actor_identity_id),
                )
                .await
                .map_err(service_error)?;
            if unblocking {
                crate::project_escalation::ProjectEscalationService::new(Arc::clone(&self.db))
                    .record_unblocking_action(&project_id, actor_identity_id, &result.task.id)
                    .await
                    .map_err(service_error)?;
            }
            let offers = task_service
                .task_action_offers(&result.task.id, &api_types::Actor::agent(actor_identity_id))
                .await
                .map_err(service_error)?;
            return Ok(
                json!({ "operation": operation, "status": "succeeded", "replayed": false, "materialized": true, "domain_committed": true, "correlation_id": correlation_id, "task_id": result.task.id, "task_status": result.task.status, "task_version": result.task.version, "available_actions": offers.available_actions, "requires_user_authorization": false }),
            );
        }

        if operation == TASK_DEPENDENCY_OPERATION {
            let Some(task_service) = self.task_service_handle() else {
                return Err(AgentHostError::Configuration(
                    "Task dependency execution is not wired to a TaskService".to_owned(),
                ));
            };
            let project_id = target_id.ok_or_else(|| {
                AgentHostError::Authority(
                    "Task dependency command has no server-derived Project target".to_owned(),
                )
            })?;
            let payload: TaskDependencyPayload =
                typed_command_payload(operation, scope, &correlation_id, payload)?;
            let (task_id, depends_on_task_id, action, _rationale) = payload.into_parts();
            let (policy_result, policy_reason) = self
                .actions
                .evaluate_direct_command_policy(
                    actor_identity_id,
                    scope_type_name(scope.scope_type),
                    &scope.scope_id,
                    requested_permission,
                    operation,
                    None,
                )
                .await
                .map_err(service_error)?;
            if !matches!(policy_result, AgentActionPolicyResult::Allowed) {
                tracing::warn!(
                    operation,
                    diagnostic = policy_reason.as_deref().unwrap_or("no reason recorded"),
                    "Task dependency command policy denied"
                );
                return Err(AgentHostError::Authority(
                    policy_reason.unwrap_or_else(|| DeniedBy::Unspecified.to_string()),
                ));
            }
            let task = task_service
                .perform_project_agent_dependency(
                    &project_id,
                    &task_id,
                    &depends_on_task_id,
                    action,
                )
                .await
                .map_err(service_error)?;
            return Ok(json!({
                "operation": operation,
                "status": "succeeded",
                "replayed": false,
                "materialized": true,
                "domain_committed": true,
                "correlation_id": correlation_id,
                "task_id": task.id,
                "task_status": task.status,
                "task_version": task.version,
                "condition": task.condition.public(),
                "requires_user_authorization": false,
            }));
        }

        if operation == TASK_ADAPTIVE_OPERATION {
            let Some(task_service) = self.task_service_handle() else {
                return Err(AgentHostError::Configuration(
                    "Adaptive Task execution is not wired to a TaskService".to_owned(),
                ));
            };
            let project_id = target_id.ok_or_else(|| {
                AgentHostError::Authority(
                    "adaptive Task command has no server-derived Project target".to_owned(),
                )
            })?;
            let adaptive_payload: AdaptiveTaskPayload =
                typed_command_payload(operation, scope, &correlation_id, payload.clone())?;
            let (
                source_task_id,
                expected_task_version,
                expected_board_revision,
                adaptive_operation,
                rationale,
            ) = adaptive_payload.into_command_parts();
            let receipt_exists = self
                .adaptive_receipt_exists(actor_identity_id, &project_id, &idempotency_key)
                .await?;
            let (policy_result, policy_reason) = if receipt_exists {
                (AgentActionPolicyResult::Allowed, None)
            } else {
                self.actions
                    .evaluate_direct_command_policy(
                        actor_identity_id,
                        scope_type_name(scope.scope_type),
                        &scope.scope_id,
                        requested_permission,
                        operation,
                        Some(&payload.to_string()),
                    )
                    .await
                    .map_err(service_error)?
            };
            if !matches!(policy_result, AgentActionPolicyResult::Allowed) {
                return Err(AgentHostError::Authority(
                    policy_reason.unwrap_or_else(|| DeniedBy::Unspecified.to_string()),
                ));
            }
            let result: AdaptiveTaskCommandResult = task_service
                .execute_adaptive_task_command(AdaptiveTaskCommand {
                    project_id,
                    source_task_id,
                    expected_task_version,
                    expected_board_revision,
                    operation: adaptive_operation,
                    rationale,
                    actor_type: "agent".to_owned(),
                    actor_id: actor_identity_id.to_owned(),
                    policy_result: "allowed".to_owned(),
                    policy_revision: None,
                    policy_digest: None,
                    requested_permission: Some(requested_permission.to_owned()),
                    idempotency_key,
                    correlation_id,
                    causation_id,
                    causation_depth,
                })
                .await
                .map_err(service_error)?;
            let task_ids = result
                .tasks
                .iter()
                .map(|task| Value::String(task.id.clone()))
                .collect::<Vec<_>>();
            return Ok(json!({
                "operation": operation,
                "status": "succeeded",
                "replayed": result.replayed,
                "materialized": true,
                "domain_committed": true,
                "receipt_id": result.receipt.id,
                "event_id": result.receipt.event_id,
                "input_digest": result.receipt.input_digest,
                "policy_result": result.receipt.policy_result,
                "correlation_id": result.receipt.correlation_id,
                "source_task_id": result.source_task.id,
                "task_ids": task_ids,
                "board_revision": result.board_revision,
                "domain_result": {
                    "source_task_id": result.source_task.id,
                    "task_ids": result.tasks.iter().map(|task| task.id.clone()).collect::<Vec<_>>(),
                    "board_revision": result.board_revision,
                },
                "action_id": Value::Null,
                "agent_action_execution_id": Value::Null,
                "requires_user_authorization": false,
            }));
        }

        if operation == TASK_PROPOSE_OPERATION {
            let Some(task_service) = self.task_service_handle() else {
                return Err(AgentHostError::Configuration(
                    "Task proposal execution is not wired to a TaskService".to_owned(),
                ));
            };
            let project_id = target_id.ok_or_else(|| {
                AgentHostError::Authority(
                    "direct task proposal has no server-derived Project target".to_owned(),
                )
            })?;
            let payload: TaskProposalPayload =
                typed_command_payload(operation, scope, &correlation_id, payload.clone())?;
            let payload_json = serde_json::to_string(&payload).map_err(|_| {
                AgentHostError::Unsupported("task proposal payload is invalid".to_owned())
            })?;
            let (policy_result, reason) = self
                .actions
                .evaluate_direct_command_policy(
                    actor_identity_id,
                    scope_type_name(scope.scope_type),
                    &scope.scope_id,
                    requested_permission,
                    operation,
                    Some(&payload_json),
                )
                .await
                .map_err(service_error)?;
            let result: TaskProposalCommandResult = task_service
                .execute_task_proposal_direct(DirectTaskProposalInput {
                    actor_identity_id: actor_identity_id.to_owned(),
                    executor_type: "agent".to_owned(),
                    executor_id: actor_identity_id.to_owned(),
                    source_scope_type: scope_type_name(scope.scope_type).to_owned(),
                    source_scope_id: scope.scope_id.clone(),
                    project_id,
                    payload,
                    idempotency_key,
                    correlation_id,
                    causation_id,
                    causation_depth,
                    policy_result: "allowed".to_owned(),
                    preflight_policy_result: Some(policy_result.to_string()),
                    preflight_policy_reason: reason,
                    policy_revision: None,
                    policy_digest: None,
                    requested_permission: requested_permission.to_owned(),
                })
                .await
                .map_err(service_error)?;
            return Ok(json!({
                "operation": operation,
                "status": "succeeded",
                "replayed": result.replayed,
                "materialized": true,
                "domain_committed": true,
                "receipt_id": result.receipt.id,
                "event_id": result.receipt.event_id,
                "input_digest": result.receipt.input_digest,
                "policy_result": result.receipt.policy_result,
                "correlation_id": result.receipt.correlation_id,
                "domain_result": {
                    "task_id": result.task.id,
                    "task_status": result.task.status,
                },
                "agent_action_execution_id": Value::Null,
                "requires_user_authorization": false,
            }));
        }

        if forge_agent_host::is_project_orchestration_operation(operation) {
            let project_id = target_id.ok_or_else(|| {
                AgentHostError::Authority(
                    "direct Project command has no server-derived Project target".to_owned(),
                )
            })?;
            let result = self
                .project_actions
                .execute_direct(ExecuteDirectProjectCommandInput {
                    actor_identity_id: actor_identity_id.to_owned(),
                    scope_type: scope_type_name(scope.scope_type).to_owned(),
                    scope_id: scope.scope_id.clone(),
                    project_id,
                    operation: operation.to_owned(),
                    payload,
                    idempotency_key,
                    correlation_id,
                    causation_id,
                    causation_depth,
                    requested_permission: requested_permission.to_owned(),
                })
                .await
                .map_err(service_error)?;
            let requires_user_authorization = result
                .result
                .get("requires_user_authorization")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            return Ok(json!({
                "operation": result.operation,
                "status": "succeeded",
                "replayed": result.replayed,
                "materialized": true,
                "domain_committed": true,
                "receipt_id": result.receipt_id,
                "event_id": result.event_id,
                "domain_result": result.result,
                "agent_action_execution_id": result.agent_action_execution_id,
                "requires_user_authorization": requires_user_authorization,
            }));
        }

        Err(AgentHostError::Authority(
            "direct command has no shared service boundary".to_owned(),
        ))
    }

    /// Execute the directly admitted Main Charter draft without touching the
    /// AgentAction queue.  Policy is evaluated through the same service
    /// ceiling as proposals; the typed Main/Genesis command owns all domain
    /// validation, receipt creation, and replay behavior.
    async fn execute_main_genesis_charter_draft(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
        mut payload: Value,
    ) -> Result<Value, AgentHostError> {
        // `action` is the transport-level operation discriminator.  The
        // command service owns the typed Charter request and all domain
        // validation; do not duplicate an action/payload schema here.
        if let Some(object) = payload.as_object_mut() {
            object.remove("action");
        }
        let request: MainGenesisCharterDraftRequest = typed_command_payload(
            MAIN_CHARTER_DRAFT_OPERATION,
            scope,
            &correlation_id(&arguments, MAIN_CHARTER_DRAFT_OPERATION, scope),
            payload.clone(),
        )?;
        let dedupe_key = required_argument(&arguments, "dedupe_key")?;
        let correlation_id = required_argument(&arguments, "correlation_id")?;
        let causation_id = arguments
            .get("causation_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let causation_depth = arguments
            .get("causation_depth")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let requested_permission =
            operation_permission(scope.scope_type, MAIN_CHARTER_DRAFT_OPERATION).ok_or_else(
                || {
                    AgentHostError::Authority(
                        "charter draft has no canonical permission descriptor".to_owned(),
                    )
                },
            )?;
        let (policy_result, policy_reason) = self
            .actions
            .evaluate_direct_command_policy(
                actor_identity_id,
                scope_type_name(scope.scope_type),
                &scope.scope_id,
                requested_permission,
                MAIN_CHARTER_DRAFT_OPERATION,
                Some(&payload.to_string()),
            )
            .await
            .map_err(service_error)?;
        if policy_result != AgentActionPolicyResult::Allowed {
            let reason = policy_reason.unwrap_or_else(|| {
                "Main Charter draft policy did not admit this command".to_owned()
            });
            tracing::warn!(diagnostic = %reason, "charter.draft policy denied");
            return Err(AgentHostError::Authority(reason));
        }
        let result = MainGenesisCommandService::new(self.db.clone())
            .execute(MainGenesisDraftCommandInput {
                principal: MainGenesisDraftPrincipal::MainAgent {
                    identity_id: actor_identity_id.to_owned(),
                    scope: scope.clone(),
                },
                request,
                idempotency_key: dedupe_key,
                correlation_id,
                causation_id,
                causation_depth,
                policy_result: policy_result.to_string(),
                requested_permission: requested_permission.to_owned(),
            })
            .await
            .map_err(service_error)?;
        Ok(json!({
            "operation": MAIN_CHARTER_DRAFT_OPERATION,
            "status": "succeeded",
            "materialized": true,
            "domain_committed": true,
            "receipt_id": result.receipt_id,
            "event_id": result.event_id,
            "domain_result": result.result,
        }))
    }

    async fn execute_main_genesis_project_agent_select(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
        mut payload: Value,
    ) -> Result<Value, AgentHostError> {
        if let Some(object) = payload.as_object_mut() {
            object.remove("action");
        }
        let correlation_id = required_argument(&arguments, "correlation_id")?;
        let request: MainGenesisProjectAgentSelectRequest = typed_command_payload(
            MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
            scope,
            &correlation_id,
            payload.clone(),
        )?;
        let requested_permission = operation_permission(
            scope.scope_type,
            MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
        )
        .ok_or_else(|| {
            AgentHostError::Authority(
                "Project Agent selection has no canonical permission descriptor".to_owned(),
            )
        })?;
        let (policy_result, policy_reason) = self
            .actions
            .evaluate_direct_command_policy(
                actor_identity_id,
                scope_type_name(scope.scope_type),
                &scope.scope_id,
                requested_permission,
                MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
                Some(&payload.to_string()),
            )
            .await
            .map_err(service_error)?;
        if policy_result != AgentActionPolicyResult::Allowed {
            return Err(AgentHostError::Authority(policy_reason.unwrap_or_else(
                || "Project Agent selection policy did not admit this command".to_owned(),
            )));
        }
        let result = MainGenesisCommandService::new(self.db.clone())
            .select_project_agent(MainGenesisProjectAgentSelectCommandInput {
                principal: MainGenesisDraftPrincipal::MainAgent {
                    identity_id: actor_identity_id.to_owned(),
                    scope: scope.clone(),
                },
                request,
                idempotency_key: required_argument(&arguments, "dedupe_key")?,
                correlation_id,
                causation_id: arguments
                    .get("causation_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                causation_depth: arguments
                    .get("causation_depth")
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
                policy_result: policy_result.to_string(),
                requested_permission: requested_permission.to_owned(),
            })
            .await
            .map_err(service_error)?;
        Ok(json!({
            "operation": MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
            "status": "succeeded",
            "replayed": result.replayed,
            "materialized": true,
            "domain_committed": true,
            "receipt_id": result.receipt_id,
            "event_id": result.event_id,
            "domain_result": result.result,
        }))
    }

    /// Start Product Genesis from the currently leased Main baseline turn.
    /// The command service resolves the source message and turn from that
    /// lease; neither identifier is accepted from model-authored payload.
    async fn execute_main_genesis_start(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
        mut payload: Value,
    ) -> Result<Value, AgentHostError> {
        if let Some(object) = payload.as_object_mut() {
            object.remove("action");
        }
        let correlation_id = required_argument(&arguments, "correlation_id")?;
        let request: MainGenesisStartRequest = typed_command_payload(
            MAIN_GENESIS_START_OPERATION,
            scope,
            &correlation_id,
            payload.clone(),
        )?;
        let idempotency_key = required_argument(&arguments, "dedupe_key")?;
        let causation_id = arguments
            .get("causation_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let causation_depth = arguments
            .get("causation_depth")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let requested_permission =
            operation_permission(scope.scope_type, MAIN_GENESIS_START_OPERATION).ok_or_else(
                || {
                    AgentHostError::Authority(
                        "Product Genesis start has no canonical permission descriptor".to_owned(),
                    )
                },
            )?;
        let (policy_result, policy_reason) = self
            .actions
            .evaluate_direct_command_policy(
                actor_identity_id,
                scope_type_name(scope.scope_type),
                &scope.scope_id,
                requested_permission,
                MAIN_GENESIS_START_OPERATION,
                Some(&payload.to_string()),
            )
            .await
            .map_err(service_error)?;
        if policy_result != AgentActionPolicyResult::Allowed {
            return Err(AgentHostError::Authority(policy_reason.unwrap_or_else(
                || "Product Genesis start policy did not admit this command".to_owned(),
            )));
        }
        let result = MainGenesisCommandService::new(self.db.clone())
            .start(MainGenesisStartCommandInput {
                principal: MainGenesisStartPrincipal::MainAgent {
                    identity_id: actor_identity_id.to_owned(),
                    scope: scope.clone(),
                },
                request,
                idempotency_key,
                correlation_id,
                causation_id,
                causation_depth,
                policy_result: policy_result.to_string(),
                requested_permission: requested_permission.to_owned(),
            })
            .await
            .map_err(service_error)?;
        Ok(json!({
            "operation": MAIN_GENESIS_START_OPERATION,
            "status": "succeeded",
            "materialized": true,
            "domain_committed": true,
            "control_transfer": result.control_transfer,
            "receipt_id": result.receipt_id,
            "event_id": result.event_id,
            "domain_result": result.result,
        }))
    }

    async fn run_public_search(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        search_scope: PublicSearchScope,
        query: &str,
        limit: u64,
    ) -> Result<Value, AgentHostError> {
        if query.trim().is_empty() || query.chars().count() > 512 {
            return Err(invalid_arguments(
                "search query must contain 1 to 512 characters".to_owned(),
            ));
        }
        if !(1..=10).contains(&limit) {
            return Err(invalid_arguments(
                "search result limit must be between 1 and 10".to_owned(),
            ));
        }

        // Re-authorize the role and derive the account/Project from the
        // authenticated scope before any network request.  Model-provided
        // identifiers are intentionally not accepted here.
        match search_scope {
            PublicSearchScope::Main => {
                self.authorization
                    .main_account_id(actor_identity_id, scope)
                    .await
                    .map_err(native_scope_error)?;
            }
            PublicSearchScope::Project => {
                self.authorization
                    .project_orchestration_target(actor_identity_id, scope)
                    .await
                    .map_err(native_scope_error)?;
            }
        }

        let config = self.public_search_config().ok_or_else(|| {
            AgentHostError::Configuration("public web search is not configured".to_owned())
        })?;
        config.validate().map_err(|_| {
            AgentHostError::Configuration("configured public search limits are invalid".to_owned())
        })?;
        let endpoint = config.endpoint.ok_or_else(|| {
            AgentHostError::Configuration("public web search is not configured".to_owned())
        })?;
        let mut endpoint = url::Url::parse(&endpoint).map_err(|_| {
            AgentHostError::Configuration("configured public search endpoint is invalid".to_owned())
        })?;
        if endpoint.scheme() != "https"
            || endpoint.host().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || endpoint
                .host_str()
                .is_some_and(is_private_or_local_search_host)
        {
            return Err(AgentHostError::Configuration(
                "configured public search endpoint must be a public HTTPS URL without credentials"
                    .to_owned(),
            ));
        }
        endpoint
            .query_pairs_mut()
            .append_pair("q", query.trim())
            .append_pair("limit", &limit.to_string());

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .dns_resolver(Arc::new(PublicSearchResolver {
                allowed_host: endpoint
                    .host_str()
                    .ok_or_else(|| {
                        AgentHostError::Configuration(
                            "configured public search endpoint has no host".to_owned(),
                        )
                    })?
                    .to_owned(),
            }))
            .timeout(Duration::from_millis(config.timeout_ms))
            .build()
            .map_err(|_| {
                AgentHostError::Configuration("public search client unavailable".to_owned())
            })?;
        let response = client
            .get(endpoint)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_| AgentHostError::Runtime("public search request failed".to_owned()))?;
        if !response.status().is_success() {
            return Err(AgentHostError::Runtime(
                "public search endpoint returned an error".to_owned(),
            ));
        }
        if response
            .content_length()
            .is_some_and(|length| length > config.max_response_bytes)
        {
            return Err(AgentHostError::Runtime(
                "public search response is too large".to_owned(),
            ));
        }
        let mut body = Vec::new();
        let mut response = response;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| AgentHostError::Runtime("public search response failed".to_owned()))?
        {
            if body.len().saturating_add(chunk.len()) > config.max_response_bytes as usize {
                return Err(AgentHostError::Runtime(
                    "public search response is too large".to_owned(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        let parsed: PublicSearchResponse = serde_json::from_slice(&body).map_err(|_| {
            AgentHostError::Runtime("public search response is not valid bounded JSON".to_owned())
        })?;
        let truncated = parsed.results.len() > limit as usize;
        let retrieved_at = Utc::now().to_rfc3339();
        let results = parsed
            .results
            .into_iter()
            .take(limit as usize)
            .map(|result| {
                let url = normalize_public_result_url(&result.url)?;
                Ok(json!({
                    "url": url,
                    "title": bounded_untrusted_text(&result.title, 512),
                    "snippet": bounded_untrusted_text(&result.snippet, 2048),
                    "retrieved_at": retrieved_at,
                    "untrusted": true,
                }))
            })
            .collect::<Result<Vec<_>, AgentHostError>>()?;
        let result_count = results.len();
        Ok(json!({
            "scope": match search_scope {
                PublicSearchScope::Main => "main",
                PublicSearchScope::Project => "project",
            },
            "query": query.trim(),
            "results": results,
            "result_count": result_count,
            "truncated": truncated,
            "content_trust": "untrusted_external_data",
            "instructions_are_data": true,
            "materialized": false,
            "persisted": false,
        }))
    }

    async fn project_chat_task_target(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
    ) -> Result<String, AgentHostError> {
        let row = sqlx::query(
            "SELECT chat.kind, chat.project_id, binding.permission_ceiling_json
             FROM agent_chat AS chat
             LEFT JOIN project_agent_binding AS binding
               ON binding.project_id = chat.project_id
              AND binding.identity_id = ?
              AND binding.state = 'active'
             WHERE chat.id = ?
             LIMIT 1",
        )
        .bind(actor_identity_id)
        .bind(&scope.scope_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?
        .ok_or_else(|| AgentHostError::Authority("Agent Chat scope is unavailable".to_owned()))?;
        let kind = row
            .try_get::<String, _>("kind")
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        if kind != "project" {
            return Err(AgentHostError::Authority(
                "Main Agent Chat cannot manage Tasks".to_owned(),
            ));
        }
        let project_id = row
            .try_get::<Option<String>, _>("project_id")
            .map_err(|_| AgentHostError::ProtectedPersistence)?
            .ok_or_else(|| {
                AgentHostError::Authority("Project Agent Chat has no owning Project".to_owned())
            })?;
        let ceiling = row
            .try_get::<Option<String>, _>("permission_ceiling_json")
            .map_err(|_| AgentHostError::ProtectedPersistence)?
            .ok_or_else(|| {
                AgentHostError::Authority(
                    "Project Agent Chat binding does not admit Task management".to_owned(),
                )
            })?;
        if !permission_set(&ceiling).contains("propose_task") {
            return Err(AgentHostError::Authority(
                "permission propose_task is outside the server-issued identity/profile/scope ceiling".to_owned(),
            ));
        }
        Ok(project_id)
    }

    /// Convert a native service result into the stable model-facing envelope.
    /// The operation and scope passed here are always the host-derived values;
    /// neither is read from model payloads.  Approval rows are deliberately
    /// represented as `approval_required`, never as a committed domain
    /// success.
    fn structured_success(
        operation: &str,
        scope: &CanonicalScope,
        correlation_id: &str,
        mut result: Value,
        approval_required: bool,
    ) -> Result<Value, AgentHostError> {
        let replayed = result
            .get("replayed")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        // `replayed` is the envelope-level outcome field. Keep the adaptive
        // domain result itself frozen-identical across first commit and exact
        // replay so callers can compare the receipt-backed result directly.
        if operation == TASK_ADAPTIVE_OPERATION {
            if let Some(object) = result.as_object_mut() {
                object.remove("replayed");
            }
        }
        let receipt_id = result
            .get("receipt_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let event_id = result
            .get("event_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let requires_user_authorization = result
            .get("requires_user_authorization")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let approval_required = approval_required || requires_user_authorization;
        let scope = outcome_scope(scope);
        let mut outcome = if approval_required {
            let mut outcome = OrchestrationOutcome::new(
                OutcomeCode::ApprovalRequired,
                OutcomeStatus::ApprovalRequired,
                operation,
                scope.clone(),
                correlation_id,
            );
            outcome.safe_message =
                "approval is required before this operation is committed".to_owned();
            outcome.approval_target = Some(approval_target(operation, scope, &result));
            // Some direct commands (for example a baseline proposal) commit
            // an exact proposal receipt before user approval, while a pure
            // approval-backed Action has no domain commit. Preserve the
            // former's frozen result without making the latter look executed.
            if receipt_id.is_some() {
                outcome.result = Some(result);
            }
            outcome
        } else {
            OrchestrationOutcome::succeeded(operation, scope, correlation_id, Some(result))
        };
        outcome.replayed = replayed;
        outcome.receipt_id = receipt_id;
        outcome.event_id = event_id;
        serde_json::to_value(outcome).map_err(|_| AgentHostError::ProtectedPersistence)
    }

    async fn project_pause_cause(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        paused_project_id: Option<&str>,
    ) -> DeniedBy {
        let project_id = if scope.scope_type == CanonicalScopeType::Task {
            sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                .bind(&scope.scope_id)
                .fetch_optional(self.db.pool())
                .await
                .ok()
                .flatten()
        } else {
            self.authorization
                .project_orchestration_target(actor_identity_id, scope)
                .await
                .ok()
        };
        let Some(project_id) =
            project_id.filter(|id| paused_project_id.is_none_or(|paused| paused == id))
        else {
            return DeniedBy::Unspecified;
        };
        let reason = sqlx::query_scalar::<_, Option<String>>(
            "SELECT system_pause_reason FROM project WHERE id = ?",
        )
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await
        .ok()
        .flatten()
        .flatten();
        db::project_pause_denial(reason.as_deref())
    }

    /// Build a structured failure after the command boundary has established
    /// the canonical actor/scope.  Current state is loaded only after that
    /// authorization check and only for the Project resource named by a
    /// typed command payload.
    async fn structured_boundary_error(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: &Value,
        error: AgentHostError,
    ) -> AgentHostError {
        let correlation_id = correlation_id(arguments, operation, scope);
        let error_kind = match &error {
            AgentHostError::StructuredOutcome(outcome) => outcome.code.as_str(),
            AgentHostError::Authority(_) => "authority",
            AgentHostError::AgentPaused { .. } => "agent_paused",
            AgentHostError::ProjectPaused { .. } => "project_paused",
            AgentHostError::Configuration(_) => "configuration",
            AgentHostError::SessionNotFound => "session_not_found",
            AgentHostError::CredentialNotFound => "credential_not_found",
            AgentHostError::VersionConflict => "version_conflict",
            AgentHostError::Unsupported(_) => "unsupported",
            AgentHostError::Runtime(_) => "runtime",
            AgentHostError::RuntimeWithUsage { .. } => "runtime",
            AgentHostError::TurnLimitReached { .. } => "turn_limit_reached",
            AgentHostError::ProtectedPersistence => "protected_persistence",
        };
        tracing::warn!(
            correlation_id = %correlation_id,
            operation,
            error_kind,
            "native Forge orchestration operation failed; cause redacted"
        );
        let mut outcome = match error {
            AgentHostError::StructuredOutcome(outcome) => *outcome,
            AgentHostError::SessionNotFound | AgentHostError::CredentialNotFound => {
                OrchestrationOutcome::failed(
                    OutcomeCode::NotFound,
                    operation,
                    outcome_scope(scope),
                    &correlation_id,
                    "the requested Forge resource is unavailable",
                )
            }
            AgentHostError::VersionConflict => OrchestrationOutcome::failed(
                OutcomeCode::VersionConflict,
                operation,
                outcome_scope(scope),
                &correlation_id,
                "the authorized resource changed; refresh current state and retry",
            ),
            AgentHostError::Configuration(_) => {
                let mut outcome = OrchestrationOutcome::failed(
                    OutcomeCode::SetupRequired,
                    operation,
                    outcome_scope(scope),
                    &correlation_id,
                    "required Forge setup is incomplete",
                );
                outcome.setup_requirements =
                    Some(vec![SetupRequirement::new("forge_configuration")]);
                outcome.retry = Some(RetryInstruction::new(RetryAction::CompleteSetup, false));
                outcome
            }
            AgentHostError::AgentPaused { agent_id } => OrchestrationOutcome::terminal_denial(
                operation,
                outcome_scope(scope),
                &correlation_id,
                if agent_id == actor_identity_id {
                    DeniedBy::IdentityPaused
                } else {
                    DeniedBy::TargetAgentPaused
                },
            ),
            AgentHostError::ProjectPaused { project_id } => OrchestrationOutcome::terminal_denial(
                operation,
                outcome_scope(scope),
                &correlation_id,
                self.project_pause_cause(actor_identity_id, scope, Some(&project_id))
                    .await,
            ),
            AgentHostError::Authority(detail) => {
                let mut cause =
                    native_denial_cause(&detail, operation_permission(scope.scope_type, operation));
                if matches!(cause, DeniedBy::ProjectPaused(_)) {
                    cause = self
                        .project_pause_cause(actor_identity_id, scope, None)
                        .await;
                }
                tracing::warn!(diagnostic = %detail, denied_by = %cause, operation,
                    "native policy refusal; diagnostic is server-only");
                OrchestrationOutcome::terminal_denial(
                    operation,
                    outcome_scope(scope),
                    &correlation_id,
                    cause,
                )
            }
            AgentHostError::Unsupported(detail) => {
                let mut outcome = OrchestrationOutcome::failed(
                    OutcomeCode::ValidationError,
                    operation,
                    outcome_scope(scope),
                    &correlation_id,
                    format!(
                        "the operation or arguments are not valid for this Forge surface ({detail})"
                    ),
                );
                outcome.retry = Some(RetryInstruction::new(RetryAction::CorrectInput, false));
                outcome
            }
            AgentHostError::Runtime(_) => OrchestrationOutcome::failed(
                OutcomeCode::InternalFailure,
                operation,
                outcome_scope(scope),
                &correlation_id,
                "the Forge operation could not complete",
            ),
            AgentHostError::RuntimeWithUsage { .. } => OrchestrationOutcome::failed(
                OutcomeCode::InternalFailure,
                operation,
                outcome_scope(scope),
                &correlation_id,
                "the Forge operation could not complete",
            ),
            AgentHostError::TurnLimitReached { .. } => OrchestrationOutcome::failed(
                OutcomeCode::TransientFailure,
                operation,
                outcome_scope(scope),
                &correlation_id,
                "the Forge operation reached a runtime limit; retry later",
            ),
            AgentHostError::ProtectedPersistence => OrchestrationOutcome::failed(
                OutcomeCode::InternalFailure,
                operation,
                outcome_scope(scope),
                &correlation_id,
                "the Forge operation could not complete",
            ),
        };
        outcome.operation = operation.to_owned();
        outcome.scope = outcome_scope(scope);
        outcome.correlation_id = correlation_id;

        if outcome.code == OutcomeCode::PolicyDenied && outcome.denied_by.is_none() {
            outcome = OrchestrationOutcome::terminal_denial(
                operation,
                outcome_scope(scope),
                &outcome.correlation_id,
                DeniedBy::Unspecified,
            );
        }
        if outcome
            .denied_by
            .as_ref()
            .is_some_and(DeniedBy::withdraws_operation)
            && operation != "message.send"
            && self
                .authorization
                .project_orchestration_target(actor_identity_id, scope)
                .await
                .is_ok()
        {
            if let Some(permission) = operation_permission(scope.scope_type, "message.send") {
                if self
                    .actions
                    .evaluate_direct_command_policy(
                        actor_identity_id,
                        scope_type_name(scope.scope_type),
                        &scope.scope_id,
                        permission,
                        "message.send",
                        None,
                    )
                    .await
                    .is_ok_and(|(result, _)| {
                        matches!(
                            result,
                            AgentActionPolicyResult::Allowed
                                | AgentActionPolicyResult::ApprovalRequired
                        )
                    })
                {
                    outcome.alternatives = Some(vec!["message.send".to_owned()]);
                    outcome.safe_message.push_str(" Use message.send to escalate to the user only what your authority cannot cover.");
                }
            }
        }

        if matches!(outcome.code, OutcomeCode::VersionConflict) {
            let current = match self
                .authorization
                .project_orchestration_target(actor_identity_id, scope)
                .await
                .map_err(native_scope_error)
            {
                Ok(project_id) => self
                    .project_actions
                    .authorized_current_version_or_revision(&project_id, operation, arguments)
                    .await
                    .ok()
                    .flatten(),
                Err(_) => None,
            };
            if let Some(current) = current {
                outcome.current_version_or_revision = Some(current.clone());
                outcome.retry = Some(retry_for_current(operation, &current));
            } else if outcome.retry.is_none() {
                outcome.retry = Some(RetryInstruction::new(RetryAction::RefreshAndRetry, true));
            }
        }
        if matches!(outcome.code, OutcomeCode::IdempotencyConflict) {
            // Never query current state for a conflicting key.  A new key is
            // the only safe corrective action when the receipt is bound to a
            // different command input.
            outcome.current_version_or_revision = None;
            outcome.retry = Some(RetryInstruction::new(
                RetryAction::UseNewIdempotencyKey,
                false,
            ));
        }
        AgentHostError::StructuredOutcome(Box::new(outcome))
    }
}

#[async_trait]
impl ForgeToolProvider for CoordinationToolProvider {
    async fn record_terminal_denial(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        session_token: &str,
        operation: &str,
        denied_by: &DeniedBy,
    ) -> Result<(), AgentHostError> {
        if matches!(denied_by, DeniedBy::PermissionMissing(name)
            if operation_permission(scope.scope_type, operation) != Some(name.as_str()))
        {
            return Ok(());
        }
        if scope.scope_type == CanonicalScopeType::AgentChat {
            db::ChatSessionDenialRepo::record_chat_session_denial(
                &*self.db,
                actor_identity_id,
                &scope.scope_id,
                session_token,
                operation,
                denied_by,
            )
            .await
            .map_err(|error| service_error(error.into()))?;
        }
        Ok(())
    }

    async fn clear_terminal_denials(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        session_token: &str,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        if scope.scope_type == CanonicalScopeType::AgentChat {
            let rows = self
                .read_session_denials
                .read()
                .ok()
                .and_then(|read| {
                    read.get(&(actor_identity_id.to_owned(), scope.scope_id.clone()))
                        .map(|rows| {
                            rows.iter()
                                .filter(|row| {
                                    row.operation == operation
                                        && (row.session_id == session_token
                                            || row.runtime_session_id.as_deref()
                                                == Some(session_token))
                                })
                                .cloned()
                                .collect::<Vec<_>>()
                        })
                })
                .unwrap_or_default();
            for row in rows {
                db::ChatSessionDenialRepo::delete_chat_session_denial(&*self.db, &row)
                    .await
                    .map_err(|error| service_error(error.into()))?;
                if let Ok(mut read) = self.read_session_denials.write() {
                    if let Some(rows) =
                        read.get_mut(&(actor_identity_id.to_owned(), scope.scope_id.clone()))
                    {
                        rows.retain(|stored| {
                            stored.session_id != row.session_id
                                || stored.operation != row.operation
                                || stored.denied_by != row.denied_by
                        });
                    }
                }
            }
        }
        Ok(())
    }

    async fn observe_command(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        observation: CommandObservation,
    ) -> Result<Value, AgentHostError> {
        // The observation belongs to the Project this session is bound to;
        // the binding, not the payload, says which Project that is.
        let arguments =
            json!({"session_id": observation.session_id, "turn_id": observation.turn_id});
        let project_id = match self
            .authorization
            .project_orchestration_target(actor_identity_id, scope)
            .await
        {
            Ok(project_id) => project_id,
            Err(error) => {
                return Err(self
                    .structured_boundary_error(
                        actor_identity_id,
                        scope,
                        "observe_command",
                        &arguments,
                        service_error(error),
                    )
                    .await)
            }
        };
        let id = db::new_uuid_v4();
        let now = db::now_rfc3339();
        let saved = sqlx::query(
            "INSERT INTO project_command_observation (
                id, project_id, actor_identity_id, scope_type, scope_id, session_id, turn_id,
                program, args_json, exit_code, success, output_digest, stdout_excerpt,
                stderr_excerpt, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(&project_id)
        .bind(actor_identity_id)
        .bind(scope_type_name(scope.scope_type))
        .bind(&scope.scope_id)
        .bind(&observation.session_id)
        .bind(observation.turn_id.as_deref())
        .bind(&observation.program)
        .bind(serde_json::to_string(&observation.args).unwrap_or_else(|_| "[]".to_owned()))
        .bind(observation.exit_code)
        .bind(i64::from(observation.success))
        .bind(&observation.output_digest)
        .bind(&observation.stdout_excerpt)
        .bind(&observation.stderr_excerpt)
        .bind(&now)
        .execute(self.db.pool())
        .await;
        if let Err(error) = saved {
            return Err(self
                .structured_boundary_error(
                    actor_identity_id,
                    scope,
                    "observe_command",
                    &arguments,
                    AgentHostError::Runtime(error.to_string()),
                )
                .await);
        }
        Ok(json!({"observation_id": id, "recorded_at": now}))
    }

    fn public_search_configured(&self) -> bool {
        self.public_search_config()
            .is_some_and(|config| config.endpoint.is_some() && config.validate().is_ok())
    }

    async fn public_search(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        search_scope: PublicSearchScope,
        query: &str,
        limit: u64,
    ) -> Result<Value, AgentHostError> {
        let result = self
            .run_public_search(actor_identity_id, scope, search_scope, query, limit)
            .await;
        match result {
            Err(error) => Err(self
                .structured_boundary_error(
                    actor_identity_id,
                    scope,
                    forge_agent_host::FORGE_PUBLIC_WEB_SEARCH_TOOL,
                    &json!({"query": query, "limit": limit}),
                    error,
                )
                .await),
            other => other,
        }
    }

    async fn read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        let boundary_arguments = arguments.clone();
        match operation_descriptor(scope.scope_type, operation, None).classification {
            OperationClassification::Query => {}
            OperationClassification::Denied => {
                return Err(AgentHostError::StructuredOutcome(Box::new(
                    OrchestrationOutcome::terminal_denial(
                        operation,
                        outcome_scope(scope),
                        correlation_id(&arguments, operation, scope),
                        DeniedBy::OperationNotInScope,
                    ),
                )));
            }
            OperationClassification::DirectCommand
            | OperationClassification::ApprovalRequiredAction => {
                return Err(AgentHostError::Unsupported(
                    "mutation operations execute through the proposal boundary".to_owned(),
                ));
            }
        }
        let result = if let Some(spec) = registered_reads::CATALOG.lookup(operation) {
            let context = registered_reads::Context {
                provider: self,
                actor_identity_id,
                scope,
            };
            match spec.dispatch(&context, arguments).await {
                Ok(result) => Ok(result),
                Err(operation_registry::DispatchError::Handler(error)) => Err(error),
                // The contract is checked before the handler authorizes the
                // caller. A caller the handler would deny gets that denial,
                // not the contract.
                Err(operation_registry::DispatchError::InvalidInput(message)) => {
                    Err(match context.main_read_denial(operation).await {
                        Some(denial) => denial,
                        None => invalid_arguments(message),
                    })
                }
            }
        } else {
            match operation {
                PROJECT_CURRENT_STATE_OPERATION => {
                    self.project_current_state_read(actor_identity_id, scope, arguments)
                        .await
                }
                PROJECT_OBSERVATIONS_OPERATION => {
                    self.project_observations_read(actor_identity_id, scope, arguments)
                        .await
                }
                "memory.read" => {
                    self.memory_read(actor_identity_id, scope, arguments, false)
                        .await
                }
                "project.summary" | "task.summary" => {
                    if operation == "project.summary"
                        && scope.scope_type == CanonicalScopeType::AgentChat
                    {
                        self.project_summary_read(actor_identity_id, scope, arguments)
                            .await
                    } else {
                        self.summary(actor_identity_id, scope).await
                    }
                }
                "decisions.read" => {
                    self.memory_read(actor_identity_id, scope, arguments, true)
                        .await
                }
                "work.read" | "events.read" | "inbox.read" | "commitments.read"
                | "delivery.read" => {
                    self.scoped_rows(actor_identity_id, scope, operation, arguments)
                        .await
                }
                _ => Err(AgentHostError::Unsupported(
                    "Forge read operation is not implemented".to_owned(),
                )),
            }
        };
        if operation_contract(operation).is_some() {
            match result {
                Ok(result) => Self::structured_success(
                    operation,
                    scope,
                    &correlation_id(&boundary_arguments, operation, scope),
                    result,
                    false,
                ),
                Err(error) => Err(self
                    .structured_boundary_error(
                        actor_identity_id,
                        scope,
                        operation,
                        &boundary_arguments,
                        error,
                    )
                    .await),
            }
        } else {
            match result {
                Err(error)
                    if matches!(&error, AgentHostError::Authority(_))
                        || matches!(
                            &error,
                            AgentHostError::StructuredOutcome(_)
                                | AgentHostError::AgentPaused { .. }
                                | AgentHostError::ProjectPaused { .. }
                        ) =>
                {
                    Err(self
                        .structured_boundary_error(
                            actor_identity_id,
                            scope,
                            operation,
                            &boundary_arguments,
                            error,
                        )
                        .await)
                }
                other => other,
            }
        }
    }

    async fn propose(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        runtime_session_id: &str,
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        let correlation = correlation_id(&arguments, operation, scope);
        let payload = arguments.get("payload");
        let approval_required = payload
            .map(|payload| {
                matches!(
                    operation_descriptor(scope.scope_type, operation, Some(payload)).classification,
                    OperationClassification::ApprovalRequiredAction
                )
            })
            .unwrap_or(false);
        let result = self
            .propose(
                actor_identity_id,
                scope,
                runtime_session_id,
                operation,
                arguments.clone(),
            )
            .await;
        if operation_contract(operation).is_some() {
            match result {
                Ok(result) => Self::structured_success(
                    operation,
                    scope,
                    &correlation,
                    result,
                    approval_required,
                ),
                Err(error) => Err(self
                    .structured_boundary_error(
                        actor_identity_id,
                        scope,
                        operation,
                        &arguments,
                        error,
                    )
                    .await),
            }
        } else {
            match result {
                Err(error)
                    if matches!(&error, AgentHostError::Authority(_))
                        || matches!(
                            &error,
                            AgentHostError::StructuredOutcome(_)
                                | AgentHostError::AgentPaused { .. }
                                | AgentHostError::ProjectPaused { .. }
                        ) =>
                {
                    Err(self
                        .structured_boundary_error(
                            actor_identity_id,
                            scope,
                            operation,
                            &arguments,
                            error,
                        )
                        .await)
                }
                other => other,
            }
        }
    }
}

fn action_value(action: &AgentAction) -> Value {
    json!({
        "id": action.id,
        "operation": action.operation,
        "scope_type": action.scope_type,
        "scope_id": action.scope_id,
        "requested_permission": action.requested_permission,
        "policy_result": action.policy_result.to_string(),
        "status": action.status.to_string(),
        "target_type": action.target_type,
        "target_id": action.target_id,
        "version": action.version,
    })
}

/// Deserialize a model-authored command payload, reporting a schema mismatch
/// as a model-facing validation outcome.  The model authored this input, so
/// the offending field path and the expected shape are safe to hand back —
/// and without them the `CorrectInput` retry instruction names no correction,
/// which leaves a wrong payload unfixable and invites the model to narrate a
/// success it never got.
fn typed_command_payload<T: serde::de::DeserializeOwned>(
    operation: &str,
    scope: &CanonicalScope,
    correlation_id: &str,
    payload: Value,
) -> Result<T, AgentHostError> {
    serde_path_to_error::deserialize(payload).map_err(|error| {
        let path = error.path().to_string();
        let detail = if path.is_empty() {
            error.inner().to_string()
        } else {
            format!("{path}: {}", error.inner())
        };
        let mut outcome = OrchestrationOutcome::failed(
            OutcomeCode::ValidationError,
            operation,
            outcome_scope(scope),
            correlation_id,
            format!("the payload does not match the {operation} schema ({detail})"),
        );
        outcome.retry = Some(RetryInstruction::new(RetryAction::CorrectInput, false));
        AgentHostError::StructuredOutcome(Box::new(outcome))
    })
}

fn outcome_scope(scope: &CanonicalScope) -> OutcomeScopeRef {
    let scope_type = match scope.scope_type {
        CanonicalScopeType::Account => OutcomeScopeType::Account,
        CanonicalScopeType::Project => OutcomeScopeType::Project,
        CanonicalScopeType::AgentChat => OutcomeScopeType::AgentChat,
        CanonicalScopeType::Task => OutcomeScopeType::Task,
    };
    OutcomeScopeRef::new(scope_type, scope.scope_id.clone())
}

fn correlation_id(arguments: &Value, operation: &str, scope: &CanonicalScope) -> String {
    let supplied = arguments
        .get("correlation_id")
        .and_then(Value::as_str)
        .or_else(|| {
            arguments
                .get("payload")
                .and_then(|payload| payload.get("correlation_id"))
                .and_then(Value::as_str)
        });
    if let Some(value) = supplied
        .filter(|value| !value.trim().is_empty())
        .filter(|value| value.chars().count() <= 256)
        .filter(|value| !value.chars().any(char::is_control))
    {
        return value.to_owned();
    }
    // Read operations do not accept a model correlation id.  Mint a fresh
    // server-side join key rather than deriving one from model-controlled
    // operation text or a scope label.
    let _ = (operation, scope);
    Uuid::new_v4().to_string()
}

fn result_string(result: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| result.get(*name).and_then(Value::as_str).map(str::to_owned))
}

fn result_i64(result: &Value, names: &[&str]) -> Option<i64> {
    names
        .iter()
        .find_map(|name| result.get(*name).and_then(Value::as_i64))
}

fn approval_target(operation: &str, scope: OutcomeScopeRef, result: &Value) -> ApprovalTarget {
    let nested_target = result
        .get("domain_result")
        .and_then(|domain_result| domain_result.get("approval_target"));
    let target_value = nested_target.unwrap_or(result);
    let target_type = result_string(target_value, &["target_type", "scope_type"])
        .or_else(|| {
            target_value
                .get("baseline_id")
                .map(|_| "execution_baseline".to_owned())
        })
        .unwrap_or_else(|| scope.scope_type.as_str().to_owned());
    let target_id = result_string(
        target_value,
        &["target_id", "baseline_id", "project_id", "id"],
    )
    .unwrap_or_else(|| scope.scope_id.clone());
    let mut target = ApprovalTarget::new(target_type, target_id);
    target.operation = Some(operation.to_owned());
    target.version = result_i64(
        target_value,
        &[
            "version",
            "baseline_version",
            "project_version",
            "charter_version",
            "document_version",
            "milestone_version",
        ],
    )
    .or_else(|| {
        result.get("domain_result").and_then(|domain_result| {
            result_i64(
                domain_result,
                &[
                    "version",
                    "baseline_version",
                    "project_version",
                    "charter_version",
                    "document_version",
                    "milestone_version",
                ],
            )
        })
    });
    target.revision_id = result_string(
        nested_target.unwrap_or(result),
        &["revision_id", "current_revision_id"],
    );
    target.revision = result_i64(nested_target.unwrap_or(result), &["revision"]);
    target.content_digest = result_string(nested_target.unwrap_or(result), &["content_digest"]);
    target.rendered_digest = result_string(
        nested_target.unwrap_or(result),
        &["render_digest", "rendered_digest"],
    );
    target.requires_user_authorization = true;
    target
}

fn retry_for_current(operation: &str, current: &CurrentVersionOrRevision) -> RetryInstruction {
    let mut retry = RetryInstruction::new(RetryAction::RefreshAndRetry, true);
    if let Some(version) = current.version {
        let field = match operation {
            _ if current.resource_type == "project" => "expected_project_version",
            TASK_ADAPTIVE_OPERATION => "expected_task_version",
            TASK_ACTION_OPERATION => "version",
            PROJECT_DOCUMENT_OPERATION => "expected_document_version",
            PROJECT_MILESTONE_OPERATION
            | PROJECT_EVIDENCE_OPERATION
            | PROJECT_VALIDATION_OPERATION => "expected_milestone_version",
            PROJECT_READINESS_OPERATION | PROJECT_RELEASE_OPERATION => "milestone_version",
            _ => "expected_version",
        };
        retry.arguments.insert(field.to_owned(), json!(version));
    }
    if let Some(revision) = current.revision {
        if matches!(operation, TASK_ADAPTIVE_OPERATION) {
            retry
                .arguments
                .insert("expected_board_revision".to_owned(), json!(revision));
        }
    }
    if let Some(revision_id) = current.revision_id.as_deref() {
        if matches!(operation, PROJECT_DOCUMENT_OPERATION) {
            retry
                .arguments
                .insert("base_revision_id".to_owned(), json!(revision_id));
        }
    }
    if let Some(content_digest) = current.content_digest.as_deref() {
        retry
            .arguments
            .insert("content_digest".to_owned(), json!(content_digest));
    }
    if let Some(rendered_digest) = current.rendered_digest.as_deref() {
        retry
            .arguments
            .insert("render_digest".to_owned(), json!(rendered_digest));
    }
    retry
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicSearchResponse {
    results: Vec<PublicSearchResult>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicSearchResult {
    url: String,
    title: String,
    snippet: String,
}

fn normalize_public_result_url(value: &str) -> Result<String, AgentHostError> {
    if value.chars().count() > 2048 {
        return Err(AgentHostError::Runtime(
            "public search result URL is too long".to_owned(),
        ));
    }
    // URL values are untrusted endpoint data.  Reject control characters
    // before parsing/serializing so logs, rendered links, and downstream
    // clients cannot receive a delimiter or terminal injection payload.
    if value.chars().any(char::is_control) {
        return Err(AgentHostError::Runtime(
            "public search result URL contains control characters".to_owned(),
        ));
    }
    let parsed = url::Url::parse(value)
        .map_err(|_| AgentHostError::Runtime("public search result URL is invalid".to_owned()))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
        || parsed
            .host_str()
            .is_some_and(is_private_or_local_search_host)
    {
        return Err(AgentHostError::Runtime(
            "public search result URL is not a public HTTP(S) URL".to_owned(),
        ));
    }
    Ok(parsed.to_string())
}

fn is_private_or_local_search_host(host: &str) -> bool {
    let normalized = host
        .trim_end_matches('.')
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    // Zone identifiers are local-interface selectors (for example
    // `fe80::1%25en0`), not public DNS/HTTP hosts.  Reject them before the
    // `IpAddr` parser can treat the value as an opaque hostname.
    if normalized.contains('%') {
        return true;
    }
    if matches!(normalized.as_str(), "localhost" | "localhost.localdomain")
        || normalized.ends_with(".localhost")
        || normalized.ends_with(".local")
    {
        return true;
    }
    let Ok(address) = normalized.parse::<IpAddr>() else {
        // Hostnames are checked again by the request-time resolver.  Result
        // URLs are metadata only, so reject known local names immediately.
        return false;
    };
    is_blocked_public_address(address)
}

/// Resolve the configured endpoint ourselves and pass only validated socket
/// addresses to reqwest.  This closes DNS rebinding/private-address gaps that
/// literal hostname checks cannot address.
#[derive(Debug, Clone)]
struct PublicSearchResolver {
    allowed_host: String,
}

impl reqwest::dns::Resolve for PublicSearchResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_owned();
        let allowed_host = self.allowed_host.clone();
        Box::pin(async move {
            let normalized_host = host.trim_end_matches('.');
            let normalized_allowed_host = allowed_host.trim_end_matches('.');
            if normalized_host.is_empty()
                || !normalized_host.eq_ignore_ascii_case(normalized_allowed_host)
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "public search resolver received an unexpected host",
                )
                .into());
            }
            let addresses = tokio::net::lookup_host((normalized_host, 0))
                .await?
                .filter(|address| !is_blocked_public_address(address.ip()))
                .collect::<Vec<_>>();
            if addresses.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "public search endpoint resolved only to blocked addresses",
                )
                .into());
            }
            let addresses: reqwest::dns::Addrs = Box::new(addresses.into_iter());
            Ok(addresses)
        })
    }
}

fn is_blocked_public_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            let octets = address.octets();
            address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_unspecified()
                || address.is_broadcast()
                || (octets[0] == 0)
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
                || (octets[0] == 192 && octets[1] == 0)
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
                || (octets[0] == 192 && octets[1] == 88 && octets[2] == 99)
                || (octets[0] == 198 && (18..=19).contains(&octets[1]))
                || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
                || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
                || octets[0] >= 224
        }
        IpAddr::V6(address) => is_blocked_public_ipv6(address),
    }
}

/// Reject IPv6 address classes that are private, local, special-use, or can
/// encode another address family.  In particular, all IPv4-compatible and
/// IPv4-mapped forms are denied (including mapped public IPv4 values), rather
/// than only checking the embedded address for private ranges.
fn is_blocked_public_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    let first = segments[0];
    address.is_loopback()
        || address.is_unspecified()
        || address.is_unique_local()
        || address.is_unicast_link_local()
        || address.to_ipv4().is_some()
        // Deprecated site-local space (fec0::/10).
        || (first & 0xffc0 == 0xfec0)
        // IPv6 multicast (ff00::/8).
        || (first & 0xff00 == 0xff00)
        // Documentation and benchmark prefixes.
        || (first == 0x2001 && segments[1] == 0x0db8)
        || (first == 0x2001 && segments[1] == 0x0002 && segments[2] == 0)
        // IANA-reserved 2001:0::/29 special-use blocks (Teredo, AMT,
        // AS112-v6, and related transition/documentation ranges).
        || (first == 0x2001 && (0..=5).contains(&segments[1]))
        // RFC 9637 documentation prefix 3fff::/20.
        || (0x3ff0..=0x3fff).contains(&first)
        // Teredo, ORCHID/ORCHIDv2, and 6to4 transition prefixes.
        || (first == 0x2001 && segments[1] == 0)
        || (first == 0x2001 && (0x0010..=0x001f).contains(&segments[1]))
        || (first == 0x2001 && (0x0020..=0x002f).contains(&segments[1]))
        || first == 0x2002
        // Discard-only and NAT64 well-known/local-use prefixes.  These can
        // otherwise hide a private IPv4 target behind a globally-looking v6
        // literal.
        || (first == 0x0100
            && segments[1] == 0
            && segments[2] == 0
            && segments[3] == 0)
        || (first == 0x0064 && segments[1] == 0xff9b && segments[2] == 0)
        || (first == 0x0064 && segments[1] == 0xff9b && segments[2] == 1)
}

fn bounded_untrusted_text(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn required_argument(arguments: &Value, field: &str) -> Result<String, AgentHostError> {
    arguments
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| AgentHostError::Unsupported(format!("{field} is required")))
}

fn validate_proposal_payload(operation: &str, payload: &Value) -> Result<(), AgentHostError> {
    if !payload.is_object() {
        return Err(AgentHostError::Authority(
            "Forge proposal payload must be an object".to_owned(),
        ));
    }
    if operation == TASK_PLAN_OPERATION {
        let object = payload.as_object().expect("payload object checked above");
        let content = object.get("content").and_then(Value::as_str);
        let exact_shape = object.len() == 2
            && object.get("action").and_then(Value::as_str) == Some("write")
            && content.is_some_and(|content| {
                !content.trim().is_empty()
                    && content.len() as u64 <= crate::plan_artifact::MAX_PLAN_ARTIFACT_SIZE_BYTES
            });
        if !exact_shape {
            return Err(AgentHostError::Authority(
                "task.plan requires only action=write and non-empty content no larger than 1 MiB"
                    .to_owned(),
            ));
        }
    } else if serde_json::to_vec(payload)
        .map(|bytes| bytes.len() > 64 * 1024)
        .unwrap_or(true)
    {
        return Err(AgentHostError::Authority(
            "Forge proposal payload is too large".to_owned(),
        ));
    }
    if operation == "session.action" {
        let action = payload
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| AgentHostError::Authority("session action is required".to_owned()))?;
        if !matches!(action, "cancel" | "steer") {
            return Err(AgentHostError::Authority(
                "only bounded cancel or steer session actions are admitted".to_owned(),
            ));
        }
        if action == "steer"
            && payload
                .get("content")
                .and_then(Value::as_str)
                .is_none_or(|content| content.chars().count() > 4096)
        {
            return Err(AgentHostError::Authority(
                "session steer content must be at most 4096 characters".to_owned(),
            ));
        }
    }
    if operation_contract(operation).is_some() && contains_authority_override(payload) {
        return Err(AgentHostError::Authority(
            "Forge orchestration scope and authority are server-derived".to_owned(),
        ));
    }
    if operation == TASK_ADAPTIVE_OPERATION {
        if contains_adaptive_authority_override(payload) {
            return Err(AgentHostError::Authority(
                "adaptive Task Project, actor, governance, and fixed boundaries are server-derived"
                    .to_owned(),
            ));
        }
        serde_json::from_value::<AdaptiveTaskPayload>(payload.clone()).map_err(|_| {
            AgentHostError::Authority(
                "adaptive Task payload must be one closed split, sequence, or replace command"
                    .to_owned(),
            )
        })?;
    }
    if operation == TASK_ACTION_OPERATION {
        serde_json::from_value::<api_types::TaskActionRequest>(
            json!({ "action": payload.get("action"), "version": payload.get("version") }),
        )
        .map_err(|_| {
            invalid_arguments("Task action requires a closed verb and exact version".to_owned())
        })?;
    }
    if operation == TASK_DEPENDENCY_OPERATION {
        serde_json::from_value::<TaskDependencyPayload>(payload.clone()).map_err(|_| {
            AgentHostError::Authority(
                "Task dependency payload must contain add or remove, both Task ids, and a rationale"
                    .to_owned(),
            )
        })?;
    }
    match operation {
        "project.lifecycle" => {
            let action = payload
                .get("action")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    AgentHostError::Authority("Project lifecycle action is required".to_owned())
                })?;
            if !matches!(action, "organize" | "pause" | "resume" | "archive") {
                return Err(AgentHostError::Authority(
                    "Project lifecycle action is not admitted".to_owned(),
                ));
            }
            if payload
                .get("project_id")
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(AgentHostError::Authority(
                    "project_id is required for this lifecycle action".to_owned(),
                ));
            }
        }
        "handoff.publish" => {
            let target = payload
                .get("target_project_id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    AgentHostError::Authority("target_project_id is required".to_owned())
                })?;
            if target.chars().count() > 200 {
                return Err(AgentHostError::Authority(
                    "handoff target is invalid".to_owned(),
                ));
            }
            let content = payload
                .get("content")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    AgentHostError::Authority("handoff content is required".to_owned())
                })?;
            if content.chars().count() > 16_384 {
                return Err(AgentHostError::Authority(
                    "handoff content is too long".to_owned(),
                ));
            }
        }
        "decision.request" => {
            return Err(AgentHostError::Authority(
                "generic decision proposals are not admitted; use the typed Project orchestration contract".to_owned(),
            ));
        }
        "project.release" | "project.milestone.release" => {
            return Err(AgentHostError::Authority(
                "final release is user-only; Project Agent may submit only a typed release candidate request".to_owned(),
            ));
        }
        _ => {}
    }
    if matches!(operation, "project.lifecycle" | "handoff.publish") {
        // The provider persists only guarded action envelopes.  This catches
        // credential-shaped model output before it reaches the action ledger,
        // while retaining the actual content in the protected runtime only.
        let serialized = serde_json::to_string(payload).map_err(|_| {
            AgentHostError::Authority("proposal payload is not serializable".to_owned())
        })?;
        guard_agent_chat_content(&serialized).map_err(|_| {
            AgentHostError::Authority("protected values cannot be proposed".to_owned())
        })?;
    }
    Ok(())
}

/// Translate only known server-authored reasons about the caller's own authority.
/// Unknown reasons may describe another scope and retain the generic redaction.
fn native_denial_cause(reason: &str, permission: Option<&str>) -> DeniedBy {
    if let Ok(cause) = reason.parse::<DeniedBy>() {
        return match cause {
            DeniedBy::PermissionMissing(name) if name == "unknown" => permission
                .map(|name| DeniedBy::PermissionMissing(name.to_owned()))
                .unwrap_or(DeniedBy::Unspecified),
            cause => cause,
        };
    }
    if let Some(name) = reason
        .strip_prefix("permission ")
        .and_then(|rest| {
            rest.strip_suffix(" is outside the server-issued identity/profile/scope ceiling")
        })
        .filter(|name| {
            !name.is_empty() && name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
        })
    {
        return DeniedBy::PermissionMissing(name.to_owned());
    }
    match reason {
        "requested permission is outside the Project binding ceiling"
        | "requested permission is outside the Agent Chat binding ceiling"
        | "direct Project command permission is outside the account ceiling"
        | "direct Project command permission is outside the selected profile ceiling"
        | "direct Project command permission is outside the active binding ceiling" => {
            permission.map(|name| DeniedBy::PermissionMissing(name.to_owned())).unwrap_or(DeniedBy::Unspecified)
        }
        "actor identity is paused or archived"
        | "Project Agent is paused or archived"
        | "direct Project command principal is paused or archived"
        | "agent principal is paused or archived" => DeniedBy::IdentityPaused,
        "direct Project command has no selected active profile" => DeniedBy::ProfileNotSelected,
        "the bound Project has no approved Charter"
        | "Project orchestration remains blocked until a user-approved Charter adoption is committed"
        | "direct Project command is blocked by current Charter state" => DeniedBy::CharterNotAdopted,
        "the Task workflow no longer admits writes in its terminal state" => DeniedBy::TaskTerminal,
        "reviewer assignments cannot perform Task writes" => DeniedBy::ReviewerReadOnly,
        "protected mutation requires an independent approval" => DeniedBy::IndependentApprovalRequired,
        "genesis.start requires an explicit user request to create or start a new Project" => DeniedBy::UserRequestRequired,
        "genesis.start requires the currently leased Main Chat turn" => DeniedBy::LeasedTurnRequired,
        "Project Charter adoption is not valid for the current Project state" => DeniedBy::CharterAdoptionNotApplicable,
        "query operations execute through the read boundary" => DeniedBy::ReadBoundaryRequired,
        "direct command is not admitted for this permission or payload" => DeniedBy::DirectCommandNotAdmitted,
        "the Task assignment does not admit review proposals" => DeniedBy::ReviewAssignmentRequired,
        "direct Project commands require the propose_project permission" => DeniedBy::PermissionMissing("propose_project".to_owned()),
        "Product Genesis start requires propose_discovery"
        | "Main Charter draft requires the propose_discovery permission" => DeniedBy::PermissionMissing("propose_discovery".to_owned()),
        "operation is denied by the canonical native operation catalog" => DeniedBy::OperationNotInScope,
        _ => DeniedBy::Unspecified,
    }
}

fn native_scope_error(error: crate::ServiceError) -> AgentHostError {
    match error {
        crate::ServiceError::AuthorizationDenied { message }
        | crate::ServiceError::InvalidOperation { message } => AgentHostError::Authority(message),
        crate::ServiceError::NotFound { .. } | crate::ServiceError::Db(db::DbError::NotFound) => {
            AgentHostError::Authority("the requested Forge scope is unavailable".to_owned())
        }
        crate::ServiceError::Db(_) => AgentHostError::ProtectedPersistence,
        _ => AgentHostError::ProtectedPersistence,
    }
}

/// An argument-shape rejection is the model's only path to a corrected call,
/// so it travels back verbatim as a structured validation outcome with a
/// `correct_input` retry. Raising it as a bare `Runtime` error renders as
/// "the Forge operation could not complete" — an internal failure the model
/// cannot act on, so it retries the same rejected shape or gives up on a
/// repair its doctrine requires. Only server-authored messages naming the
/// offending field belong here; never echo caller-supplied values.
fn invalid_arguments(message: String) -> AgentHostError {
    let mut outcome = OrchestrationOutcome::failed(
        OutcomeCode::ValidationError,
        "unknown",
        OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
        "",
        format!("the operation or arguments are not valid for this Forge surface ({message})"),
    );
    outcome.retry = Some(RetryInstruction::new(RetryAction::CorrectInput, false));
    AgentHostError::StructuredOutcome(Box::new(outcome))
}

fn service_error(error: crate::ServiceError) -> AgentHostError {
    if let crate::ServiceError::TurnFailure { error, .. } = error {
        return service_error(*error);
    }
    if let crate::ServiceError::TaskActionUnavailable {
        available_actions,
        reason,
        wait_cause,
    } = &error
    {
        let mut outcome = if let Some(cause) = wait_cause {
            OrchestrationOutcome::terminal_denial(
                "task.action",
                OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
                "",
                cause.clone(),
            )
        } else {
            OrchestrationOutcome::failed(
                OutcomeCode::ActionUnavailable,
                "task.action",
                OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
                "",
                reason.clone(),
            )
        };
        outcome.details = Some(json!({ "available_actions": available_actions }));
        if wait_cause.is_none() {
            outcome.retry = Some(RetryInstruction::new(RetryAction::CorrectInput, false));
        }
        return AgentHostError::StructuredOutcome(Box::new(outcome));
    }
    let target_refusal = match &error {
        crate::ServiceError::PlacementUnavailable(error) if error.needs_daemon_upgrade() => {
            Some(DeniedBy::DaemonUpgradeRequired)
        }
        crate::ServiceError::PlacementUnavailable(_) => Some(DeniedBy::PlacementUnavailable),
        crate::ServiceError::DaemonUpgradeRequired { .. } => Some(DeniedBy::DaemonUpgradeRequired),
        crate::ServiceError::WorkspaceResetRequired { .. } => {
            Some(DeniedBy::WorkspaceResetRequired)
        }
        _ => None,
    };
    if let Some(cause) = target_refusal {
        return AgentHostError::StructuredOutcome(Box::new(OrchestrationOutcome::terminal_denial(
            "unknown",
            OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
            "",
            cause,
        )));
    }
    match &error {
        crate::ServiceError::AuthorizationDenied { message } => {
            return AgentHostError::Authority(message.clone());
        }
        crate::ServiceError::AgentPaused { agent_id }
        | crate::ServiceError::Db(db::DbError::AgentPaused { agent_id }) => {
            return AgentHostError::AgentPaused {
                agent_id: agent_id.clone(),
            };
        }
        crate::ServiceError::ProjectPaused { project_id }
        | crate::ServiceError::Db(db::DbError::ProjectPaused { project_id }) => {
            return AgentHostError::ProjectPaused {
                project_id: project_id.clone(),
            };
        }
        crate::ServiceError::Db(db::DbError::Check(message))
            if native_denial_cause(message, Some("propose_project")) != DeniedBy::Unspecified =>
        {
            return AgentHostError::Authority(message.clone());
        }
        _ => {}
    }
    // A validation reason from the command boundary names the offending
    // input, so it returns to the model verbatim. Collapsing it to a bare
    // "not valid" leaves the model retrying the same rejected shape with
    // nothing to correct.
    if let crate::ServiceError::InvalidOperation { message }
    | crate::ServiceError::TerminalInvalidInput { message } = &error
    {
        return invalid_arguments(message.clone());
    }
    // A conflict's prose is the only account of what the caller got wrong, so
    // it is shown. It stays unstructured on purpose: read it, never parse it
    // to infer version or digest semantics.
    if let crate::ServiceError::Conflict(message) = &error {
        let mut outcome = OrchestrationOutcome::failed(
            OutcomeCode::ValidationError,
            "unknown",
            OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
            "",
            format!("the command could not be accepted; correct the typed input ({message})"),
        );
        outcome.retry = Some(RetryInstruction::new(RetryAction::CorrectInput, false));
        return AgentHostError::StructuredOutcome(Box::new(outcome));
    }
    // A `not_found` that names nothing is uncorrectable: a caller that passed
    // several ids in one payload cannot tell which one Forge could not resolve,
    // and retries the same rejected shape. The entity is a server-authored
    // static name and the id is the one the caller just supplied, so neither
    // discloses anything the caller did not already hold — the HTTP surface
    // returns exactly this pair for the same failure.
    if let crate::ServiceError::NotFound { entity, id } = &error {
        let mut outcome = OrchestrationOutcome::failed(
            OutcomeCode::NotFound,
            "unknown",
            OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
            "",
            format!("the requested Forge resource is unavailable ({entity} {id})"),
        );
        outcome.retry = Some(RetryInstruction::new(RetryAction::CorrectInput, false));
        return AgentHostError::StructuredOutcome(Box::new(outcome));
    }
    let (code, safe_message, setup_requirement, retry) = match error {
        crate::ServiceError::NotFound { .. } | crate::ServiceError::Db(db::DbError::NotFound) => (
            OutcomeCode::NotFound,
            "the requested Forge resource is unavailable",
            None,
            None,
        ),
        crate::ServiceError::Db(db::DbError::TurnNotRetryable | db::DbError::ChatTurnLive) => (
            OutcomeCode::ValidationError,
            "Agent Chat turn cannot be retried in its current state",
            None,
            None,
        ),
        crate::ServiceError::Db(db::DbError::IdempotencyConflict) => (
            OutcomeCode::IdempotencyConflict,
            "the idempotency key is already bound to a different command",
            None,
            Some(RetryInstruction::new(
                RetryAction::UseNewIdempotencyKey,
                false,
            )),
        ),
        crate::ServiceError::Db(
            db::DbError::VersionConflict
            | db::DbError::TaskVersionConflict { .. }
            | db::DbError::BoardRevisionConflict { .. }
            | db::DbError::MoveOperationConflict { .. },
        ) => (
            OutcomeCode::VersionConflict,
            "the authorized resource changed; refresh current state and retry",
            None,
            Some(RetryInstruction::new(RetryAction::RefreshAndRetry, true)),
        ),
        // ServiceError::Conflict intentionally has no structured discriminator;
        // never parse its prose to guess version or digest semantics.
        crate::ServiceError::Conflict(_) => (
            OutcomeCode::ValidationError,
            "the command could not be accepted; correct the typed input",
            None,
            None,
        ),
        crate::ServiceError::ProductGenesisActiveSession { .. } => (
            OutcomeCode::ActiveSessionConflict,
            "a Product Genesis discovery session is already active",
            None,
            None,
        ),
        crate::ServiceError::AuthorizationDenied { .. } => unreachable!("handled above"),
        crate::ServiceError::InvalidOperation { .. }
        | crate::ServiceError::TerminalInvalidInput { .. } => (
            OutcomeCode::ValidationError,
            "the operation or arguments are not valid for this Forge surface",
            None,
            Some(RetryInstruction::new(RetryAction::CorrectInput, false)),
        ),
        crate::ServiceError::ExecutionSetupRequired { requirements, .. } => (
            OutcomeCode::SetupRequired,
            "required Forge setup is incomplete",
            requirements.first().cloned(),
            Some(RetryInstruction::new(RetryAction::CompleteSetup, true)),
        ),
        crate::ServiceError::DependencyGate
        | crate::ServiceError::MissingPrimaryRepo { .. }
        | crate::ServiceError::PrimaryRepoNotFound { .. }
        | crate::ServiceError::RepoMismatch { .. }
        | crate::ServiceError::TerminalWorkspaceNotReady
        | crate::ServiceError::TerminalDisabled => (
            OutcomeCode::SetupRequired,
            "required Forge setup is incomplete",
            Some(SetupRequirement::new("forge_setup")),
            Some(RetryInstruction::new(RetryAction::CompleteSetup, false)),
        ),
        crate::ServiceError::TaskActionUnavailable { .. } => (
            OutcomeCode::SetupRequired,
            "the requested Forge action is not currently available",
            Some(SetupRequirement::new("task_action")),
            Some(RetryInstruction::new(RetryAction::RefreshAndRetry, true)),
        ),
        crate::ServiceError::RateLimited {
            retry_after_seconds,
        } => {
            let mut retry = RetryInstruction::new(RetryAction::RetryAfter, true);
            retry.after_seconds = Some(retry_after_seconds);
            (
                OutcomeCode::TransientFailure,
                "the Forge operation is temporarily rate limited",
                None,
                Some(retry),
            )
        }
        crate::ServiceError::DaemonNotReady { .. }
        | crate::ServiceError::PrepareFailed { .. }
        | crate::ServiceError::DaemonUnavailable { .. }
        | crate::ServiceError::DaemonTimeout { .. }
        | crate::ServiceError::TerminalDaemonUnavailable { .. }
        | crate::ServiceError::TerminalSessionLimit { .. } => (
            OutcomeCode::TransientFailure,
            "the Forge operation is temporarily unavailable; retry later",
            None,
            Some(RetryInstruction::new(RetryAction::RetryAfter, true)),
        ),
        other => {
            // The outer boundary adds the authoritative correlation id before
            // recording an operator diagnostic.  Keep this mapper itself
            // free of model-visible implementation details.
            let _ = other;
            (
                OutcomeCode::InternalFailure,
                "the Forge operation could not complete",
                None,
                None,
            )
        }
    };
    let mut outcome = OrchestrationOutcome::failed(
        code,
        "unknown",
        OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
        "",
        safe_message,
    );
    outcome.setup_requirements = setup_requirement.map(|requirement| vec![requirement]);
    outcome.retry = retry;
    AgentHostError::StructuredOutcome(Box::new(outcome))
}

fn scope_type_name(scope_type: CanonicalScopeType) -> &'static str {
    match scope_type {
        CanonicalScopeType::Account => "account",
        CanonicalScopeType::Project => "project",
        CanonicalScopeType::AgentChat => "agent_chat",
        CanonicalScopeType::Task => "task",
    }
}

fn workspace_access_name(access: WorkspaceAccess) -> &'static str {
    match access {
        WorkspaceAccess::Deny => "deny",
        WorkspaceAccess::TaskRead => "task_read",
        WorkspaceAccess::TaskWrite => "task_write",
        WorkspaceAccess::ProjectVerify => "project_verify",
        WorkspaceAccess::AccountScratch => "account_scratch",
    }
}

fn permission_set(value: &str) -> BTreeSet<String> {
    let Ok(value) = serde_json::from_str::<Value>(value) else {
        return BTreeSet::new();
    };
    match value {
        Value::Array(values) => values
            .into_iter()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .collect(),
        Value::Object(map) => map
            .get("permissions")
            .or_else(|| map.get("allowed"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .collect(),
        _ => BTreeSet::new(),
    }
}

fn truncate(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

/// A captured artifact is evidence, not a transfer channel: keep it small
/// enough that a runaway capture cannot fill the media store.
struct StoredEvidence {
    media_id: String,
    asset_id: String,
    checksum: String,
    byte_size: i64,
}

/// The execution whose outbox is ingested. Provenance comes from here, never
/// from the files the harness wrote.
#[derive(Debug, Clone, Copy)]
pub struct ExecutionOutboxInput<'a> {
    pub task_id: &'a str,
    pub execution_id: &'a str,
    pub agent_id: &'a str,
    pub role: Option<&'a str>,
    pub worktree_path: &'a str,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExecutionOutboxReport {
    pub worklog_entries: usize,
    pub evidence_items: usize,
    /// One human-readable reason per entry that was not ingested.
    pub rejected: Vec<String>,
    pub(crate) plan_candidate: bool,
    pub(crate) plan_rejected: bool,
}

/// At most this many bytes of one outbox file are read.
const MAX_OUTBOX_FILE_BYTES: u64 = 1024 * 1024;
/// At most this many entries of one outbox file are ingested.
const MAX_OUTBOX_ENTRIES: usize = 200;

fn evidence_kind_and_caption(payload: &Value) -> Result<(&str, String), String> {
    let kind = payload
        .get("kind")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or("kind must be a non-empty string")?;
    let caption = payload
        .get("caption")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or("caption describing the artifact is required")?;
    match kind {
        "screenshot" | "walkthrough_video" | "log" | "report" | "other" => {
            Ok((kind, caption.to_owned()))
        }
        _ => Ok(("other", format!("[{kind}] {caption}"))),
    }
}

fn outbox_evidence_media_id(execution_id: &str, position: &str) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("forge:execution:{execution_id}:outbox-evidence:{position}").as_bytes(),
    )
    .to_string()
}

/// JSON objects from a bounded file, including pretty-printed and concatenated
/// objects. Malformed content is reported before resuming at the next line.
fn read_outbox_entries(
    outbox: &Path,
    file: &str,
    report: &mut ExecutionOutboxReport,
) -> Vec<(String, Result<Value, String>)> {
    let path = outbox.join(file);
    let bytes = match read_bounded_regular_file(&path, outbox, MAX_OUTBOX_FILE_BYTES, true) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Vec::new(),
        Err(error) => {
            report.rejected.push(format!("{file}: unreadable: {error}"));
            return Vec::new();
        }
    };
    let text = match String::from_utf8(bytes) {
        Ok(text) => text,
        Err(_) => {
            report
                .rejected
                .push(format!("{file}: unreadable: content is not valid UTF-8"));
            return Vec::new();
        }
    };
    let line_starts = std::iter::once(0)
        .chain(text.match_indices('\n').map(|(offset, _)| offset + 1))
        .collect::<Vec<_>>();
    let mut entries = Vec::new();
    let mut offset = 0;
    let mut previous_line = 0;
    while offset < text.len() {
        offset += text.as_bytes()[offset..]
            .iter()
            .take_while(|byte| matches!(**byte, b' ' | b'\t' | b'\r' | b'\n'))
            .count();
        if offset == text.len() {
            break;
        }
        if entries.len() == MAX_OUTBOX_ENTRIES {
            report.rejected.push(format!(
                "{file}: only the first {MAX_OUTBOX_ENTRIES} entries were ingested"
            ));
            break;
        }
        let line_no = line_starts.partition_point(|start| *start <= offset);
        // Preserve existing JSONL receipt keys; a second object on the same
        // physical line also needs its byte column to remain distinct.
        let position = if line_no == previous_line {
            format!("{line_no}:{}", offset - line_starts[line_no - 1] + 1)
        } else {
            line_no.to_string()
        };
        previous_line = line_no;
        let mut stream = serde_json::Deserializer::from_str(&text[offset..]).into_iter::<Value>();
        let Some(entry) = stream.next() else {
            break;
        };
        match entry {
            Ok(value) => {
                offset += stream.byte_offset();
                entries.push((
                    position,
                    if value.is_object() {
                        Ok(value)
                    } else {
                        Err("entry must be a JSON object".to_owned())
                    },
                ));
            }
            Err(error) => {
                entries.push((position, Err(error.to_string())));
                offset += text[offset..]
                    .find('\n')
                    .map_or(text.len() - offset, |index| index + 1);
            }
        }
    }
    entries
}

/// Resolve an outbox evidence path: worktree-relative, or absolute inside the
/// outbox (where a read-only role saves what it captured).
fn resolve_outbox_artifact(
    worktree: &Path,
    outbox: &Path,
    path: &str,
) -> Result<(PathBuf, PathBuf, bool), String> {
    if !Path::new(path).is_absolute() {
        return resolve_workspace_artifact(worktree, path)
            .map(|path| (path, worktree.to_path_buf(), false));
    }
    let canonical_outbox = outbox
        .canonicalize()
        .map_err(|error| format!("outbox is unavailable: {error}"))?;
    let canonical = Path::new(path)
        .canonicalize()
        .map_err(|error| format!("captured artifact does not exist: {error}"))?;
    if !canonical.starts_with(&canonical_outbox) {
        return Err(
            "an absolute evidence path must point inside the outbox; use a worktree-relative path otherwise"
                .to_owned(),
        );
    }
    if !canonical.is_file() {
        return Err("captured artifact is not a file".to_owned());
    }
    Ok((PathBuf::from(path), outbox.to_path_buf(), true))
}

/// Open and read a regular file within one allowed root while enforcing the
/// byte limit on the opened handle. The metadata comparison prevents a leaf
/// replacement between validation and open from being accepted, and outbox
/// callers also require a single link so a hard link cannot turn the outbox
/// into a read channel for another file.
fn read_bounded_regular_file(
    path: &Path,
    allowed_root: &Path,
    max_bytes: u64,
    require_single_link: bool,
) -> Result<Option<Vec<u8>>, String> {
    let leaf_metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    if !leaf_metadata.file_type().is_file() {
        return Err("not a regular file".to_owned());
    }

    let canonical_root = fs::canonicalize(allowed_root).map_err(|error| error.to_string())?;
    let canonical_path = fs::canonicalize(path).map_err(|error| error.to_string())?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err("path escapes its allowed root".to_owned());
    }
    let path_metadata = fs::symlink_metadata(&canonical_path).map_err(|error| error.to_string())?;
    if !path_metadata.file_type().is_file() {
        return Err("not a regular file".to_owned());
    }
    let file = File::open(&canonical_path).map_err(|error| error.to_string())?;
    let opened_metadata = file.metadata().map_err(|error| error.to_string())?;
    if !same_open_file(&path_metadata, &opened_metadata) {
        return Err("file changed while it was being opened".to_owned());
    }
    #[cfg(unix)]
    if require_single_link {
        use std::os::unix::fs::MetadataExt;
        if opened_metadata.nlink() != 1 {
            return Err("file must not be a hard link".to_owned());
        }
    }
    if opened_metadata.len() > max_bytes {
        return Err(format!("exceeds the {max_bytes} byte limit"));
    }

    let mut bytes = Vec::with_capacity(usize::try_from(opened_metadata.len()).unwrap_or(0));
    file.take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > max_bytes {
        return Err(format!("exceeds the {max_bytes} byte limit"));
    }
    Ok(Some(bytes))
}

#[cfg(unix)]
fn same_open_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_open_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.file_type() == right.file_type()
        && left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
}

const MAX_CAPTURED_EVIDENCE_BYTES: i64 = 25 * 1024 * 1024;

/// A worklog entry is a summary the next role reads, not a transcript.
const MAX_WORKLOG_SUMMARY_CHARS: usize = 4_000;

/// Inline only what an Agent can read and act on; anything larger is named
/// rather than pasted into a turn.
const MAX_INLINE_ARTIFACT_BYTES: i64 = 256 * 1024;
const MAX_INLINE_ARTIFACT_CHARS: usize = 8_000;

/// Resolve a caller-supplied artifact path inside the Task workspace.
/// Traversal, absolute paths, and symlinked escapes are all rejected: the
/// capture surface must never read a file the Task session could not read.
fn resolve_workspace_artifact(root: &Path, relative: &str) -> Result<PathBuf, String> {
    let candidate = Path::new(relative);
    if candidate.is_absolute() {
        return Err("captured artifact path must be workspace-relative".to_owned());
    }
    if candidate
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err("captured artifact path escapes the Task workspace".to_owned());
    }
    let joined = root.join(candidate);
    let root_metadata = fs::symlink_metadata(root)
        .map_err(|error| format!("Task workspace is unavailable: {error}"))?;
    if !root_metadata.file_type().is_dir() {
        return Err("Task workspace must be a real directory".to_owned());
    }
    let leaf_metadata = fs::symlink_metadata(&joined)
        .map_err(|error| format!("captured artifact does not exist: {error}"))?;
    if !leaf_metadata.file_type().is_file() {
        return Err("captured artifact must be a regular file, not a symlink".to_owned());
    }
    let canonical_root = root
        .canonicalize()
        .map_err(|error| format!("Task workspace is unavailable: {error}"))?;
    let canonical = joined
        .canonicalize()
        .map_err(|error| format!("captured artifact does not exist: {error}"))?;
    if !canonical.starts_with(&canonical_root) {
        return Err("captured artifact path escapes the Task workspace".to_owned());
    }
    if !canonical.is_file() {
        return Err("captured artifact is not a file".to_owned());
    }
    Ok(joined)
}

/// Build the on-disk destination for a storage key without letting the key
/// itself walk out of the media root.
fn safe_media_destination(root: &Path, storage_key: &str) -> Result<PathBuf, AgentHostError> {
    let candidate = Path::new(storage_key);
    if candidate.is_absolute()
        || candidate
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(AgentHostError::Authority(
            "media storage key is invalid".to_owned(),
        ));
    }
    Ok(root.join(candidate))
}

fn content_type_for(filename: &str, kind: &str) -> String {
    let extension = Path::new(filename)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    match extension.as_str() {
        "png" => "image/png".to_owned(),
        "jpg" | "jpeg" => "image/jpeg".to_owned(),
        "gif" => "image/gif".to_owned(),
        "webp" => "image/webp".to_owned(),
        "webm" => "video/webm".to_owned(),
        "mp4" => "video/mp4".to_owned(),
        "json" => "application/json".to_owned(),
        "txt" | "log" | "md" => "text/plain".to_owned(),
        _ if kind == "screenshot" => "image/png".to_owned(),
        _ if kind == "walkthrough_video" => "video/webm".to_owned(),
        _ => "application/octet-stream".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn native_work_condition_projection_uses_all_seven_kinds_without_legacy_aliases() {
        let fixture = native_plan_fixture("coder", "coder").await;
        let e = db::ConditionEvidence::default();
        let reason = db::ParkReason::Held {
            actor: "user".into(),
        };
        for c in [
            db::TaskCondition::Clear {
                evidence: e.clone(),
            },
            db::TaskCondition::Entering {
                state: "review".into(),
                epoch: 1,
                step_id: "step".into(),
                phase: "checks".into(),
                since: "now".into(),
                evidence: e.clone(),
            },
            db::TaskCondition::Running {
                execution_id: fixture.execution_id.clone(),
                role: "coder".into(),
                epoch: 1,
                since: "now".into(),
                evidence: e.clone(),
            },
            db::TaskCondition::Deferred {
                until: None,
                reason: db::RetryCause::Legacy,
                resume: db::ConditionContinuation::Reconcile,
                evidence: e.clone(),
            },
            db::TaskCondition::Parked {
                primary: reason.clone(),
                additional: vec![],
                resume: db::ConditionContinuation::Reconcile,
                since: None,
                evidence: e.clone(),
            },
            db::TaskCondition::Failed {
                failure: reason,
                additional: vec![],
                resume: db::ConditionContinuation::Reconcile,
                since: None,
                evidence: e.clone(),
            },
            db::TaskCondition::Settled {
                outcome: db::TerminalOutcome::Completed,
                evidence: e,
            },
        ] {
            sqlx::query("UPDATE task SET condition_json=?,error_annotation='legacy poison',blocked_json='legacy poison',failed_json='legacy poison' WHERE id=?").bind(serde_json::to_string(&c).unwrap()).bind(&fixture.task_id).execute(fixture.db.pool()).await.unwrap();
            let value = fixture
                .provider
                .read_work(&fixture.agent_id, &fixture.scope, 10)
                .await
                .unwrap();
            let item = &value["items"][0];
            assert_eq!(item["condition"], serde_json::to_value(c.public()).unwrap());
            for removed in ["blocked", "error", "failed", "error_annotation"] {
                assert!(item.get(removed).is_none(), "{removed}");
            }
        }
    }

    struct NativePlanFixture {
        db: Arc<db::SqliteDb>,
        provider: CoordinationToolProvider,
        _temp: tempfile::TempDir,
        agent_id: String,
        runtime_session_id: String,
        task_id: String,
        execution_id: String,
        workspace_id: String,
        worktree: PathBuf,
        scope: CanonicalScope,
    }

    async fn native_plan_fixture(task_role: &str, execution_role: &str) -> NativePlanFixture {
        native_plan_fixture_with_permissions(task_role, execution_role, "{}").await
    }

    async fn native_plan_fixture_with_permissions(
        task_role: &str,
        execution_role: &str,
        permissions: &str,
    ) -> NativePlanFixture {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations run");
        let db = Arc::new(db::SqliteDb::new(pool));
        let now = db::now_rfc3339();
        let project_id = db::new_uuid_v4();
        let repo_id = db::new_uuid_v4();
        let task_id = db::new_uuid_v4();
        let workspace_id = db::new_uuid_v4();
        let execution_id = db::new_uuid_v4();
        let agent_id = db::new_uuid_v4();
        let profile_id = db::new_uuid_v4();
        let context_scope_id = db::new_uuid_v4();
        let runtime_session_id = db::new_uuid_v4();
        let temp = tempfile::tempdir().expect("temp dir creates");
        let worktree = temp.path().join(&task_id).join("repo");
        std::fs::create_dir_all(&worktree).expect("worktree creates");

        let project = db::ProjectRepo::create(
            &*db,
            db::CreateProject {
                id: project_id.clone(),
                name: "Native plan project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");
        db::RepoRepo::create(
            &*db,
            db::CreateRepo {
                id: repo_id.clone(),
                project_id: project_id.clone(),
                name: "repo".to_owned(),
                remote_url: Some("https://example.invalid/repo.git".to_owned()),
                local_path: None,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("repo creates");
        db::ProjectRepo::update_at_version(
            &*db,
            db::UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_id.clone())),
                paused_at: None,
                updated_at: now.clone(),
            },
            project.version,
            None,
        )
        .await
        .expect("primary repo binds");
        db::TaskRepo::create(
            &*db,
            db::CreateTask {
                id: task_id.clone(),
                project_id: project_id.clone(),
                parent_task_id: None,
                assignee_type: None,
                assignee_id: None,
                title: "Write a native plan".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "planning".to_owned(),
                is_automation: false,
                priority: 0,
                subtask_order: None,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("task creates");
        db::WorkspaceRepo::create(
            &*db,
            db::CreateWorkspace {
                id: workspace_id.clone(),
                task_id: task_id.clone(),
                repo_id,
                worktree_path: worktree.to_string_lossy().into_owned(),
                branch: workspace::task_branch_name(&task_id),
                status: db::WorkspaceStatus::Ready,
                before_sha: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("workspace creates");
        db::AgentRepo::create_identity_with_profile(
            &*db,
            db::CreateAgentIdentity {
                id: agent_id.clone(),
                name: "native-plan-agent".to_owned(),
                description: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: db::AgentStatus::Busy,
                last_heartbeat_at: Some(now.clone()),
                is_default: false,
                paused: false,
                owner_id: None,
                visibility: "global".to_owned(),
                account_permission_ceiling: permissions.to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            db::CreateAgentProfile {
                id: profile_id.clone(),
                identity_id: agent_id.clone(),
                backend_kind: "native".to_owned(),
                executor_type: "native".to_owned(),
                provider: None,
                model: Some("test-model".to_owned()),
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "{}".to_owned(),
                tool_policy_json: permissions.to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("agent identity creates");
        db::AgentContextScopeRepo::create_context_scope(
            &*db,
            db::CreateAgentContextScope {
                id: context_scope_id.clone(),
                identity_id: agent_id.clone(),
                scope_type: "task".to_owned(),
                scope_id: task_id.clone(),
                project_id: Some(project_id),
                task_id: Some(task_id.clone()),
                task_role: Some(task_role.to_owned()),
                workspace_access: "task_write".to_owned(),
                authority_json: "{}".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
                workspace_path: None,
            },
        )
        .await
        .expect("context scope creates");
        db::AgentSessionRepo::create_agent_session(
            &*db,
            db::CreateAgentSession {
                id: db::new_uuid_v4(),
                identity_id: agent_id.clone(),
                profile_id,
                context_scope_id,
                backend_kind: "native".to_owned(),
                runtime_session_id: Some(runtime_session_id.clone()),
                status: "running".to_owned(),
                capabilities_json: "{}".to_owned(),
                connection_status: "healthy".to_owned(),
                predecessor_session_id: None,
                last_activity_at: Some(now.clone()),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("agent session creates");
        db::ExecutionRepo::create(
            &*db,
            db::CreateExecution {
                id: execution_id.clone(),
                task_id: task_id.clone(),
                agent_id: Some(agent_id.clone()),
                role: execution_role.to_owned(),
                status: db::ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: Some(runtime_session_id.clone()),
                agent_message_id: None,
                last_activity_at: Some(now.clone()),
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: Some(
                    r#"{"executor_type":"native","plan_delivery":"execution_outbox"}"#.to_owned(),
                ),
                workspace_id: Some(workspace_id.clone()),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("execution creates");

        let provider = CoordinationToolProvider::new(Arc::clone(&db));
        NativePlanFixture {
            db,
            provider,
            _temp: temp,
            agent_id,
            runtime_session_id,
            task_id: task_id.clone(),
            execution_id,
            workspace_id,
            worktree,
            scope: CanonicalScope {
                scope_type: CanonicalScopeType::Task,
                scope_id: task_id,
                workspace_access: WorkspaceAccess::TaskWrite,
            },
        }
    }

    fn assert_correctable_plan_validation(error: AgentHostError) {
        match error {
            AgentHostError::StructuredOutcome(outcome) => {
                assert_eq!(outcome.code, OutcomeCode::ValidationError);
                assert_eq!(outcome.status, OutcomeStatus::Failed);
                assert_eq!(
                    outcome.retry.as_ref().map(|retry| retry.action),
                    Some(RetryAction::CorrectInput)
                );
                assert!(outcome.safe_message.contains("Markdown checklist"));
            }
            other => panic!("invalid plan input must be correctable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn native_task_plan_writes_only_the_owning_execution_outbox() {
        let fixture = native_plan_fixture("planner", "planner").await;
        let result = fixture
            .provider
            .execute_task_plan_write(
                &fixture.agent_id,
                &fixture.runtime_session_id,
                &fixture.scope,
                &json!({"action": "write", "content": "# Plan\n\n- [ ] implement it\n"}),
            )
            .await
            .expect("owning planner session writes a plan candidate");

        assert_eq!(result["task_id"], fixture.task_id);
        assert_eq!(result["execution_id"], fixture.execution_id);
        assert_eq!(result["checklist_items"], 1);
        let outbox = executors::execution_outbox_path(&fixture.worktree, &fixture.execution_id)
            .expect("outbox path");
        assert_eq!(
            std::fs::read_to_string(outbox.join("plan.md")).expect("outbox plan reads"),
            "# Plan\n\n- [ ] implement it\n"
        );
        assert!(
            !fixture
                .worktree
                .parent()
                .expect("Task directory")
                .join("plan.md")
                .exists(),
            "the native tool must not write the canonical Task plan"
        );
    }

    #[tokio::test]
    async fn native_task_plan_denies_a_reviewer_session() {
        let fixture = native_plan_fixture("reviewer", "reviewer").await;
        let error = fixture
            .provider
            .execute_task_plan_write(
                &fixture.agent_id,
                &fixture.runtime_session_id,
                &fixture.scope,
                &json!({"action": "write", "content": "- [ ] must not publish\n"}),
            )
            .await
            .expect_err("reviewer session has no plan-write authority");

        assert!(matches!(error, AgentHostError::Authority(_)));
        let outbox = executors::execution_outbox_path(&fixture.worktree, &fixture.execution_id)
            .expect("outbox path");
        assert!(!outbox.exists());
    }

    #[tokio::test]
    async fn native_task_plan_fails_closed_for_duplicate_running_executions() {
        let fixture = native_plan_fixture("planner", "planner").await;
        let now = db::now_rfc3339();
        // Reproduce a legacy/corrupt duplicate directly: current admission
        // prevents creating two live rows for one workspace, while this tool
        // still has to fail closed if an upgraded database already has them.
        sqlx::query(
            "INSERT INTO execution (
                 id, task_id, agent_id, role, status, agent_session_id,
                 workspace_id, created_at, updated_at
             ) VALUES (?, ?, ?, 'planner', 'running', ?, ?, ?, ?)",
        )
        .bind(db::new_uuid_v4())
        .bind(&fixture.task_id)
        .bind(&fixture.agent_id)
        .bind(&fixture.runtime_session_id)
        .bind(&fixture.workspace_id)
        .bind(&now)
        .bind(&now)
        .execute(fixture.db.pool())
        .await
        .expect("duplicate running execution creates");

        let error = fixture
            .provider
            .execute_task_plan_write(
                &fixture.agent_id,
                &fixture.runtime_session_id,
                &fixture.scope,
                &json!({"action": "write", "content": "- [ ] ambiguous\n"}),
            )
            .await
            .expect_err("ambiguous execution authority fails closed");
        assert!(matches!(error, AgentHostError::Authority(_)));
        let outbox = executors::execution_outbox_path(&fixture.worktree, &fixture.execution_id)
            .expect("outbox path");
        assert!(!outbox.exists());
    }

    #[tokio::test]
    async fn invalid_native_task_plan_preserves_the_previous_candidate() {
        let fixture = native_plan_fixture("planner", "planner").await;
        let original = "# Plan\n\n- [ ] keep this candidate\n";
        fixture
            .provider
            .execute_task_plan_write(
                &fixture.agent_id,
                &fixture.runtime_session_id,
                &fixture.scope,
                &json!({"action": "write", "content": original}),
            )
            .await
            .expect("initial plan candidate writes");
        let outbox = executors::execution_outbox_path(&fixture.worktree, &fixture.execution_id)
            .expect("outbox path");
        let plan_path = outbox.join("plan.md");
        let before = std::fs::read(&plan_path).expect("initial candidate reads");

        let error = fixture
            .provider
            .execute_task_plan_write(
                &fixture.agent_id,
                &fixture.runtime_session_id,
                &fixture.scope,
                &json!({"action": "write", "content": "# Plan without a checklist\n"}),
            )
            .await
            .expect_err("a checklist-free plan is invalid");

        assert_correctable_plan_validation(error);
        assert_eq!(
            std::fs::read(plan_path).expect("preserved candidate reads"),
            before,
            "validation must happen before replacement"
        );
    }

    #[tokio::test]
    async fn retained_invalid_plan_outbox_replays_worklog_and_evidence_exactly_once() {
        let fixture = native_plan_fixture("planner", "planner").await;
        let media_root = fixture._temp.path().join("media");
        fixture.provider.set_media_root(media_root.clone());
        let outbox = executors::execution_outbox_path(&fixture.worktree, &fixture.execution_id)
            .expect("outbox path");
        std::fs::create_dir_all(&outbox).expect("outbox creates");
        std::fs::write(outbox.join("plan.md"), "# Plan without checklist\n")
            .expect("invalid plan writes");
        std::fs::write(
            outbox.join(executors::OUTBOX_WORKLOG_FILE),
            "{\"kind\":\"validation\",\"summary\":\"focused tests passed\"}\n",
        )
        .expect("worklog writes");
        std::fs::write(
            outbox.join(executors::OUTBOX_EVIDENCE_FILE),
            concat!(
                r#"{"kind":"log","caption":"focused tests passed","content":"OK"}"#,
                "\n",
            ),
        )
        .expect("evidence writes");

        // Reproduce a stop after the media commit but before the caption
        // comment. The next ingestion must resolve this durable identity.
        let media_id = outbox_evidence_media_id(&fixture.execution_id, "1");
        fixture
            .provider
            .store_task_evidence_with_id(
                &fixture.agent_id,
                &fixture.task_id,
                "log.txt",
                "text/plain".to_owned(),
                b"OK",
                media_id.clone(),
            )
            .await
            .expect("interrupted media commit creates");

        let worktree_path = fixture.worktree.to_string_lossy().into_owned();
        let input = ExecutionOutboxInput {
            task_id: &fixture.task_id,
            execution_id: &fixture.execution_id,
            agent_id: &fixture.agent_id,
            role: Some("planner"),
            worktree_path: &worktree_path,
        };
        let first = fixture.provider.ingest_execution_outbox(&input).await;
        assert_eq!(first.worklog_entries, 1, "{first:?}");
        assert_eq!(first.evidence_items, 1, "{first:?}");
        assert!(first.plan_rejected, "{first:?}");
        assert_eq!(first.rejected.len(), 1, "{first:?}");
        assert!(first.rejected[0].starts_with("plan.md:"));
        assert!(outbox.exists(), "the rejected plan retains its outbox");

        let replay = fixture.provider.ingest_execution_outbox(&input).await;
        assert_eq!(replay.worklog_entries, 1, "{replay:?}");
        assert_eq!(replay.evidence_items, 1, "{replay:?}");
        assert!(replay.plan_rejected, "{replay:?}");
        assert_eq!(replay.rejected, first.rejected);
        assert!(outbox.exists(), "the rejected plan remains diagnosable");

        let media = db::TaskMediaRepo::list_active_media_for_task(&*fixture.db, &fixture.task_id)
            .await
            .expect("media lists");
        assert_eq!(media.len(), 1, "replay must not create another media row");
        assert_eq!(media[0].id, media_id);
        assert!(
            media_root.join(&media[0].storage_key).is_file(),
            "the single media row keeps one stored artifact"
        );
        let asset_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM media_asset WHERE legacy_task_media_id = ?")
                .bind(&media_id)
                .fetch_one(fixture.db.pool())
                .await
                .expect("asset count");
        assert_eq!(asset_count, 1, "replay must not create another media asset");

        let comments = db::TaskCommentRepo::list_comments(
            &*fixture.db,
            &fixture.task_id,
            db::PageRequest {
                cursor: None,
                limit: 10,
                include_total: false,
                sort_by: db::SortBy::CreatedAt,
                sort_order: db::SortOrder::Asc,
            },
        )
        .await
        .expect("comments list")
        .items;
        assert_eq!(comments.len(), 2, "replay must not append another entry");
        let worklog_key = format!("outbox:{}:worklog:1", fixture.execution_id);
        assert_eq!(
            comments[0].idempotency_key.as_deref(),
            Some(worklog_key.as_str())
        );
        let receipt_key = format!("outbox:{}:evidence:1", fixture.execution_id);
        assert_eq!(
            comments[1].idempotency_key.as_deref(),
            Some(receipt_key.as_str())
        );
    }

    #[tokio::test]
    async fn execution_outbox_normalizes_unknown_evidence_kinds() {
        let fixture = native_plan_fixture("worker", "worker").await;
        fixture
            .provider
            .set_media_root(fixture._temp.path().join("media"));
        let outbox = executors::prepare_execution_outbox(&fixture.worktree, &fixture.execution_id)
            .expect("outbox creates");
        std::fs::write(fixture.worktree.join("benchmark.log"), "120 ops/s\n")
            .expect("benchmark writes");
        std::fs::write(
            outbox.join(executors::OUTBOX_EVIDENCE_FILE),
            concat!(
                r#"{"kind":"test","caption":"focused tests","content":"20 tests OK"}"#,
                "\n",
                r#"{"kind":"benchmark","caption":"throughput","path":"benchmark.log"}"#,
                "\n",
                r#"{"kind":"log","caption":"standard kind","content":"OK"}"#,
                "\n",
                r#"{"caption":"missing kind","content":"OK"}"#,
                "\n",
                r#"{"kind":7,"caption":"invalid kind","content":"OK"}"#,
                "\n",
            ),
        )
        .expect("evidence writes");
        let worktree_path = fixture.worktree.to_string_lossy().into_owned();
        let report = fixture
            .provider
            .ingest_execution_outbox(&ExecutionOutboxInput {
                task_id: &fixture.task_id,
                execution_id: &fixture.execution_id,
                agent_id: &fixture.agent_id,
                role: Some("worker"),
                worktree_path: &worktree_path,
            })
            .await;

        assert_eq!(report.evidence_items, 3, "{report:?}");
        assert_eq!(report.rejected.len(), 2, "{report:?}");
        assert!(report
            .rejected
            .iter()
            .all(|reason| reason.contains("kind must be a non-empty string")));
        let comments: Vec<String> =
            sqlx::query_scalar("SELECT content FROM task_comment WHERE task_id = ?")
                .bind(&fixture.task_id)
                .fetch_all(fixture.db.pool())
                .await
                .expect("comments list");
        assert!(comments
            .contains(&"Captured other evidence `other.txt`: [test] focused tests".to_owned()));
        assert!(comments.contains(
            &"Captured other evidence `benchmark.log`: [benchmark] throughput".to_owned()
        ));
        assert!(comments.contains(&"Captured log evidence `log.txt`: standard kind".to_owned()));
        let media = db::TaskMediaRepo::list_active_media_for_task(&*fixture.db, &fixture.task_id)
            .await
            .expect("media lists");
        assert_eq!(media.len(), 3);
    }

    #[tokio::test]
    async fn native_task_evidence_preserves_unknown_kind_in_stored_filename() {
        let fixture = native_plan_fixture("worker", "worker").await;
        fixture
            .provider
            .set_media_root(fixture._temp.path().join("media"));
        let result = fixture
            .provider
            .execute_task_evidence_capture(
                &fixture.agent_id,
                &fixture.scope,
                &json!({"kind": "test", "caption": "focused tests", "content": "OK"}),
            )
            .await
            .expect("unknown kind captures");

        assert_eq!(result["kind"], "other");
        assert_eq!(result["caption"], "[test] focused tests");
        let media = db::TaskMediaRepo::list_active_media_for_task(&*fixture.db, &fixture.task_id)
            .await
            .expect("media lists");
        assert_eq!(media.len(), 1);
        assert_eq!(media[0].display_filename, "[test] other.txt");
    }

    #[tokio::test]
    async fn execution_outbox_ingests_pretty_printed_and_concatenated_objects() {
        let fixture = native_plan_fixture("worker", "worker").await;
        fixture
            .provider
            .set_media_root(fixture._temp.path().join("media"));
        let outbox = executors::prepare_execution_outbox(&fixture.worktree, &fixture.execution_id)
            .expect("outbox creates");
        std::fs::write(outbox.join("plan.md"), "# Missing checklist\n")
            .expect("invalid plan writes");
        std::fs::write(
            outbox.join(executors::OUTBOX_WORKLOG_FILE),
            format!(
                "{}\n\n{}{}\n",
                serde_json::to_string_pretty(&json!({"kind": "progress", "summary": "approach"}))
                    .expect("pretty worklog serializes"),
                json!({"kind": "decision", "summary": "implementation"}),
                json!({"kind": "validation", "summary": "verification"}),
            ),
        )
        .expect("worklog writes");
        std::fs::write(
            outbox.join(executors::OUTBOX_EVIDENCE_FILE),
            format!(
                "{}\n{}{}\n",
                serde_json::to_string_pretty(
                    &json!({"kind": "log", "caption": "first", "content": "one"})
                )
                .expect("pretty evidence serializes"),
                json!({"kind": "log", "caption": "second", "content": "two"}),
                json!({"kind": "log", "caption": "third", "content": "three"}),
            ),
        )
        .expect("evidence writes");
        let worktree_path = fixture.worktree.to_string_lossy().into_owned();
        let input = ExecutionOutboxInput {
            task_id: &fixture.task_id,
            execution_id: &fixture.execution_id,
            agent_id: &fixture.agent_id,
            role: Some("worker"),
            worktree_path: &worktree_path,
        };
        for _ in 0..2 {
            let report = fixture.provider.ingest_execution_outbox(&input).await;
            assert_eq!(report.worklog_entries, 3, "{report:?}");
            assert_eq!(report.evidence_items, 3, "{report:?}");
            assert_eq!(report.rejected.len(), 1, "{report:?}");
            assert!(report.rejected[0].starts_with("plan.md:"));
        }
        let comments: Vec<(String, String)> =
            sqlx::query_as("SELECT idempotency_key, content FROM task_comment WHERE task_id = ?")
                .bind(&fixture.task_id)
                .fetch_all(fixture.db.pool())
                .await
                .expect("comments list");
        assert_eq!(
            comments.len(),
            6,
            "each streamed object has a distinct receipt"
        );
        for summary in ["approach", "implementation", "verification"] {
            assert!(comments.iter().any(|(_, content)| content == summary));
        }
        assert!(comments
            .iter()
            .any(|(key, _)| key == &format!("outbox:{}:worklog:1", fixture.execution_id)));
        let media = db::TaskMediaRepo::list_active_media_for_task(&*fixture.db, &fixture.task_id)
            .await
            .expect("media lists");
        assert_eq!(
            media.len(),
            3,
            "replay does not duplicate concatenated artifacts"
        );
    }

    #[tokio::test]
    async fn concurrent_outbox_worklog_and_evidence_replays_are_already_ingested() {
        let fixture = native_plan_fixture("worker", "worker").await;
        fixture
            .provider
            .set_media_root(fixture._temp.path().join("media"));
        let outbox = executors::prepare_execution_outbox(&fixture.worktree, &fixture.execution_id)
            .expect("outbox creates");
        let worktree_path = fixture.worktree.to_string_lossy().into_owned();
        let input = ExecutionOutboxInput {
            task_id: &fixture.task_id,
            execution_id: &fixture.execution_id,
            agent_id: &fixture.agent_id,
            role: Some("worker"),
            worktree_path: &worktree_path,
        };
        let key = format!("outbox:{}:worklog:1", fixture.execution_id);
        let (first, replay) = tokio::join!(
            fixture.provider.append_outbox_worklog(
                &input,
                "worker",
                "validation",
                "OK",
                key.clone()
            ),
            fixture
                .provider
                .append_outbox_worklog(&input, "worker", "validation", "OK", key),
        );
        first.expect("first append succeeds");
        replay.expect("concurrent replay succeeds");
        let evidence = json!({"kind": "log", "caption": "focused tests", "content": "OK"});
        let (first, replay) = tokio::join!(
            fixture
                .provider
                .ingest_local_outbox_evidence(&input, "worker", &outbox, "1", &evidence),
            fixture
                .provider
                .ingest_local_outbox_evidence(&input, "worker", &outbox, "1", &evidence),
        );
        first.expect("first evidence capture succeeds");
        replay.expect("concurrent evidence replay succeeds");
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_comment WHERE task_id = ?")
            .bind(&fixture.task_id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("comments count");
        assert_eq!(count, 2, "one worklog entry and one evidence caption");
        let media = db::TaskMediaRepo::list_active_media_for_task(&*fixture.db, &fixture.task_id)
            .await
            .expect("media lists");
        assert_eq!(media.len(), 1);
    }

    #[tokio::test]
    async fn execution_outbox_ingests_valid_entries_and_reports_the_rest() {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations run");
        let db = Arc::new(db::SqliteDb::new(pool));
        let now = db::now_rfc3339();
        let project = db::ProjectRepo::create(
            &*db,
            db::CreateProject {
                id: db::new_uuid_v4(),
                name: "outbox".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");
        let task = db::TaskRepo::create(
            &*db,
            db::CreateTask {
                id: db::new_uuid_v4(),
                project_id: project.id.clone(),
                parent_task_id: None,
                assignee_type: None,
                assignee_id: None,
                title: "outbox".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "in_progress".to_owned(),
                is_automation: false,
                priority: 0,
                subtask_order: None,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("task creates");
        let now = db::now_rfc3339();
        db::ExecutionRepo::create(
            &*db,
            db::CreateExecution {
                id: "exec-1".to_owned(),
                task_id: task.id.clone(),
                agent_id: None,
                role: "reviewer".to_owned(),
                status: db::ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("execution creates");

        let temp = tempfile::tempdir().expect("temp dir");
        let worktree = temp.path().join(&task.id).join("repo");
        std::fs::create_dir_all(&worktree).expect("worktree");
        std::fs::write(worktree.join("run.log"), "20 tests OK\n").expect("artifact");
        let outbox = executors::execution_outbox_path(&worktree, "exec-1").expect("outbox path");
        std::fs::create_dir_all(&outbox).expect("outbox");
        std::fs::write(outbox.join("plan.md"), "- [ ] reviewer must not publish\n")
            .expect("reviewer plan");
        std::fs::write(outbox.join("shot.png"), [0x89, b'P', b'N', b'G']).expect("shot");
        std::fs::write(
            outbox.join(executors::OUTBOX_WORKLOG_FILE),
            concat!(
                r#"{"kind":"validation","summary":"unittest: 20 OK"}"#,
                "\n\n",
                r#"{"kind":"chatter","summary":"not a kind"}"#,
                "\nnot json\n",
                r#"{"kind":"progress","summary":"valid entry after malformed content"}"#,
                "\n",
            ),
        )
        .expect("worklog");
        std::fs::write(
            outbox.join(executors::OUTBOX_EVIDENCE_FILE),
            format!(
                "{}\n{}\n{}\n{}\n",
                r#"{"kind":"log","caption":"inline","content":"OK"}"#,
                r#"{"kind":"log","caption":"worktree file","path":"run.log"}"#,
                serde_json::json!({
                    "kind": "screenshot",
                    "caption": "saved in the outbox",
                    "path": outbox.join("shot.png"),
                }),
                r#"{"kind":"log","caption":"escape","path":"/etc/hosts"}"#,
            ),
        )
        .expect("evidence");

        let provider = CoordinationToolProvider::new(Arc::clone(&db));
        provider.set_media_root(temp.path().join("media"));
        let report = provider
            .ingest_execution_outbox(&ExecutionOutboxInput {
                task_id: &task.id,
                execution_id: "exec-1",
                agent_id: "agent-1",
                role: Some("reviewer"),
                worktree_path: &worktree.to_string_lossy(),
            })
            .await;

        assert_eq!(report.worklog_entries, 2, "{report:?}");
        assert_eq!(report.evidence_items, 3, "{report:?}");
        assert_eq!(report.rejected.len(), 3, "{report:?}");
        assert!(report.rejected[0].starts_with("worklog.jsonl:3: kind must be"));
        assert!(report.rejected[1].starts_with("worklog.jsonl:4: "));
        assert!(report.rejected[2].contains("evidence.jsonl:4: an absolute evidence path"));
        assert!(!outbox.exists(), "the outbox is consumed");
        assert!(
            !worktree
                .parent()
                .expect("task dir")
                .join("plan.md")
                .exists(),
            "a reviewer cannot publish a plan"
        );

        let comments = db::TaskCommentRepo::list_comments(
            &*db,
            &task.id,
            db::PageRequest {
                cursor: None,
                limit: 50,
                include_total: false,
                sort_by: db::SortBy::CreatedAt,
                sort_order: db::SortOrder::Asc,
            },
        )
        .await
        .expect("comments list")
        .items;
        assert_eq!(comments.len(), 5);
        assert!(comments.iter().all(|comment| {
            comment.execution_id.as_deref() == Some("exec-1")
                && comment.role.as_deref() == Some("reviewer")
                && comment.author_id.as_deref() == Some("agent-1")
        }));
        assert_eq!(comments[0].worklog_kind.as_deref(), Some("validation"));
        assert_eq!(comments[0].content, "unittest: 20 OK");
        assert!(comments.iter().any(|comment| {
            comment.content == "valid entry after malformed content"
                && comment.worklog_kind.as_deref() == Some("progress")
        }));
        assert!(comments
            .iter()
            .any(|comment| comment.content.contains("`shot.png`: saved in the outbox")));

        // A second completion pass finds nothing left to ingest.
        let replay = provider
            .ingest_execution_outbox(&ExecutionOutboxInput {
                task_id: &task.id,
                execution_id: "exec-1",
                agent_id: "agent-1",
                role: Some("reviewer"),
                worktree_path: &worktree.to_string_lossy(),
            })
            .await;
        assert_eq!(replay, ExecutionOutboxReport::default());

        let planner_outbox =
            executors::execution_outbox_path(&worktree, "planner-exec").expect("planner outbox");
        std::fs::create_dir_all(&planner_outbox).expect("planner outbox creates");
        std::fs::write(planner_outbox.join("plan.md"), "- [ ] implement\n").expect("planner plan");
        let planner_report = provider
            .ingest_execution_outbox(&ExecutionOutboxInput {
                task_id: &task.id,
                execution_id: "planner-exec",
                agent_id: "agent-1",
                role: Some("planner"),
                worktree_path: &worktree.to_string_lossy(),
            })
            .await;
        assert!(planner_report.plan_candidate, "{planner_report:?}");
        assert!(
            !planner_outbox.exists(),
            "the agent-writable outbox is removed after staging"
        );
        assert!(
            crate::plan_artifact::publish_staged_execution_plan(&worktree, "planner-exec")
                .expect("planner plan publishes")
        );
        crate::plan_artifact::discard_staged_execution_plan(&worktree, "planner-exec")
            .expect("settled planner stage removes");
        assert_eq!(
            std::fs::read_to_string(worktree.parent().expect("task dir").join("plan.md"))
                .expect("canonical plan reads"),
            "- [ ] implement\n"
        );

        let coder_outbox =
            executors::execution_outbox_path(&worktree, "coder-exec").expect("coder outbox");
        std::fs::create_dir_all(&coder_outbox).expect("coder outbox creates");
        std::fs::write(coder_outbox.join("plan.md"), "- [x] implement\n").expect("coder plan");
        let coder_report = provider
            .ingest_execution_outbox(&ExecutionOutboxInput {
                task_id: &task.id,
                execution_id: "coder-exec",
                agent_id: "agent-1",
                role: Some("coder"),
                worktree_path: &worktree.to_string_lossy(),
            })
            .await;
        assert!(coder_report.plan_candidate, "{coder_report:?}");
        assert!(
            crate::plan_artifact::publish_staged_execution_plan(&worktree, "coder-exec")
                .expect("coder plan publishes")
        );
        assert_eq!(
            std::fs::read_to_string(worktree.parent().expect("task dir").join("plan.md"))
                .expect("updated plan reads"),
            "- [x] implement\n"
        );
    }

    async fn assert_terminal_native_denial(reason: &str, expected: &str) {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let provider = CoordinationToolProvider::new(Arc::new(SqliteDb::new(pool)));
        let scope = CanonicalScope {
            scope_type: CanonicalScopeType::AgentChat,
            scope_id: "own-chat".to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        };
        let error = provider
            .structured_boundary_error(
                "own-identity",
                &scope,
                TASK_ACTION_OPERATION,
                &json!({}),
                service_error(crate::ServiceError::AuthorizationDenied {
                    message: reason.to_owned(),
                }),
            )
            .await;
        let AgentHostError::StructuredOutcome(outcome) = error else {
            panic!("typed denial")
        };
        assert_eq!(outcome.code, OutcomeCode::PolicyDenied);
        assert_eq!(
            outcome
                .denied_by
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some(expected)
        );
        let generic = expected == "unspecified";
        assert_eq!(outcome.safe_message.contains("Do not retry"), !generic);
        let retry = outcome.retry.unwrap();
        assert_eq!(retry.action, RetryAction::None);
        assert!(!retry.retryable);
    }

    #[tokio::test]
    async fn terminal_denial_names_missing_permission() {
        assert_terminal_native_denial(
            "permission propose_task is outside the server-issued identity/profile/scope ceiling",
            "permission_missing(propose_task)",
        )
        .await;
        assert_terminal_native_denial(
            "requested permission is outside the Agent Chat binding ceiling",
            "permission_missing(propose_task)",
        )
        .await;
    }

    #[tokio::test]
    async fn terminal_denial_names_paused_identity() {
        assert_terminal_native_denial("actor identity is paused or archived", "identity_paused")
            .await;
        assert_terminal_native_denial("agent principal is paused or archived", "identity_paused")
            .await;
    }

    #[tokio::test]
    async fn terminal_denial_names_unadopted_charter() {
        assert_terminal_native_denial(
            "Project orchestration remains blocked until a user-approved Charter adoption is committed",
            "charter_not_adopted",
        ).await;
    }

    #[tokio::test]
    async fn terminal_denial_names_operation_scope() {
        assert_terminal_native_denial(
            "operation is denied by the canonical native operation catalog",
            "operation_not_in_scope",
        )
        .await;
    }

    #[tokio::test]
    async fn terminal_denial_names_other_safe_evaluator_causes() {
        for (reason, cause) in [
            (
                "the Task workflow no longer admits writes in its terminal state",
                "task_terminal",
            ),
            (
                "reviewer assignments cannot perform Task writes",
                "reviewer_read_only",
            ),
            (
                "protected mutation requires an independent approval",
                "independent_approval_required",
            ),
            (
                "genesis.start requires an explicit user request to create or start a new Project",
                "user_request_required",
            ),
            (
                "genesis.start requires the currently leased Main Chat turn",
                "leased_turn_required",
            ),
            (
                "Project Charter adoption is not valid for the current Project state",
                "charter_adoption_not_applicable",
            ),
            (
                "query operations execute through the read boundary",
                "read_boundary_required",
            ),
            (
                "direct command is not admitted for this permission or payload",
                "direct_command_not_admitted",
            ),
            (
                "the Task assignment does not admit review proposals",
                "review_assignment_required",
            ),
            (
                "direct Project command has no selected active profile",
                "profile_not_selected",
            ),
        ] {
            assert_terminal_native_denial(reason, cause).await;
        }
    }

    #[tokio::test]
    async fn terminal_denial_maps_real_policy_evaluator_causes() {
        // These reasons come from the real evaluator, so a producer wording
        // change cannot silently turn a capability denial into an unknown one.
        for (operation, permission, paused, permissions, expected) in [
            (
                "task.action",
                "propose_task",
                false,
                r#"{"permissions":[]}"#,
                DeniedBy::PermissionMissing("propose_task".to_owned()),
            ),
            (
                "task.action",
                "propose_task",
                true,
                r#"{"permissions":["read_project","propose_task","propose_project"]}"#,
                DeniedBy::IdentityPaused,
            ),
            (
                "project.document",
                "propose_project",
                false,
                r#"{"permissions":["read_project","propose_task","propose_project"]}"#,
                DeniedBy::CharterNotAdopted,
            ),
            (
                "project.charter.adoption",
                "propose_project",
                false,
                r#"{"permissions":["read_project","propose_task","propose_project"]}"#,
                DeniedBy::CharterAdoptionNotApplicable,
            ),
            (
                "unlisted.operation",
                "read_project",
                false,
                r#"{"permissions":["read_project","propose_task","propose_project"]}"#,
                DeniedBy::OperationNotInScope,
            ),
            (
                "project.current_state",
                "read_project",
                false,
                r#"{"permissions":["read_project","propose_task","propose_project"]}"#,
                DeniedBy::ReadBoundaryRequired,
            ),
            (
                "task.action",
                "read_project",
                false,
                r#"{"permissions":["read_project","propose_task","propose_project"]}"#,
                DeniedBy::DirectCommandNotAdmitted,
            ),
            (
                "review.request",
                "read_project",
                false,
                r#"{"permissions":["read_project","propose_task","propose_project"]}"#,
                DeniedBy::IndependentApprovalRequired,
            ),
        ] {
            let fixture =
                native_plan_fixture_with_permissions("worker", "coder", permissions).await;
            let profile_id: String =
                sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id = ?")
                    .bind(&fixture.agent_id)
                    .fetch_one(fixture.db.pool())
                    .await
                    .unwrap();
            let project = db::ProjectRepo::create_with_agent_binding(
                &*fixture.db,
                db::CreateProject {
                    id: db::new_uuid_v4(),
                    name: "Policy Project".to_owned(),
                    settings: "{}".to_owned(),
                    workflow_definition: "{}".to_owned(),
                    primary_repo_id: None,
                    owner_id: None,
                    created_at: db::now_rfc3339(),
                    updated_at: db::now_rfc3339(),
                },
                Some(fixture.agent_id.clone()),
                Some(profile_id.clone()),
            )
            .await
            .unwrap();
            sqlx::query(
                "UPDATE agent_identity SET paused = ?, account_permission_ceiling = ? WHERE id = ?",
            )
            .bind(i64::from(paused))
            .bind(permissions)
            .bind(&fixture.agent_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
            sqlx::query("UPDATE project_agent_binding SET permission_ceiling_json = ? WHERE project_id = ? AND state = 'active'")
                .bind(permissions).bind(&project.id).execute(fixture.db.pool()).await.unwrap();
            if expected == DeniedBy::CharterAdoptionNotApplicable {
                // Exercise the evaluator's inconsistent-state guard rather
                // than inventing its prose. Normal writes enforce this invariant.
                sqlx::query("DROP TRIGGER project_charter_pointer_guard_update")
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
                sqlx::query("UPDATE project SET charter_setup_required = 0 WHERE id = ?")
                    .bind(&project.id)
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
            }
            let (_, reason) = fixture
                .provider
                .actions
                .evaluate_direct_command_policy(
                    &fixture.agent_id,
                    "project",
                    &project.id,
                    permission,
                    operation,
                    Some(r#"{"action":"draft_revision"}"#),
                )
                .await
                .unwrap();
            let scope = CanonicalScope {
                scope_type: CanonicalScopeType::Project,
                scope_id: project.id,
                workspace_access: WorkspaceAccess::Deny,
            };
            let error = fixture
                .provider
                .structured_boundary_error(
                    &fixture.agent_id,
                    &scope,
                    operation,
                    &json!({}),
                    AgentHostError::Authority(reason.expect("real evaluator cause")),
                )
                .await;
            let AgentHostError::StructuredOutcome(outcome) = error else {
                panic!("typed denial")
            };
            assert_eq!(outcome.denied_by, Some(expected));
            assert_eq!(outcome.retry.unwrap().action, RetryAction::None);
        }
    }

    #[tokio::test]
    async fn terminal_denial_maps_real_repository_profile_guard() {
        let fixture = native_plan_fixture_with_permissions(
            "worker",
            "coder",
            r#"{"permissions":["propose_project"]}"#,
        )
        .await;
        let project_id: String = sqlx::query_scalar("SELECT project_id FROM task WHERE id = ?")
            .bind(&fixture.task_id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE agent_identity SET selected_profile_id = NULL WHERE id = ?")
            .bind(&fixture.agent_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        let refusal = db::ProjectOrchestrationRepo::create_project_document_shell_command(
            &*fixture.db,
            db::CreateProjectDocumentShellCommand {
                document: db::CreateProjectDocument {
                    id: db::new_uuid_v4(),
                    project_id: project_id.clone(),
                    kind: "research".to_owned(),
                    title: "Profile guard".to_owned(),
                    approval_policy: "user".to_owned(),
                    created_at: db::now_rfc3339(),
                    updated_at: db::now_rfc3339(),
                },
                expected_project_version: 1,
                action_execution: None,
                command_receipt: Some(db::CreateCommandReceipt {
                    id: db::new_uuid_v4(),
                    principal_type: "agent".to_owned(),
                    principal_id: fixture.agent_id.clone(),
                    scope_type: "project".to_owned(),
                    scope_id: project_id.clone(),
                    operation: "project.document".to_owned(),
                    idempotency_key: "missing-profile".to_owned(),
                    input_digest: "input-digest".to_owned(),
                    policy_result: "allowed".to_owned(),
                    correlation_id: "missing-profile".to_owned(),
                    causation_id: None,
                    causation_depth: 0,
                    event_id: db::new_uuid_v4(),
                    agent_action_execution_id: None,
                    outcome_json: "{}".to_owned(),
                    committed_at: db::now_rfc3339(),
                }),
            },
        )
        .await
        .expect_err("real repository evaluator rejects the missing selected Profile");
        let scope = CanonicalScope {
            scope_type: CanonicalScopeType::Project,
            scope_id: project_id,
            workspace_access: WorkspaceAccess::Deny,
        };
        let error = fixture
            .provider
            .structured_boundary_error(
                &fixture.agent_id,
                &scope,
                "project.document",
                &json!({}),
                service_error(refusal.into()),
            )
            .await;
        let AgentHostError::StructuredOutcome(outcome) = error else {
            panic!("typed denial")
        };
        assert_eq!(outcome.denied_by, Some(DeniedBy::ProfileNotSelected));
        assert_eq!(
            outcome.retry.unwrap().scope,
            Some(api_types::RetryScope::Turn)
        );
    }

    #[tokio::test]
    async fn terminal_denial_maps_real_task_assignment_causes() {
        for (status, role, permission, expected) in [
            ("done", "worker", "task_write", DeniedBy::TaskTerminal),
            ("todo", "reviewer", "task_write", DeniedBy::ReviewerReadOnly),
            (
                "todo",
                "worker",
                "propose_review",
                DeniedBy::ReviewAssignmentRequired,
            ),
        ] {
            let fixture = native_plan_fixture("worker", "coder").await;
            sqlx::query("UPDATE task SET status = ? WHERE id = ?")
                .bind(status)
                .bind(&fixture.task_id)
                .execute(fixture.db.pool())
                .await
                .unwrap();
            sqlx::query("INSERT INTO task_role_assignment (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at) VALUES (?, ?, ?, 'agent', ?, ?, ?)")
                .bind(db::new_uuid_v4()).bind(&fixture.task_id).bind(role).bind(&fixture.agent_id)
                .bind(db::now_rfc3339()).bind(db::now_rfc3339()).execute(fixture.db.pool()).await.unwrap();
            let (_, reason) = fixture
                .provider
                .actions
                .evaluate_direct_command_policy(
                    &fixture.agent_id,
                    "task",
                    &fixture.task_id,
                    permission,
                    "review.request",
                    None,
                )
                .await
                .unwrap();
            let error = fixture
                .provider
                .structured_boundary_error(
                    &fixture.agent_id,
                    &fixture.scope,
                    "review.request",
                    &json!({}),
                    AgentHostError::Authority(reason.expect("assignment evaluator reason")),
                )
                .await;
            let AgentHostError::StructuredOutcome(outcome) = error else {
                panic!("typed denial")
            };
            assert_eq!(outcome.denied_by, Some(expected));
            assert_eq!(
                outcome.retry.unwrap().scope,
                Some(api_types::RetryScope::Turn)
            );
        }
    }

    #[tokio::test]
    async fn terminal_denial_names_own_project_pause() {
        let fixture = native_plan_fixture("worker", "coder").await;
        let profile_id: String =
            sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id = ?")
                .bind(&fixture.agent_id)
                .fetch_one(fixture.db.pool())
                .await
                .unwrap();
        let now = db::now_rfc3339();
        let project = db::ProjectRepo::create_with_agent_binding(
            &*fixture.db,
            db::CreateProject {
                id: db::new_uuid_v4(),
                name: "own paused Project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
            Some(fixture.agent_id.clone()),
            Some(profile_id),
        )
        .await
        .unwrap();
        sqlx::query("UPDATE project SET paused_at = 'now', system_pause_reason = 'environment_not_ready' WHERE id = ?")
            .bind(&project.id).execute(fixture.db.pool()).await.unwrap();
        let scope = CanonicalScope {
            scope_type: CanonicalScopeType::Project,
            scope_id: project.id,
            workspace_access: WorkspaceAccess::Deny,
        };
        let paused_task = db::new_uuid_v4();
        sqlx::query("INSERT INTO task (id, project_id, title, status, created_at, updated_at) VALUES (?, ?, 'Paused work', 'todo', ?, ?)")
            .bind(&paused_task).bind(&scope.scope_id).bind(db::now_rfc3339()).bind(db::now_rfc3339())
            .execute(fixture.db.pool()).await.unwrap();
        let refusal = TaskService::new(fixture.db.clone(), Arc::new(events::EventBus::new(16)))
            .claim_task(
                paused_task,
                crate::Assignee::Agent(fixture.agent_id.clone()),
                None,
            )
            .await
            .expect_err("real claim evaluator rejects the paused Project");
        let error = fixture
            .provider
            .structured_boundary_error(
                &fixture.agent_id,
                &scope,
                TASK_ACTION_OPERATION,
                &json!({}),
                service_error(refusal),
            )
            .await;
        let AgentHostError::StructuredOutcome(outcome) = error else {
            panic!("typed denial")
        };
        assert_eq!(
            outcome
                .denied_by
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some("project_paused(environment_not_ready)")
        );
        assert!(outcome.safe_message.contains("Do not retry"));
        assert_eq!(
            outcome.retry.unwrap().scope,
            Some(api_types::RetryScope::Turn)
        );
    }

    #[tokio::test]
    async fn terminal_denial_keeps_cross_scope_reasons_redacted() {
        assert_terminal_native_denial(
            "another Project private-project-secret is paused: confidential-detail",
            "unspecified",
        )
        .await;
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let provider = CoordinationToolProvider::new(Arc::new(SqliteDb::new(pool)));
        let scope = CanonicalScope {
            scope_type: CanonicalScopeType::Project,
            scope_id: "opaque-target".to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        };
        let mut values = Vec::new();
        for error in [
            native_scope_error(crate::ServiceError::not_found("project", "opaque-target")),
            native_scope_error(crate::ServiceError::AuthorizationDenied {
                message: "Project Agent binding does not own this Project scope".to_owned(),
            }),
        ] {
            let AgentHostError::StructuredOutcome(outcome) = provider
                .structured_boundary_error(
                    "actor",
                    &scope,
                    PROJECT_CURRENT_STATE_OPERATION,
                    &json!({"correlation_id":"same-correlation"}),
                    error,
                )
                .await
            else {
                panic!("typed redacted outcome")
            };
            values.push(serde_json::to_value(outcome).unwrap());
        }
        assert_eq!(
            values[0], values[1],
            "missing and inaccessible scopes remain indistinguishable"
        );
        assert!(!values[0].to_string().contains("binding"));
    }

    #[test]
    fn daemon_target_refusals_are_specific_tool_results_without_withdrawal() {
        for (error, cause) in [
            (
                crate::ServiceError::PlacementUnavailable(crate::placement::PlacementUnavailable {
                    task_id: "target".into(),
                    repo_id: "repo".into(),
                    rejected_candidates: vec![],
                }),
                DeniedBy::PlacementUnavailable,
            ),
            (
                crate::ServiceError::DaemonUpgradeRequired {
                    daemon_id: "target".into(),
                },
                DeniedBy::DaemonUpgradeRequired,
            ),
            (
                crate::ServiceError::WorkspaceResetRequired {
                    task_id: "target".into(),
                    reason: "lost".into(),
                },
                DeniedBy::WorkspaceResetRequired,
            ),
        ] {
            let AgentHostError::StructuredOutcome(outcome) = service_error(error) else {
                panic!("tool result expected")
            };
            assert_eq!(outcome.denied_by, Some(cause.clone()));
            assert!(!cause.withdraws_operation());
            assert_eq!(
                outcome.retry.unwrap().scope,
                Some(api_types::RetryScope::Turn)
            );
            assert_eq!(cause.to_string().parse::<DeniedBy>().unwrap(), cause);
        }
    }

    #[test]
    fn argument_shape_rejections_are_correctable_validation_outcomes() {
        // A Project Agent that sends `task.action` with an unknown action
        // must learn which field to fix; an opaque internal failure leaves it
        // retrying the same shape or abandoning a repair its doctrine requires.
        let error = invalid_arguments("action must use a current closed Task verb".to_owned());
        match error {
            AgentHostError::StructuredOutcome(outcome) => {
                assert_eq!(outcome.code, OutcomeCode::ValidationError);
                assert_eq!(outcome.status, OutcomeStatus::Failed);
                assert!(outcome
                    .safe_message
                    .contains("action must use a current closed Task verb"));
                assert_eq!(
                    outcome.retry.as_ref().map(|retry| retry.action),
                    Some(RetryAction::CorrectInput)
                );
            }
            other => panic!("argument rejections must be structured, got {other:?}"),
        }

        // A task_id naming no Task in this Project is the same class of
        // mistake. Raising it as an authority refusal told the Agent its
        // scope had been denied recovery, so one transposed character in a
        // UUID stalled a Project behind a blocker that did not exist.
        let unresolved = invalid_arguments("task_id must name a Task in this Project".to_owned());
        match unresolved {
            AgentHostError::StructuredOutcome(outcome) => {
                assert_eq!(outcome.code, OutcomeCode::ValidationError);
                assert_eq!(
                    outcome.retry.as_ref().map(|retry| retry.action),
                    Some(RetryAction::CorrectInput)
                );
            }
            other => panic!("an unresolved task_id must be correctable, got {other:?}"),
        }
    }

    #[test]
    fn generic_conflicts_are_typed_and_keep_the_actionable_reason() {
        let error = service_error(crate::ServiceError::Conflict(
            "send expected_charter_version = 2".to_owned(),
        ));
        match error {
            AgentHostError::StructuredOutcome(outcome) => {
                assert_eq!(outcome.code, OutcomeCode::ValidationError);
                assert_eq!(outcome.status, OutcomeStatus::Failed);
                assert!(outcome.safe_message.contains("expected_charter_version"));
                assert!(outcome.safe_message.contains("correct the typed input"));
            }
            other => panic!("conflict must be structured, got {other:?}"),
        }
    }

    #[test]
    fn database_version_conflicts_have_typed_retry_without_prose() {
        let error = service_error(crate::ServiceError::Db(db::DbError::VersionConflict));
        match error {
            AgentHostError::StructuredOutcome(outcome) => {
                assert_eq!(outcome.code, OutcomeCode::VersionConflict);
                assert_eq!(outcome.status, OutcomeStatus::Failed);
                assert_eq!(
                    outcome.safe_message,
                    "the authorized resource changed; refresh current state and retry"
                );
                assert_eq!(
                    outcome.retry.as_ref().map(|retry| retry.action),
                    Some(RetryAction::RefreshAndRetry)
                );
                assert!(outcome.current_version_or_revision.is_none());
            }
            other => panic!("version conflicts must be structured, got {other:?}"),
        }
    }

    #[test]
    fn idempotency_conflicts_do_not_query_or_expose_current_state() {
        let error = service_error(crate::ServiceError::Db(db::DbError::IdempotencyConflict));
        match error {
            AgentHostError::StructuredOutcome(outcome) => {
                assert_eq!(outcome.code, OutcomeCode::IdempotencyConflict);
                assert!(outcome.current_version_or_revision.is_none());
                let retry = outcome.retry.expect("fresh key guidance");
                assert_eq!(retry.action, RetryAction::UseNewIdempotencyKey);
                assert!(!retry.retryable);
            }
            other => panic!("idempotency conflicts must be structured, got {other:?}"),
        }
    }

    #[test]
    fn genesis_start_failures_keep_stable_structured_codes() {
        let cases = [
            (
                crate::ServiceError::ExecutionSetupRequired {
                    message: "private setup detail".to_owned(),
                    requirements: vec![SetupRequirement::new("main_agent")],
                },
                OutcomeCode::SetupRequired,
            ),
            (
                crate::ServiceError::ProductGenesisActiveSession {
                    session_id: "private-session-id".to_owned(),
                },
                OutcomeCode::ActiveSessionConflict,
            ),
            (
                crate::ServiceError::Db(db::DbError::IdempotencyConflict),
                OutcomeCode::IdempotencyConflict,
            ),
            (
                crate::ServiceError::Domain("private storage failure".to_owned()),
                OutcomeCode::InternalFailure,
            ),
        ];
        for (error, expected) in cases {
            match service_error(error) {
                AgentHostError::StructuredOutcome(outcome) => {
                    assert_eq!(outcome.code, expected);
                    assert!(!outcome.safe_message.contains("private"));
                }
                other => panic!("Genesis start failure must be structured, got {other:?}"),
            }
        }
    }

    #[test]
    fn proposal_targets_are_derived_from_scope() {
        let scope = CanonicalScope {
            scope_type: CanonicalScopeType::Project,
            scope_id: "project-1".to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        };
        let arguments = json!({
            "payload": {"title":"bounded"},
            "dedupe_key":"dedupe",
            "correlation_id":"corr",
        });
        assert_eq!(
            scope_type_name(scope.scope_type),
            "project",
            "the operation target is taken from the canonical scope"
        );
        assert_eq!(arguments["payload"]["title"], "bounded");
    }

    #[test]
    fn adaptive_payload_is_closed_to_the_three_bounded_actions() {
        let split = json!({
            "action": "split",
            "source_task_id": "task-1",
            "expected_task_version": 1,
            "expected_board_revision": 2,
            "rationale": "separate bounded work",
            "items": [{"title": "child", "description": null, "assignee_id": null}]
        });
        let sequence = json!({
            "action": "sequence",
            "source_task_id": "task-1",
            "expected_task_version": 1,
            "expected_board_revision": 2,
            "rationale": "order bounded work",
            "ordered_task_ids": ["task-2", "task-3"]
        });
        let replace = json!({
            "action": "replace",
            "source_task_id": "task-1",
            "expected_task_version": 1,
            "expected_board_revision": 2,
            "rationale": "replace bounded work",
            "title": "replacement",
            "description": "updated outcome"
        });
        for payload in [split, sequence, replace] {
            assert!(validate_proposal_payload(TASK_ADAPTIVE_OPERATION, &payload).is_ok());
        }
    }

    #[test]
    fn adaptive_payload_rejects_unknown_and_server_owned_fields() {
        let base = json!({
            "action": "replace",
            "source_task_id": "task-1",
            "expected_task_version": 1,
            "expected_board_revision": 2,
            "rationale": "bounded",
            "title": "replacement",
            "description": null
        });
        for field in [
            "project_id",
            "scope_id",
            "actor_id",
            "governance",
            "fixed_boundary_digest",
            "unknown",
        ] {
            let mut payload = base.clone();
            payload[field] = json!("forbidden");
            assert!(
                validate_proposal_payload(TASK_ADAPTIVE_OPERATION, &payload).is_err(),
                "adaptive payload field {field} must be rejected"
            );
        }
    }
    #[test]
    fn session_action_payload_is_bounded_and_allowlisted() {
        assert!(validate_proposal_payload("session.action", &json!({"action":"cancel"}),).is_ok());
        assert!(validate_proposal_payload(
            "session.action",
            &json!({"action":"steer","content":"continue"}),
        )
        .is_ok());
        assert!(
            validate_proposal_payload("session.action", &json!({"action":"execute"}),).is_err()
        );
        assert!(validate_proposal_payload("session.action", &json!({"action":"steer"}),).is_err());
    }

    #[test]
    fn generic_project_lifecycle_cannot_create_projects() {
        assert!(
            validate_proposal_payload("project.lifecycle", &json!({"action":"create"}),).is_err()
        );
        assert!(validate_proposal_payload(
            "project.lifecycle",
            &json!({"action":"organize","project_id":"project-1"}),
        )
        .is_ok());
        assert!(validate_proposal_payload(
            MAIN_PROJECT_CREATE_OPERATION,
            &json!({"action":"create_from_approval","approval_id":"approval-1"}),
        )
        .is_ok());
    }

    #[test]
    fn public_search_result_urls_reject_private_and_special_use_hosts() {
        for url in [
            "https://localhost/result",
            "https://127.0.0.1/result",
            "https://10.0.0.1/result",
            "https://169.254.169.254/result",
            "https://[::1]/result",
            "https://[::ffff:127.0.0.1]/result",
            "https://[::ffff:8.8.8.8]/result",
            "https://[::8.8.8.8]/result",
            "https://[64:ff9b::192.0.2.1]/result",
            "https://[fe80::1%25en0]/result",
            "https://192.0.2.1/result",
            "https://[2001:db8::1]/result",
            "https://[ff02::1]/result",
            "https://user@example.com/result",
            "https://example.com/result#fragment",
        ] {
            assert!(
                normalize_public_result_url(url).is_err(),
                "result URL must be rejected: {url}"
            );
        }
        assert_eq!(
            normalize_public_result_url("https://example.com/result").expect("public URL"),
            "https://example.com/result"
        );
        assert_eq!(
            normalize_public_result_url("http://example.com/result").expect("public URL"),
            "http://example.com/result"
        );
        assert!(normalize_public_result_url("https://example.com/\u{000a}").is_err());
    }

    #[test]
    fn public_search_address_filter_rejects_private_mapped_and_special_use_ranges() {
        for address in [
            "0.0.0.0",
            "10.0.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.1.1",
            "192.0.0.1",
            "192.0.2.1",
            "192.88.99.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "::1",
            "::ffff:127.0.0.1",
            "::ffff:8.8.8.8",
            "::8.8.8.8",
            "fc00::1",
            "fe80::1",
            "64:ff9b::192.0.2.1",
            "2001:2::1",
            "2001:db8::1",
            "ff02::1",
        ] {
            let address = address.parse().expect("valid test address");
            assert!(
                is_blocked_public_address(address),
                "address must be blocked: {address}"
            );
        }
        assert!(!is_blocked_public_address(
            "8.8.8.8".parse().expect("public IPv4")
        ));
        assert!(!is_blocked_public_address(
            "2001:4860:4860::8888".parse().expect("public IPv6")
        ));
    }

    #[test]
    fn outbox_entry_reader_reports_malformed_and_non_object_content() {
        let temp = tempfile::tempdir().expect("temp dir");
        std::fs::write(
            temp.path().join(executors::OUTBOX_WORKLOG_FILE),
            concat!(
                "not json\n",
                "{\n\"kind\":\"progress\",\n\"summary\":\"valid\"\n}\n",
                "[]\n",
                "{\"kind\":\"validation\",\"summary\":",
            ),
        )
        .expect("worklog writes");
        let mut report = ExecutionOutboxReport::default();
        let entries = read_outbox_entries(temp.path(), executors::OUTBOX_WORKLOG_FILE, &mut report);
        assert!(
            report.rejected.is_empty(),
            "entry errors stay in file order"
        );
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].0, "1");
        assert!(entries[0].1.is_err());
        assert_eq!(entries[1].0, "2");
        assert_eq!(
            entries[1].1.as_ref().expect("pretty object parses")["summary"],
            "valid"
        );
        assert_eq!(entries[2].0, "6");
        assert_eq!(
            entries[2].1.as_ref().expect_err("array is not an entry"),
            "entry must be a JSON object"
        );
        assert_eq!(entries[3].0, "7");
        assert!(entries[3]
            .1
            .as_ref()
            .expect_err("truncated object is malformed")
            .contains("EOF"));
    }

    #[test]
    fn outbox_entry_reader_limits_objects_rather_than_pretty_printed_lines() {
        let temp = tempfile::tempdir().expect("temp dir");
        let entry = json!({
            "kind": "progress",
            "summary": "large pretty object",
            "details": vec![0; MAX_OUTBOX_ENTRIES + 1],
        });
        let pretty = serde_json::to_string_pretty(&entry).expect("pretty object serializes");
        assert!(pretty.lines().count() > MAX_OUTBOX_ENTRIES);
        let path = temp.path().join(executors::OUTBOX_WORKLOG_FILE);
        std::fs::write(&path, &pretty).expect("pretty object writes");
        let mut report = ExecutionOutboxReport::default();
        let entries = read_outbox_entries(temp.path(), executors::OUTBOX_WORKLOG_FILE, &mut report);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1.as_ref().expect("pretty object parses"), &entry);
        assert!(report.rejected.is_empty());

        std::fs::write(&path, "{}".repeat(MAX_OUTBOX_ENTRIES + 1)).expect("object stream writes");
        let entries = read_outbox_entries(temp.path(), executors::OUTBOX_WORKLOG_FILE, &mut report);
        assert_eq!(entries.len(), MAX_OUTBOX_ENTRIES);
        assert_eq!(report.rejected.len(), 1);
        assert!(report.rejected[0].contains("only the first 200 entries"));
        let positions: BTreeSet<_> = entries.iter().map(|(position, _)| position).collect();
        assert_eq!(positions.len(), MAX_OUTBOX_ENTRIES);
    }

    #[cfg(unix)]
    #[test]
    fn outbox_entry_reader_rejects_symlinks_and_hard_links() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("temp dir");
        let outside = tempfile::tempdir().expect("outside dir");
        let outbox = temp.path().join("outbox");
        fs::create_dir(&outbox).expect("outbox creates");
        let outside_log = outside.path().join("worklog.jsonl");
        fs::write(
            &outside_log,
            "{\"kind\":\"progress\",\"summary\":\"secret\"}\n",
        )
        .expect("outside log writes");

        symlink(&outside_log, outbox.join(executors::OUTBOX_WORKLOG_FILE))
            .expect("log symlink creates");
        let mut symlink_report = ExecutionOutboxReport::default();
        assert!(
            read_outbox_entries(&outbox, executors::OUTBOX_WORKLOG_FILE, &mut symlink_report)
                .is_empty()
        );
        assert!(symlink_report
            .rejected
            .iter()
            .any(|reason| reason.contains("not a regular file")));

        fs::remove_file(outbox.join(executors::OUTBOX_WORKLOG_FILE)).expect("log symlink removes");
        fs::hard_link(&outside_log, outbox.join(executors::OUTBOX_WORKLOG_FILE))
            .expect("log hard link creates");
        let mut hard_link_report = ExecutionOutboxReport::default();
        assert!(read_outbox_entries(
            &outbox,
            executors::OUTBOX_WORKLOG_FILE,
            &mut hard_link_report
        )
        .is_empty());
        assert!(hard_link_report
            .rejected
            .iter()
            .any(|reason| reason.contains("hard link")));
    }

    #[cfg(unix)]
    #[test]
    fn workspace_artifact_reader_rejects_leaf_symlinks() {
        use std::os::unix::fs::symlink;

        let workspace = tempfile::tempdir().expect("workspace dir");
        let outside = tempfile::tempdir().expect("outside dir");
        fs::write(outside.path().join("secret"), "secret").expect("outside file writes");
        symlink(
            outside.path().join("secret"),
            workspace.path().join("capture"),
        )
        .expect("capture symlink creates");

        let error = resolve_workspace_artifact(workspace.path(), "capture")
            .expect_err("leaf symlink fails closed");

        assert_eq!(
            error,
            "captured artifact must be a regular file, not a symlink"
        );
    }

    #[test]
    fn outbox_artifact_rejection_preserves_plain_reason() {
        let workspace = tempfile::tempdir().unwrap();
        let outbox = workspace.path().join("outbox");
        let error = resolve_outbox_artifact(workspace.path(), &outbox, "../secret").unwrap_err();
        assert_eq!(error, "captured artifact path escapes the Task workspace");
    }

    #[tokio::test]
    async fn terminal_denial_project_pause_uses_bound_project_and_task_own_project() {
        let fixture = native_plan_fixture("worker", "coder").await;
        let project_id: String = sqlx::query_scalar("SELECT project_id FROM task WHERE id = ?")
            .bind(&fixture.task_id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE project SET paused_at = 'now', system_pause_reason = 'environment_not_ready' WHERE id = ?")
            .bind(&project_id).execute(fixture.db.pool()).await.unwrap();
        for (paused_project, expected) in [
            (
                project_id.as_str(),
                db::project_pause_denial(Some("environment_not_ready")),
            ),
            ("unrelated-project", DeniedBy::Unspecified),
        ] {
            let error = fixture
                .provider
                .structured_boundary_error(
                    &fixture.agent_id,
                    &fixture.scope,
                    TASK_EVIDENCE_OPERATION,
                    &json!({}),
                    service_error(crate::ServiceError::ProjectPaused {
                        project_id: paused_project.to_owned(),
                    }),
                )
                .await;
            let AgentHostError::StructuredOutcome(outcome) = error else {
                panic!("structured refusal")
            };
            assert_eq!(outcome.denied_by, Some(expected));
            assert!(!outcome.safe_message.contains("unrelated-project"));
        }
    }

    #[tokio::test]
    async fn native_search_validation_and_observation_refusal_have_boundary_metadata() {
        let fixture = native_plan_fixture("worker", "coder").await;
        let search = fixture
            .provider
            .public_search(
                &fixture.agent_id,
                &fixture.scope,
                PublicSearchScope::Project,
                "",
                5,
            )
            .await
            .unwrap_err();
        let AgentHostError::StructuredOutcome(search) = search else {
            panic!("structured validation")
        };
        assert_eq!(search.code, OutcomeCode::ValidationError);
        assert_eq!(
            search.operation,
            forge_agent_host::FORGE_PUBLIC_WEB_SEARCH_TOOL
        );
        assert_eq!(search.scope, outcome_scope(&fixture.scope));
        assert!(!search.correlation_id.is_empty());
        let observation = fixture
            .provider
            .observe_command(
                &fixture.agent_id,
                &fixture.scope,
                CommandObservation {
                    session_id: fixture.runtime_session_id.clone(),
                    turn_id: Some("turn".to_owned()),
                    program: "git".to_owned(),
                    args: vec![],
                    exit_code: Some(0),
                    success: true,
                    output_digest: "digest".to_owned(),
                    stdout_excerpt: String::new(),
                    stderr_excerpt: String::new(),
                },
            )
            .await
            .unwrap_err();
        let AgentHostError::StructuredOutcome(observation) = observation else {
            panic!("structured refusal")
        };
        assert_eq!(observation.denied_by, Some(DeniedBy::Unspecified));
        assert_eq!(observation.operation, "observe_command");
        assert_eq!(observation.scope, outcome_scope(&fixture.scope));
        assert!(!observation.correlation_id.is_empty());
        assert!(!observation.safe_message.contains("corrected input"));
    }

    #[tokio::test]
    async fn public_search_resolver_rejects_unexpected_and_local_hosts() {
        use std::str::FromStr;

        let resolver = PublicSearchResolver {
            allowed_host: "search.example.test".to_owned(),
        };
        let unexpected = <PublicSearchResolver as reqwest::dns::Resolve>::resolve(
            &resolver,
            reqwest::dns::Name::from_str("other.example.test").expect("DNS name"),
        )
        .await;
        assert!(unexpected.is_err());

        let localhost_resolver = PublicSearchResolver {
            allowed_host: "localhost".to_owned(),
        };
        let local = <PublicSearchResolver as reqwest::dns::Resolve>::resolve(
            &localhost_resolver,
            reqwest::dns::Name::from_str("localhost").expect("DNS name"),
        )
        .await;
        assert!(
            local.is_err(),
            "localhost must not resolve for public search"
        );
    }
}
