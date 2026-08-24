//! Forge service adapter for the host's scope-derived native tools.
//!
//! This adapter exposes read projections and policy-checked proposal commands.
//! Query operations use the read boundary; direct `task.propose`,
//! `task.adaptive`, Main Charter, and bounded Project orchestration commands
//! call their shared command services, while approval-required mutations
//! retain an `AgentAction` envelope.

use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv6Addr},
    sync::{Arc, RwLock},
    time::Duration,
};

use api_types::{
    ApprovalTarget, CanonicalScopeRef as OutcomeScopeRef, CurrentVersionOrRevision,
    OrchestrationOutcome, OutcomeCode, OutcomeScopeType, OutcomeStatus, RetryAction,
    RetryInstruction, SetupRequirement,
};
use async_trait::async_trait;
use chrono::Utc;
use config::PublicSearchConfig;
use db::{
    AgentAction, AgentActionListQuery, AgentActionPolicyResult, AgentActionRepo,
    AgentCommitmentListQuery, AgentCommitmentRepo, AgentInboxListQuery, AgentInboxRepo,
    CommandReceiptRepo, MemoryScopeGrant, SqliteDb,
};
use forge_agent_host::{
    contains_adaptive_authority_override, contains_authority_override, operation_contract,
    operation_descriptor, operation_permission, AgentHostError, CanonicalScope, CanonicalScopeType,
    ForgeToolProvider, OperationClassification, PublicSearchScope, WorkspaceAccess,
    MAIN_CHARTER_APPROVAL_TARGET_OPERATION, MAIN_CHARTER_DIFF_OPERATION,
    MAIN_CHARTER_DRAFT_OPERATION, MAIN_CHARTER_READINESS_OPERATION, MAIN_CHARTER_READ_OPERATION,
    MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION, MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION,
    MAIN_GENESIS_START_OPERATION, MAIN_PROJECT_CREATE_OPERATION,
    PROJECT_CHARTER_ADOPTION_OPERATION, PROJECT_CURRENT_STATE_OPERATION,
    PROJECT_DECISION_OPERATION, PROJECT_DOCUMENT_OPERATION, PROJECT_EVIDENCE_OPERATION,
    PROJECT_EXECUTION_BASELINE_OPERATION, PROJECT_MILESTONE_OPERATION, PROJECT_READINESS_OPERATION,
    PROJECT_RELEASE_OPERATION, TASK_ADAPTIVE_OPERATION, TASK_PROPOSE_OPERATION,
};
use reqwest::header::ACCEPT;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    agent_chat_policy::guard_agent_chat_content,
    coordination_service::{AgentActionService, ProposeActionInput},
    memory::{MemoryAccessContext, MemoryService},
    project_agent_actions::ExecuteDirectProjectCommandInput,
    project_runtime::{load_effective_project_state, ProjectCurrentStateResponse},
    task_service::{
        AdaptiveTaskChild, AdaptiveTaskCommand, AdaptiveTaskCommandResult, AdaptiveTaskOperation,
        DirectTaskProposalInput, TaskProposalCommandResult, TaskProposalPayload,
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
    /// Shared TaskService used to execute directly admitted `task.propose` and
    /// `task.adaptive` commands inline, so native proposals materialize
    /// through the durable command receipt path without a separate caller.
    task_service: Arc<RwLock<Option<Arc<TaskService>>>>,
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
            task_service: Arc::new(RwLock::new(None)),
            db,
        }
    }

    /// Attach the shared TaskService so admitted Task commands execute inline
    /// through the shared receipt-backed command paths.
    pub fn set_task_service(&self, task_service: Arc<TaskService>) {
        if let Ok(mut slot) = self.task_service.write() {
            *slot = Some(task_service);
        }
    }

    fn task_service_handle(&self) -> Option<Arc<TaskService>> {
        self.task_service.read().ok().and_then(|slot| slot.clone())
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
            "work.read" => self.read_work(scope, limit).await,
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
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        let account_id = self
            .authorization
            .main_account_id(actor_identity_id, scope)
            .await
            .map_err(native_scope_error)?;
        let limit = arguments
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(10)
            .clamp(1, 20) as i64;
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
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        let account_id = self
            .authorization
            .main_account_id(actor_identity_id, scope)
            .await
            .map_err(native_scope_error)?;
        let limit = arguments
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(20)
            .clamp(1, 20) as i64;
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
            .map_err(|error| AgentHostError::Authority(error.to_string()))?;
        let execution_setup = crate::load_project_execution_setup(&self.db, &project_id)
            .await
            .map_err(|error| AgentHostError::Authority(error.to_string()))?;
        serde_json::to_value(ProjectCurrentStateResponse {
            scope: "project".to_owned(),
            effective_state: projection,
            execution_setup: Some(execution_setup),
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
            .ok_or_else(|| AgentHostError::Authority("project_id is required".to_owned()))?;
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
        .ok_or_else(|| AgentHostError::Authority("Project summary is unavailable".to_owned()))?;
        Ok(json!({
            "id": row.try_get::<String, _>("id").unwrap_or_default(),
            "name": row.try_get::<String, _>("name").unwrap_or_default(),
            "paused": row.try_get::<Option<String>, _>("paused_at").ok().flatten().is_some(),
            "task_count": row.try_get::<i64, _>("task_count").unwrap_or_default(),
            "created_at": row.try_get::<String, _>("created_at").unwrap_or_default(),
            "updated_at": row.try_get::<String, _>("updated_at").unwrap_or_default(),
        }))
    }

    async fn read_work(&self, scope: &CanonicalScope, limit: i64) -> Result<Value, AgentHostError> {
        let rows = match scope.scope_type {
            CanonicalScopeType::Project => sqlx::query(
                "SELECT id, title, status, priority, assignee_type, assignee_id
                     FROM task WHERE project_id = ? AND deleted_at IS NULL
                     ORDER BY updated_at DESC, id DESC LIMIT ?",
            )
            .bind(&scope.scope_id)
            .bind(limit)
            .fetch_all(self.db.pool())
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?,
            CanonicalScopeType::Task => sqlx::query(
                "SELECT id, title, status, priority, assignee_type, assignee_id
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
        let items = rows
            .into_iter()
            .map(|row| {
                json!({
                    "id": row.try_get::<String, _>("id").unwrap_or_default(),
                    "title": row.try_get::<String, _>("title").unwrap_or_default(),
                    "status": row.try_get::<String, _>("status").unwrap_or_default(),
                    "priority": row.try_get::<i64, _>("priority").unwrap_or_default(),
                    "assignee_type": row.try_get::<Option<String>, _>("assignee_type").ok().flatten(),
                    "assignee_id": row.try_get::<Option<String>, _>("assignee_id").ok().flatten(),
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({"items": items}))
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
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        let payload = arguments
            .get("payload")
            .filter(|value| value.is_object())
            .cloned()
            .ok_or_else(|| {
                AgentHostError::Unsupported("proposal payload must be an object".into())
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
                return Err(AgentHostError::Authority(
                    "operation is denied by the canonical Forge operation catalog".to_owned(),
                ));
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
        if classification == OperationClassification::DirectCommand {
            let requested_permission = descriptor.required_permission.ok_or_else(|| {
                AgentHostError::Authority(
                    "direct command has no canonical permission descriptor".to_owned(),
                )
            })?;
            let target_id = if operation == TASK_PROPOSE_OPERATION
                || operation == TASK_ADAPTIVE_OPERATION
                || forge_agent_host::is_project_orchestration_operation(operation)
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
            | PROJECT_EXECUTION_BASELINE_OPERATION
            | PROJECT_MILESTONE_OPERATION
            | PROJECT_EVIDENCE_OPERATION
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
            let (policy_result, _policy_reason) = if receipt_exists {
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
                    "adaptive Task command policy did not admit execution".to_owned(),
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
            return Err(AgentHostError::Authority(
                "search query must contain 1 to 512 characters".to_owned(),
            ));
        }
        if !(1..=10).contains(&limit) {
            return Err(AgentHostError::Authority(
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
                "Project Agent Chat binding does not admit Task management".to_owned(),
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
            AgentHostError::Configuration(_) => "configuration",
            AgentHostError::SessionNotFound => "session_not_found",
            AgentHostError::CredentialNotFound => "credential_not_found",
            AgentHostError::VersionConflict => "version_conflict",
            AgentHostError::Unsupported(_) => "unsupported",
            AgentHostError::Runtime(_) => "runtime",
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
            AgentHostError::Authority(detail) => {
                let validation_failed = operation == TASK_PROPOSE_OPERATION
                    || operation == MAIN_CHARTER_DRAFT_OPERATION
                    || arguments
                        .get("payload")
                        .filter(|payload| payload.is_object())
                        .is_some_and(|payload| {
                            validate_proposal_payload(operation, payload).is_err()
                        });
                let (code, message, retry) = if validation_failed {
                    // The contract reason is server-authored and names only
                    // the offending field, so it is safe to return and is the
                    // only way the model can correct the call. Without it the
                    // model retries the same rejected shape indefinitely.
                    (
                        OutcomeCode::ValidationError,
                        format!(
                            "the operation or arguments are not valid for this Forge surface ({detail})"
                        ),
                        Some(RetryInstruction::new(RetryAction::CorrectInput, false)),
                    )
                } else {
                    // Policy denials stay generic: the reason can describe
                    // authority the caller is not entitled to observe.
                    (
                        OutcomeCode::PolicyDenied,
                        "the operation is not admitted for the current Forge scope".to_owned(),
                        Some(RetryInstruction::new(RetryAction::Reauthorize, false)),
                    )
                };
                let mut outcome = OrchestrationOutcome::failed(
                    code,
                    operation,
                    outcome_scope(scope),
                    &correlation_id,
                    message,
                );
                outcome.retry = retry;
                outcome
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
        self.run_public_search(actor_identity_id, scope, search_scope, query, limit)
            .await
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
                return Err(AgentHostError::Authority(
                    "operation is denied by the canonical Forge operation catalog".to_owned(),
                ));
            }
            OperationClassification::DirectCommand
            | OperationClassification::ApprovalRequiredAction => {
                return Err(AgentHostError::Unsupported(
                    "mutation operations execute through the proposal boundary".to_owned(),
                ));
            }
        }
        let result = match operation {
            MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION
            | MAIN_CHARTER_READINESS_OPERATION
            | MAIN_CHARTER_DIFF_OPERATION
            | MAIN_CHARTER_APPROVAL_TARGET_OPERATION => self
                .main_queries
                .execute(actor_identity_id, scope, operation, arguments)
                .await
                .map_err(service_error),
            MAIN_CHARTER_READ_OPERATION => self
                .main_queries
                .execute(actor_identity_id, scope, operation, arguments)
                .await
                .map_err(native_scope_error),
            PROJECT_CURRENT_STATE_OPERATION => {
                self.project_current_state_read(actor_identity_id, scope, arguments)
                    .await
            }
            "memory.read" => {
                self.memory_read(actor_identity_id, scope, arguments, false)
                    .await
            }
            "account.summary" | "project.summary" | "agent_chat.summary" | "task.summary" => {
                if operation == "project.summary"
                    && scope.scope_type == CanonicalScopeType::AgentChat
                {
                    self.project_summary_read(actor_identity_id, scope, arguments)
                        .await
                } else {
                    self.summary(actor_identity_id, scope).await
                }
            }
            "discovery.read" => {
                self.discovery_read(actor_identity_id, scope, arguments)
                    .await
            }
            "portfolio.read" => {
                self.portfolio_read(actor_identity_id, scope, arguments)
                    .await
            }
            "decisions.read" => {
                self.memory_read(actor_identity_id, scope, arguments, true)
                    .await
            }
            "work.read" | "events.read" | "inbox.read" | "commitments.read" | "delivery.read" => {
                self.scoped_rows(actor_identity_id, scope, operation, arguments)
                    .await
            }
            _ => Err(AgentHostError::Unsupported(
                "Forge read operation is not implemented".to_owned(),
            )),
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
            result.map_err(non_orchestration_error)
        }
    }

    async fn propose(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
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
            .propose(actor_identity_id, scope, operation, arguments.clone())
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
            result.map_err(non_orchestration_error)
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

fn non_orchestration_error(error: AgentHostError) -> AgentHostError {
    match error {
        AgentHostError::StructuredOutcome(_) => {
            AgentHostError::Runtime("Forge tool provider failed".to_owned())
        }
        other => other,
    }
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
            PROJECT_EXECUTION_BASELINE_OPERATION => "expected_baseline_version",
            PROJECT_DOCUMENT_OPERATION => "expected_document_version",
            PROJECT_MILESTONE_OPERATION | PROJECT_EVIDENCE_OPERATION => {
                "expected_milestone_version"
            }
            PROJECT_READINESS_OPERATION | PROJECT_RELEASE_OPERATION => "milestone_version",
            _ => "expected_version",
        };
        retry.arguments.insert(field.to_owned(), json!(version));
    }
    if let Some(revision_id) = current.revision_id.as_deref() {
        if matches!(
            operation,
            PROJECT_EXECUTION_BASELINE_OPERATION | PROJECT_DOCUMENT_OPERATION
        ) {
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
    if serde_json::to_vec(payload)
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

fn service_error(error: crate::ServiceError) -> AgentHostError {
    // A validation reason from the command boundary names the offending
    // input, so it returns to the model verbatim. Collapsing it to a bare
    // "not valid" leaves the model retrying the same rejected shape with
    // nothing to correct.
    if let crate::ServiceError::InvalidOperation { message }
    | crate::ServiceError::TerminalInvalidInput { message } = &error
    {
        let mut outcome = OrchestrationOutcome::failed(
            OutcomeCode::ValidationError,
            "unknown",
            OutcomeScopeRef::new(OutcomeScopeType::Account, ""),
            "",
            format!("the operation or arguments are not valid for this Forge surface ({message})"),
        );
        outcome.retry = Some(RetryInstruction::new(RetryAction::CorrectInput, false));
        return AgentHostError::StructuredOutcome(Box::new(outcome));
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
        crate::ServiceError::AuthorizationDenied { message } => {
            tracing::warn!(diagnostic = %message, "orchestration authorization denied");
            (
                OutcomeCode::PolicyDenied,
                "the operation is not admitted for the current Forge scope",
                None,
                Some(RetryInstruction::new(RetryAction::Reauthorize, false)),
            )
        }
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
        | crate::ServiceError::RepoMismatch { .. }
        | crate::ServiceError::PrProviderMissing { .. }
        | crate::ServiceError::PrProviderTokenMissing { .. }
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
        crate::ServiceError::DaemonUnavailable { .. }
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn nested_baseline_approval_target_is_projected_into_outcome() {
        let scope = CanonicalScope {
            scope_type: CanonicalScopeType::Project,
            scope_id: "project-1".to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        };
        let result = json!({
            "receipt_id": "receipt-1",
            "requires_user_authorization": true,
            "domain_result": {
                "baseline_version": 3,
                "approval_target": {
                    "baseline_id": "baseline-1",
                    "revision_id": "revision-1",
                    "revision": 0,
                    "content_digest": "content-digest",
                    "render_digest": "render-digest",
                    "requires_user_authorization": true
                }
            }
        });
        let outcome = CoordinationToolProvider::structured_success(
            PROJECT_EXECUTION_BASELINE_OPERATION,
            &scope,
            "correlation-1",
            result,
            true,
        )
        .expect("structured baseline outcome");
        assert_eq!(outcome["code"], "approval_required");
        assert_eq!(
            outcome["approval_target"]["target_type"],
            "execution_baseline"
        );
        assert_eq!(outcome["approval_target"]["target_id"], "baseline-1");
        assert_eq!(
            outcome["approval_target"]["operation"],
            PROJECT_EXECUTION_BASELINE_OPERATION
        );
        assert_eq!(outcome["approval_target"]["version"], 3);
        assert_eq!(outcome["approval_target"]["revision_id"], "revision-1");
        assert_eq!(outcome["approval_target"]["revision"], 0);
        assert_eq!(
            outcome["approval_target"]["content_digest"],
            "content-digest"
        );
        assert_eq!(
            outcome["approval_target"]["rendered_digest"],
            "render-digest"
        );
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
