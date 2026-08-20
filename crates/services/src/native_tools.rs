//! Forge service adapter for the host's scope-derived native tools.
//!
//! This adapter deliberately exposes only read projections and proposal
//! envelopes.  It never calls Task mutation/workflow methods directly; an
//! admitted `task.propose` becomes an `AgentAction` and is executed through
//! the existing coordination/Task services' normal policy and execution
//! steps (inline when a `TaskService` is attached).

use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv6Addr},
    sync::{Arc, RwLock},
    time::Duration,
};

use async_trait::async_trait;
use chrono::Utc;
use config::PublicSearchConfig;
use db::{
    AgentAction, AgentActionListQuery, AgentActionPolicyResult, AgentActionRepo, AgentActionStatus,
    AgentCommitmentListQuery, AgentCommitmentRepo, AgentInboxListQuery, AgentInboxRepo,
    MemoryScopeGrant, SqliteDb,
};
use forge_agent_host::{
    AgentHostError, CanonicalScope, CanonicalScopeType, ForgeToolProvider, PublicSearchScope,
    WorkspaceAccess, MAIN_CHARTER_APPROVAL_TARGET_OPERATION, MAIN_CHARTER_DIFF_OPERATION,
    MAIN_CHARTER_DRAFT_OPERATION, MAIN_CHARTER_READINESS_OPERATION, MAIN_CHARTER_READ_OPERATION,
    MAIN_PROJECT_CREATE_OPERATION, PROJECT_CHARTER_ADOPTION_OPERATION,
    PROJECT_CURRENT_STATE_OPERATION, PROJECT_DECISION_OPERATION, PROJECT_DOCUMENT_OPERATION,
    PROJECT_EVIDENCE_OPERATION, PROJECT_EXECUTION_BASELINE_OPERATION, PROJECT_MILESTONE_OPERATION,
    PROJECT_READINESS_OPERATION, PROJECT_RELEASE_OPERATION,
};
use reqwest::header::ACCEPT;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sqlx::Row;

use crate::{
    agent_chat_policy::guard_agent_chat_content,
    coordination_service::{AgentActionService, ExecuteTaskProposalInput, ProposeActionInput},
    memory::{MemoryAccessContext, MemoryService},
    project_runtime::{load_effective_project_state, ProjectCurrentStateResponse},
    ExecuteMainOrchestrationActionInput, ExecuteProjectOrchestrationActionInput,
    MainOrchestrationActionService, ProjectOrchestrationActionService, TaskService,
};

/// Forge-owned provider injected into native Agent Runtime compositions.
#[derive(Clone)]
pub struct CoordinationToolProvider {
    db: Arc<SqliteDb>,
    actions: AgentActionService,
    memory: MemoryService,
    project_actions: ProjectOrchestrationActionService,
    public_search: Arc<RwLock<Option<PublicSearchConfig>>>,
    /// Shared TaskService used to execute admitted `task.propose` actions
    /// inline, so agent-proposed Tasks materialize without a separate caller.
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
            memory: MemoryService::new(Arc::clone(&db)),
            project_actions: ProjectOrchestrationActionService::new(Arc::clone(&db)),
            public_search: Arc::new(RwLock::new(None)),
            task_service: Arc::new(RwLock::new(None)),
            db,
        }
    }

    /// Attach the shared TaskService so admitted `task.propose` actions
    /// execute inline through the normal Task creation path.
    pub fn set_task_service(&self, task_service: Arc<TaskService>) {
        if let Ok(mut slot) = self.task_service.write() {
            *slot = Some(task_service);
        }
    }

    fn task_service_handle(&self) -> Option<Arc<TaskService>> {
        self.task_service.read().ok().and_then(|slot| slot.clone())
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

    /// Resolve the account represented by a Main Chat without trusting the
    /// opaque chat id or any account id supplied in model arguments.  Main
    /// projections are intentionally account-owned and never fan out into
    /// Project Chat history or private Project memory.
    async fn main_account_id(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
    ) -> Result<String, AgentHostError> {
        let owner_id = sqlx::query_scalar::<_, Option<String>>(
            "SELECT owner_id FROM agent_identity WHERE id = ?",
        )
        .bind(actor_identity_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?
        .flatten()
        .ok_or_else(|| AgentHostError::Authority("Main Agent account is unavailable".to_owned()))?;
        let account_id = match scope.scope_type {
            CanonicalScopeType::Account => scope.scope_id.clone(),
            CanonicalScopeType::AgentChat => {
                let row =
                    sqlx::query("SELECT kind, account_id FROM agent_chat WHERE id = ? LIMIT 1")
                        .bind(&scope.scope_id)
                        .fetch_optional(self.db.pool())
                        .await
                        .map_err(|_| AgentHostError::ProtectedPersistence)?
                        .ok_or_else(|| {
                            AgentHostError::Authority("Main Agent Chat is unavailable".to_owned())
                        })?;
                let kind: String = row
                    .try_get("kind")
                    .map_err(|_| AgentHostError::ProtectedPersistence)?;
                if kind != "account_main" {
                    return Err(AgentHostError::Authority(
                        "global Main Agent operations are unavailable in Project Chat".to_owned(),
                    ));
                }
                row.try_get::<Option<String>, _>("account_id")
                    .map_err(|_| AgentHostError::ProtectedPersistence)?
                    .ok_or_else(|| {
                        AgentHostError::Authority("Main Agent account is unavailable".to_owned())
                    })?
            }
            _ => {
                return Err(AgentHostError::Authority(
                    "global Main Agent operation is unavailable in this scope".to_owned(),
                ));
            }
        };
        if owner_id != account_id {
            return Err(AgentHostError::Authority(
                "actor identity does not own the Main Agent scope".to_owned(),
            ));
        }
        Ok(account_id)
    }

    async fn discovery_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        let account_id = self.main_account_id(actor_identity_id, scope).await?;
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
        let account_id = self.main_account_id(actor_identity_id, scope).await?;
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

    /// Returns only Genesis-owned Charter projections for the authenticated
    /// Main scope.  The caller cannot select a Genesis session, Project, or
    /// account; all three are derived from the persisted Main binding.
    async fn main_charter_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        let account_id = self.main_account_id(actor_identity_id, scope).await?;
        let limit = arguments
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(20)
            .clamp(1, 50) as i64;
        let charter_id = arguments
            .get("charter_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        let revision_id = arguments
            .get("revision_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        let genesis_session_id = arguments
            .get("genesis_session_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        let rows = sqlx::query(
            "SELECT id, genesis_session_id, current_draft_revision_id,
                    current_approved_revision_id, project_mode, maturity,
                    lifecycle, version, updated_at
             FROM project_charter
             WHERE account_id = ? AND project_id IS NULL
               AND (? IS NULL OR id = ?)
               AND (? IS NULL OR genesis_session_id = ?)
               AND (? IS NULL OR current_draft_revision_id = ?
                    OR current_approved_revision_id = ?)
             ORDER BY updated_at DESC, id DESC LIMIT ?",
        )
        .bind(account_id)
        .bind(charter_id)
        .bind(charter_id)
        .bind(genesis_session_id)
        .bind(genesis_session_id)
        .bind(revision_id)
        .bind(revision_id)
        .bind(revision_id)
        .bind(limit)
        .fetch_all(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?;
        Ok(json!({
            "scope": "main",
            "items": rows.into_iter().map(|row| json!({
                "id": row.try_get::<String, _>("id").unwrap_or_default(),
                "genesis_session_id": row.try_get::<Option<String>, _>("genesis_session_id").ok().flatten(),
                "current_draft_revision_id": row.try_get::<Option<String>, _>("current_draft_revision_id").ok().flatten(),
                "current_approved_revision_id": row.try_get::<Option<String>, _>("current_approved_revision_id").ok().flatten(),
                "project_mode": row.try_get::<String, _>("project_mode").unwrap_or_default(),
                "maturity": row.try_get::<String, _>("maturity").unwrap_or_default(),
                "lifecycle": row.try_get::<String, _>("lifecycle").unwrap_or_default(),
                "version": row.try_get::<i64, _>("version").unwrap_or_default(),
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
            .project_orchestration_target(actor_identity_id, scope)
            .await?;
        let limit = arguments
            .get("limit")
            .and_then(Value::as_i64)
            .map(|value| value.clamp(1, 64));
        let projection = load_effective_project_state(&self.db, &project_id, limit)
            .await
            .map_err(|error| AgentHostError::Authority(error.to_string()))?;
        serde_json::to_value(ProjectCurrentStateResponse {
            scope: "project".to_owned(),
            effective_state: projection,
        })
        .map_err(|_| AgentHostError::ProtectedPersistence)
    }

    async fn project_summary_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        let account_id = self.main_account_id(actor_identity_id, scope).await?;
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
        if operation == "web.search" {
            return Err(AgentHostError::Unsupported(
                "web.search is a direct read-only tool; it is not persisted as an AgentAction"
                    .to_owned(),
            ));
        }
        if matches!(
            operation,
            "project.lifecycle"
                | "handoff.publish"
                | "decision.request"
                | "project.release"
                | "project.milestone.release"
        ) {
            return Err(AgentHostError::Authority(
                "This operation is not an admitted native proposal; use the typed scope contract and user-authorized Forge transaction".to_owned(),
            ));
        }
        let payload = arguments
            .get("payload")
            .filter(|value| value.is_object())
            .cloned()
            .ok_or_else(|| {
                AgentHostError::Authority("proposal payload must be an object".into())
            })?;
        validate_proposal_payload(operation, &payload)?;
        let project_chat_target =
            if operation == "task.propose" && scope.scope_type == CanonicalScopeType::AgentChat {
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
                    .project_orchestration_target(actor_identity_id, scope)
                    .await?;
            }
            "commitment.propose" | "commitment.update" | "memory.publish" | "memory.supersede"
            | "session.action" | "review.propose" | "review.request"
                if scope.scope_type != CanonicalScopeType::Task =>
            {
                let project_id = self
                    .project_orchestration_target(actor_identity_id, scope)
                    .await?;
                self.require_project_charter_backed(&project_id).await?;
            }
            _ => {}
        }
        let (requested_permission, target_type, target_id) = match operation {
            MAIN_CHARTER_DRAFT_OPERATION
            | MAIN_CHARTER_READINESS_OPERATION
            | MAIN_CHARTER_DIFF_OPERATION
            | MAIN_CHARTER_APPROVAL_TARGET_OPERATION => {
                let account_id = self.main_account_id(actor_identity_id, scope).await?;
                (
                    "propose_discovery",
                    Some("account".to_owned()),
                    Some(account_id),
                )
            }
            MAIN_PROJECT_CREATE_OPERATION => {
                let account_id = self.main_account_id(actor_identity_id, scope).await?;
                (
                    "propose_project",
                    Some("account".to_owned()),
                    Some(account_id),
                )
            }
            PROJECT_CHARTER_ADOPTION_OPERATION => {
                let project_id = self
                    .project_orchestration_target(actor_identity_id, scope)
                    .await?;
                self.require_project_adoption_scope(&project_id).await?;
                (
                    "propose_project",
                    Some("project".to_owned()),
                    Some(project_id),
                )
            }
            PROJECT_DOCUMENT_OPERATION
            | PROJECT_EXECUTION_BASELINE_OPERATION
            | PROJECT_MILESTONE_OPERATION
            | PROJECT_EVIDENCE_OPERATION
            | PROJECT_READINESS_OPERATION
            | PROJECT_RELEASE_OPERATION => {
                let project_id = self
                    .project_orchestration_target(actor_identity_id, scope)
                    .await?;
                self.require_project_charter_backed(&project_id).await?;
                (
                    "propose_project",
                    Some("project".to_owned()),
                    Some(project_id),
                )
            }
            PROJECT_DECISION_OPERATION => {
                let project_id = self
                    .project_orchestration_target(actor_identity_id, scope)
                    .await?;
                self.require_project_charter_backed(&project_id).await?;
                self.require_active_project_baseline(&project_id, &payload)
                    .await?;
                (
                    "propose_project",
                    Some("project".to_owned()),
                    Some(project_id),
                )
            }
            "message.propose" | "message.send" => (
                "propose_message",
                Some(scope_type_name(scope.scope_type).to_owned()),
                Some(scope.scope_id.clone()),
            ),
            "task.propose" if scope.scope_type == CanonicalScopeType::Project => {
                self.require_project_charter_backed(&scope.scope_id).await?;
                self.validate_task_proposal_binding(&scope.scope_id, &payload)
                    .await?;
                (
                    "propose_task",
                    Some("project".to_owned()),
                    Some(scope.scope_id.clone()),
                )
            }
            "task.propose" if project_chat_target.is_some() => {
                let project_id = project_chat_target.as_deref().ok_or_else(|| {
                    AgentHostError::Authority("Project Agent Chat has no owning Project".to_owned())
                })?;
                self.require_project_charter_backed(project_id).await?;
                self.validate_task_proposal_binding(project_id, &payload)
                    .await?;
                (
                    "propose_task",
                    Some("project".to_owned()),
                    project_chat_target,
                )
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
                    "propose_review",
                    Some(scope_type_name(scope.scope_type).to_owned()),
                    Some(scope.scope_id.clone()),
                )
            }
            "commitment.propose" | "commitment.update" => (
                "propose_commitment",
                Some(scope_type_name(scope.scope_type).to_owned()),
                Some(scope.scope_id.clone()),
            ),
            "memory.publish" | "memory.supersede" => (
                "propose_memory",
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
                (
                    "propose_session",
                    Some("scope".to_owned()),
                    Some(scope.scope_id.clone()),
                )
            }
            _ => {
                return Err(AgentHostError::Authority(
                    "proposal operation is not admitted for this scope".into(),
                ));
            }
        };
        let dedupe_key = required_argument(&arguments, "dedupe_key")?;
        let correlation_id = required_argument(&arguments, "correlation_id")?;
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
                causation_id: arguments
                    .get("causation_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                causation_depth: arguments
                    .get("causation_depth")
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
                requested_permission: requested_permission.to_owned(),
                policy_reason: None,
                target_type,
                target_id,
            })
            .await
            .map_err(service_error)?;
        let mut response = action_value(&action);
        if is_auto_materialized_main_operation(operation)
            && action.policy_result == AgentActionPolicyResult::Allowed
            && action.status == AgentActionStatus::Proposed
        {
            let execution = MainOrchestrationActionService::new(Arc::clone(&self.db))
                .execute(ExecuteMainOrchestrationActionInput {
                    action_id: action.id.clone(),
                    expected_version: action.version,
                    executed_by_type: "agent".to_owned(),
                    executed_by_id: actor_identity_id.to_owned(),
                    idempotency_key: action.dedupe_key.clone(),
                })
                .await
                .map_err(service_error)?;
            let domain_result = execution
                .result_json
                .as_deref()
                .and_then(|value| serde_json::from_str::<Value>(value).ok())
                .unwrap_or(Value::Null);
            if let Some(object) = response.as_object_mut() {
                object.insert("status".to_owned(), Value::String("executed".to_owned()));
                object.insert("materialized".to_owned(), Value::Bool(true));
                object.insert(
                    "domain_committed".to_owned(),
                    Value::Bool(operation == MAIN_CHARTER_DRAFT_OPERATION),
                );
                object.insert("execution_id".to_owned(), Value::String(execution.id));
                object.insert("domain_result".to_owned(), domain_result);
                object.insert("requires_user_authorization".to_owned(), Value::Bool(false));
            }
        } else if is_auto_materialized_project_operation(operation)
            && action.policy_result == AgentActionPolicyResult::Allowed
            && action.status == AgentActionStatus::Proposed
        {
            let execution = self
                .project_actions
                .execute(ExecuteProjectOrchestrationActionInput {
                    action_id: action.id.clone(),
                    expected_version: action.version,
                    executed_by_type: "agent".to_owned(),
                    executed_by_id: actor_identity_id.to_owned(),
                    idempotency_key: action.dedupe_key.clone(),
                })
                .await
                .map_err(service_error)?;
            let domain_result = execution
                .result_json
                .as_deref()
                .and_then(|value| serde_json::from_str::<Value>(value).ok())
                .unwrap_or(Value::Null);
            let requires_user_authorization = domain_result
                .get("requires_user_authorization")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if let Some(object) = response.as_object_mut() {
                object.insert("status".to_owned(), Value::String("executed".to_owned()));
                object.insert("materialized".to_owned(), Value::Bool(true));
                object.insert("domain_committed".to_owned(), Value::Bool(true));
                object.insert("execution_id".to_owned(), Value::String(execution.id));
                object.insert("domain_result".to_owned(), domain_result);
                object.insert(
                    "requires_user_authorization".to_owned(),
                    Value::Bool(requires_user_authorization),
                );
            }
        } else if operation == "task.propose"
            && action.policy_result == AgentActionPolicyResult::Allowed
            && action.status == AgentActionStatus::Proposed
        {
            // An admitted Task proposal materializes through the normal
            // TaskService path immediately; nothing else in the product
            // executes it later. Validation failures (missing plan item,
            // stale baseline, class outside the envelope) surface to the
            // model as this tool call's error and can be corrected in-turn.
            let Some(task_service) = self.task_service_handle() else {
                return Err(AgentHostError::Configuration(
                    "Task proposal execution is not wired to a TaskService".to_owned(),
                ));
            };
            let executed = self
                .actions
                .execute_task_proposal(
                    &task_service,
                    ExecuteTaskProposalInput {
                        action_id: action.id.clone(),
                        expected_version: action.version,
                        executed_by_id: actor_identity_id.to_owned(),
                        idempotency_key: action.dedupe_key.clone(),
                    },
                )
                .await
                .map_err(service_error)?;
            if let Some(object) = response.as_object_mut() {
                object.insert("status".to_owned(), Value::String("executed".to_owned()));
                object.insert("materialized".to_owned(), Value::Bool(true));
                object.insert("domain_committed".to_owned(), Value::Bool(true));
                object.insert(
                    "execution_id".to_owned(),
                    Value::String(executed.execution.id.clone()),
                );
                object.insert(
                    "domain_result".to_owned(),
                    serde_json::json!({
                        "task_id": executed.task.id,
                        "task_status": executed.task.status,
                    }),
                );
                object.insert("requires_user_authorization".to_owned(), Value::Bool(false));
            }
        } else if is_orchestration_operation(operation) {
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
                self.main_account_id(actor_identity_id, scope).await?;
            }
            PublicSearchScope::Project => {
                self.project_orchestration_target(actor_identity_id, scope)
                    .await?;
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

    async fn require_active_project_baseline(
        &self,
        project_id: &str,
        payload: &Value,
    ) -> Result<(), AgentHostError> {
        let baseline_id = required_payload_string(payload_object(payload)?, "baseline_id")?;
        let baseline_revision_id =
            required_payload_string(payload_object(payload)?, "baseline_revision_id")?;
        let current_revision_id = sqlx::query_scalar::<_, Option<String>>(
            "SELECT current_revision_id
             FROM project_execution_baseline
             WHERE id = ? AND project_id = ? AND lifecycle = 'active'
             LIMIT 1",
        )
        .bind(baseline_id)
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?
        .flatten();
        if current_revision_id.as_deref() != Some(baseline_revision_id.as_str()) {
            return Err(AgentHostError::Authority(
                "Project decision must reference the current active execution baseline".to_owned(),
            ));
        }
        Ok(())
    }

    async fn require_project_adoption_scope(&self, project_id: &str) -> Result<(), AgentHostError> {
        let state = sqlx::query(
            "SELECT charter_status, charter_setup_required,
                    current_charter_id, current_charter_revision_id
             FROM project WHERE id = ? LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?
        .ok_or_else(|| AgentHostError::Authority("Project scope is unavailable".to_owned()))?;
        let charter_status: String = state
            .try_get("charter_status")
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        let setup_required: i64 = state
            .try_get("charter_setup_required")
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        let has_current_charter = state
            .try_get::<Option<String>, _>("current_charter_id")
            .map_err(|_| AgentHostError::ProtectedPersistence)?
            .is_some()
            && state
                .try_get::<Option<String>, _>("current_charter_revision_id")
                .map_err(|_| AgentHostError::ProtectedPersistence)?
                .is_some();
        let setup = charter_status == "legacy_unverified" && setup_required == 1;
        let amendment =
            charter_status == "charter_backed" && setup_required == 0 && has_current_charter;
        if !setup && !amendment {
            return Err(AgentHostError::Authority(
                "Project Charter adoption/amendment is unavailable for the current Project state"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    /// Reject a Task proposal the server could only downgrade or duplicate.
    ///
    /// Once a Project has an active user-approved execution baseline, an
    /// implementation Task (task_type `task`/`sub_task`) on a
    /// repository-backed Project becomes runnable only when it names a plan
    /// item from that exact baseline. A proposal without one used to be
    /// admitted silently as a never-runnable plan and stranded at dispatch;
    /// it is rejected here instead, before an `AgentAction` row exists, with
    /// the valid plan-item ids in the error. A plan item that already
    /// carries a non-cancelled Task (any state, `done` included) never
    /// admits a second one — the error names the existing Task so the agent
    /// can act on it. Planning/discovery Tasks stay proposable without a
    /// plan item: they are read-only by construction and never need the
    /// baseline binding. Errors return to the model verbatim in-turn.
    async fn validate_task_proposal_binding(
        &self,
        project_id: &str,
        payload: &Value,
    ) -> Result<(), AgentHostError> {
        let task_type = payload
            .get("task_type")
            .and_then(Value::as_str)
            .unwrap_or("task");
        // Agent proposals normally carry the flat shortcut field, but the
        // payload contract also admits a full `governance` envelope; honor
        // the plan item wherever it was named.
        let plan_item_id = payload
            .get("plan_item_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .or_else(|| {
                payload
                    .get("governance")
                    .and_then(|governance| governance.get("plan_item_id"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
            });
        let repository_backed = sqlx::query_scalar::<_, Option<String>>(
            "SELECT primary_repo_id FROM project WHERE id = ? LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?
        .flatten()
        .is_some();
        let active_plan_items: Option<Vec<String>> = sqlx::query_scalar::<_, String>(
            "SELECT r.plan_items_json
             FROM project_execution_baseline AS b
             JOIN project_execution_baseline_revision AS r
               ON r.id = b.current_revision_id AND r.baseline_id = b.id
             WHERE b.project_id = ? AND b.lifecycle = 'active'
               AND r.lifecycle = 'approved'
             ORDER BY b.updated_at DESC, b.id DESC LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?
        .map(|json| baseline_plan_item_ids(&json));
        let implementation = repository_backed && matches!(task_type, "task" | "sub_task");
        if let Some(plan_items) = active_plan_items.as_deref().filter(|_| implementation) {
            match plan_item_id {
                None => {
                    return Err(AgentHostError::Authority(format!(
                        "task.propose rejected: this Project has an active execution baseline, \
                         so implementation Tasks (task_type \"task\") must include plan_item_id \
                         — without it the Task can never become runnable. Valid plan_item_ids: \
                         [{}]. Propose the Task again with the plan item it implements, or use \
                         task_type \"planning_task\"/\"discovery\" for ungoverned read-only work.",
                        plan_items.join(", "),
                    )));
                }
                Some(candidate) if !plan_items.iter().any(|id| id == candidate) => {
                    return Err(AgentHostError::Authority(format!(
                        "task.propose rejected: plan_item_id \"{candidate}\" is not present in \
                         the Project's active execution baseline. Valid plan_item_ids: [{}].",
                        plan_items.join(", "),
                    )));
                }
                _ => {}
            }
        }
        if let Some(plan_item_id) = plan_item_id {
            let existing = sqlx::query(
                "SELECT g.task_id, t.status
                 FROM project_task_governance AS g
                 JOIN task AS t ON t.id = g.task_id
                 WHERE g.project_id = ? AND g.plan_item_id = ?
                   AND t.status <> 'cancelled'
                 ORDER BY g.created_at ASC LIMIT 1",
            )
            .bind(project_id)
            .bind(plan_item_id)
            .fetch_optional(self.db.pool())
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
            if let Some(existing) = existing {
                let task_id: String = existing
                    .try_get("task_id")
                    .map_err(|_| AgentHostError::ProtectedPersistence)?;
                let status: String = existing
                    .try_get("status")
                    .map_err(|_| AgentHostError::ProtectedPersistence)?;
                return Err(AgentHostError::Authority(format!(
                    "task.propose rejected: plan item \"{plan_item_id}\" is already bound to \
                     Task {task_id} (status: {status}). Do not propose a duplicate — read that \
                     Task and act on it instead (wait for it, address its failure, or have it \
                     cancelled before replacing it).",
                )));
            }
        }
        Ok(())
    }

    async fn require_project_charter_backed(&self, project_id: &str) -> Result<(), AgentHostError> {
        let state = sqlx::query(
            "SELECT charter_status, charter_setup_required,
                    current_charter_id, current_charter_revision_id
             FROM project WHERE id = ? LIMIT 1",
        )
        .bind(project_id)
        .fetch_optional(self.db.pool())
        .await
        .map_err(|_| AgentHostError::ProtectedPersistence)?
        .ok_or_else(|| AgentHostError::Authority("Project scope is unavailable".to_owned()))?;
        let charter_status: String = state
            .try_get("charter_status")
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        let setup_required: i64 = state
            .try_get("charter_setup_required")
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        let charter_id: Option<String> = state
            .try_get("current_charter_id")
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        let revision_id: Option<String> = state
            .try_get("current_charter_revision_id")
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        if charter_status != "charter_backed"
            || setup_required != 0
            || charter_id.is_none()
            || revision_id.is_none()
        {
            return Err(AgentHostError::Authority(
                "Project execution operations remain blocked until a user-approved Charter adoption is committed"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    /// Resolve the one Project owned by the authenticated Project Agent
    /// binding.  This is the only source of Project identity for the typed
    /// orchestration operations; payload `project_id` fields are never read.
    async fn project_orchestration_target(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
    ) -> Result<String, AgentHostError> {
        match scope.scope_type {
            CanonicalScopeType::Project => {
                let project_id = sqlx::query_scalar::<_, Option<String>>(
                    "SELECT p.id
                     FROM project AS p
                     JOIN project_agent_binding AS binding
                       ON binding.project_id = p.id
                      AND binding.identity_id = ?
                      AND binding.state = 'active'
                     WHERE p.id = ?
                     LIMIT 1",
                )
                .bind(actor_identity_id)
                .bind(&scope.scope_id)
                .fetch_optional(self.db.pool())
                .await
                .map_err(|_| AgentHostError::ProtectedPersistence)?
                .flatten();
                project_id.ok_or_else(|| {
                    AgentHostError::Authority(
                        "Project Agent binding does not own this Project scope".to_owned(),
                    )
                })
            }
            CanonicalScopeType::AgentChat => {
                let row = sqlx::query(
                    "SELECT chat.kind, chat.project_id, binding.identity_id
                     FROM agent_chat AS chat
                     JOIN project_agent_binding AS binding
                       ON binding.project_id = chat.project_id
                      AND binding.identity_id = ?
                      AND binding.state = 'active'
                     WHERE chat.id = ? AND chat.kind = 'project'
                     LIMIT 1",
                )
                .bind(actor_identity_id)
                .bind(&scope.scope_id)
                .fetch_optional(self.db.pool())
                .await
                .map_err(|_| AgentHostError::ProtectedPersistence)?
                .ok_or_else(|| {
                    AgentHostError::Authority(
                        "Project Agent Chat is not bound to this identity".to_owned(),
                    )
                })?;
                let _kind: String = row
                    .try_get("kind")
                    .map_err(|_| AgentHostError::ProtectedPersistence)?;
                row.try_get::<Option<String>, _>("project_id")
                    .map_err(|_| AgentHostError::ProtectedPersistence)?
                    .ok_or_else(|| {
                        AgentHostError::Authority(
                            "Project Agent Chat has no owning Project".to_owned(),
                        )
                    })
            }
            _ => Err(AgentHostError::Authority(
                "Project orchestration is unavailable outside the bound Project scope".to_owned(),
            )),
        }
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
        match operation {
            MAIN_CHARTER_READ_OPERATION => {
                self.main_charter_read(actor_identity_id, scope, arguments)
                    .await
            }
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
        }
    }

    async fn propose(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        self.propose(actor_identity_id, scope, operation, arguments)
            .await
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
        .ok_or_else(|| AgentHostError::Authority(format!("{field} is required")))
}

/// Plan-item ids named by a baseline revision's `plan_items_json` column.
/// The authoring surface persists a flat id array, but object entries with
/// an `id` field are accepted for robustness (they match the shape
/// `json_contains_identifier` admits in Task governance validation).
fn baseline_plan_item_ids(plan_items_json: &str) -> Vec<String> {
    serde_json::from_str::<Value>(plan_items_json)
        .ok()
        .and_then(|value| match value {
            Value::Array(items) => Some(items),
            _ => None,
        })
        .map(|items| {
            items
                .iter()
                .filter_map(|item| match item {
                    Value::String(id) => Some(id.clone()),
                    Value::Object(map) => map.get("id").and_then(Value::as_str).map(str::to_owned),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default()
}

fn payload_object(payload: &Value) -> Result<&Map<String, Value>, AgentHostError> {
    payload.as_object().ok_or_else(|| {
        AgentHostError::Authority("Forge orchestration payload must be an object".to_owned())
    })
}

fn reject_unknown_payload_keys(
    object: &Map<String, Value>,
    allowed: &[&str],
    context: &str,
) -> Result<(), AgentHostError> {
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(AgentHostError::Authority(format!(
            "{context} contains unsupported field `{key}`"
        )));
    }
    Ok(())
}

fn required_payload_value<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a Value, AgentHostError> {
    object.get(field).ok_or_else(|| {
        AgentHostError::Authority(format!("orchestration payload field `{field}` is required"))
    })
}

fn required_payload_string(
    object: &Map<String, Value>,
    field: &str,
) -> Result<String, AgentHostError> {
    required_payload_value(object, field)?
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            AgentHostError::Authority(format!(
                "orchestration payload field `{field}` must be a non-empty string"
            ))
        })
}

fn required_payload_integer(
    object: &Map<String, Value>,
    field: &str,
) -> Result<i64, AgentHostError> {
    let value = required_payload_value(object, field)?
        .as_i64()
        .ok_or_else(|| {
            AgentHostError::Authority(format!(
                "orchestration payload field `{field}` must be an integer"
            ))
        })?;
    if value < 1 {
        return Err(AgentHostError::Authority(format!(
            "orchestration payload field `{field}` must be at least 1"
        )));
    }
    Ok(value)
}

fn required_payload_nonnegative_integer(
    object: &Map<String, Value>,
    field: &str,
) -> Result<i64, AgentHostError> {
    let value = required_payload_value(object, field)?
        .as_i64()
        .ok_or_else(|| {
            AgentHostError::Authority(format!(
                "orchestration payload field `{field}` must be an integer"
            ))
        })?;
    if value < 0 {
        return Err(AgentHostError::Authority(format!(
            "orchestration payload field `{field}` must be non-negative"
        )));
    }
    Ok(value)
}

fn optional_payload_string(object: &Map<String, Value>, field: &str) -> Result<(), AgentHostError> {
    if let Some(value) = object.get(field) {
        if !value.is_null() && value.as_str().is_none() {
            return Err(AgentHostError::Authority(format!(
                "orchestration payload field `{field}` must be a string or null"
            )));
        }
    }
    Ok(())
}

fn optional_nonempty_payload_string(
    object: &Map<String, Value>,
    field: &str,
) -> Result<(), AgentHostError> {
    if let Some(value) = object.get(field) {
        if !value.is_null() && value.as_str().is_none_or(|value| value.trim().is_empty()) {
            return Err(AgentHostError::Authority(format!(
                "orchestration payload field `{field}` must be a non-empty string or null"
            )));
        }
    }
    Ok(())
}

fn validate_enum_payload(
    object: &Map<String, Value>,
    field: &str,
    allowed: &[&str],
) -> Result<String, AgentHostError> {
    let value = required_payload_string(object, field)?;
    if !allowed.contains(&value.as_str()) {
        return Err(AgentHostError::Authority(format!(
            "orchestration payload field `{field}` is not an admitted value"
        )));
    }
    Ok(value)
}

fn validate_string_array_field(
    object: &Map<String, Value>,
    field: &str,
) -> Result<(), AgentHostError> {
    let Some(value) = object.get(field) else {
        return Ok(());
    };
    let values = value.as_array().ok_or_else(|| {
        AgentHostError::Authority(format!(
            "orchestration payload field `{field}` must be an array of strings"
        ))
    })?;
    if values.iter().any(|value| value.as_str().is_none()) {
        return Err(AgentHostError::Authority(format!(
            "orchestration payload field `{field}` must be an array of strings"
        )));
    }
    Ok(())
}

fn validate_principal_payload(value: &Value, context: &str) -> Result<(), AgentHostError> {
    let object = value
        .as_object()
        .ok_or_else(|| AgentHostError::Authority(format!("{context} must be an object")))?;
    reject_unknown_payload_keys(object, &["kind", "id", "display_name"], context)?;
    let kind = required_payload_string(object, "kind")?;
    if !["user", "agent", "worker", "reviewer", "service", "system"].contains(&kind.as_str()) {
        return Err(AgentHostError::Authority(format!(
            "{context}.kind is not an admitted principal kind"
        )));
    }
    required_payload_string(object, "id")?;
    optional_payload_string(object, "display_name")
}

fn validate_provenance_ref_payload(value: &Value, context: &str) -> Result<(), AgentHostError> {
    let object = value
        .as_object()
        .ok_or_else(|| AgentHostError::Authority(format!("{context} must be an object")))?;
    reject_unknown_payload_keys(
        object,
        &[
            "source_kind",
            "source_id",
            "revision_id",
            "digest",
            "label",
            "observed_at",
        ],
        context,
    )?;
    validate_enum_payload(
        object,
        "source_kind",
        &[
            "user",
            "main_chat",
            "project_chat",
            "research",
            "task",
            "validation",
            "document",
            "decision",
            "milestone",
            "release",
            "system",
        ],
    )?;
    required_payload_string(object, "source_id")?;
    for field in ["revision_id", "digest", "label", "observed_at"] {
        optional_payload_string(object, field)?;
    }
    Ok(())
}

fn validate_artifact_ref_payload(value: &Value, context: &str) -> Result<(), AgentHostError> {
    let object = value
        .as_object()
        .ok_or_else(|| AgentHostError::Authority(format!("{context} must be an object")))?;
    reject_unknown_payload_keys(
        object,
        &[
            "artifact_id",
            "revision_id",
            "content_digest",
            "render_version",
            "render_digest",
        ],
        context,
    )?;
    for field in ["artifact_id", "revision_id", "content_digest"] {
        required_payload_string(object, field)?;
    }
    optional_payload_string(object, "render_version")?;
    optional_payload_string(object, "render_digest")
}

fn validate_revision_provenance_payload(value: &Value) -> Result<(), AgentHostError> {
    let object = value.as_object().ok_or_else(|| {
        AgentHostError::Authority("charter.draft.provenance must be an object".to_owned())
    })?;
    reject_unknown_payload_keys(
        object,
        &[
            "author",
            "profile_revision",
            "operating_skill_revision",
            "source_refs",
            "change_summary",
            "material_diff",
        ],
        "charter.draft.provenance",
    )?;
    validate_principal_payload(
        required_payload_value(object, "author")?,
        "charter.draft.provenance.author",
    )?;
    required_payload_string(object, "change_summary")?;
    for field in [
        "profile_revision",
        "operating_skill_revision",
        "material_diff",
    ] {
        optional_payload_string(object, field)?;
    }
    if let Some(value) = object.get("source_refs") {
        let values = value.as_array().ok_or_else(|| {
            AgentHostError::Authority(
                "charter.draft.provenance.source_refs must be an array".to_owned(),
            )
        })?;
        for (index, value) in values.iter().enumerate() {
            validate_provenance_ref_payload(
                value,
                &format!("charter.draft.provenance.source_refs[{index}]"),
            )?;
        }
    }
    Ok(())
}

fn validate_charter_risk_payload(value: &Value, context: &str) -> Result<(), AgentHostError> {
    let object = value
        .as_object()
        .ok_or_else(|| AgentHostError::Authority(format!("{context} must be an object")))?;
    reject_unknown_payload_keys(
        object,
        &[
            "id",
            "description",
            "impact",
            "treatment",
            "revisit_trigger",
            "owner",
        ],
        context,
    )?;
    required_payload_string(object, "id")?;
    required_payload_string(object, "description")?;
    for field in ["impact", "treatment", "revisit_trigger"] {
        optional_payload_string(object, field)?;
    }
    if let Some(owner) = object.get("owner") {
        if !owner.is_null() {
            validate_principal_payload(owner, &format!("{context}.owner"))?;
        }
    }
    Ok(())
}

fn validate_charter_content_payload(value: &Value) -> Result<(), AgentHostError> {
    let object = value.as_object().ok_or_else(|| {
        AgentHostError::Authority("charter.draft.content must be an object".to_owned())
    })?;
    reject_unknown_payload_keys(
        object,
        &[
            "identity",
            "problem_and_people",
            "core_experience",
            "scope",
            "success",
            "constraints_and_risks",
            "knowledge_ledger",
            "handoff_note",
        ],
        "charter.draft.content",
    )?;
    let identity = required_payload_value(object, "identity")?
        .as_object()
        .ok_or_else(|| {
            AgentHostError::Authority("charter identity must be an object".to_owned())
        })?;
    reject_unknown_payload_keys(
        identity,
        &[
            "working_name",
            "slug_proposal",
            "one_line_vision",
            "maturity",
            "lifecycle_intent",
            "project_type",
            "value_proposition",
        ],
        "charter.draft.content.identity",
    )?;
    required_payload_string(identity, "working_name")?;
    required_payload_string(identity, "one_line_vision")?;
    validate_enum_payload(
        identity,
        "maturity",
        &["prototype", "mvp", "production", "critical"],
    )?;
    for field in [
        "slug_proposal",
        "lifecycle_intent",
        "project_type",
        "value_proposition",
    ] {
        optional_payload_string(identity, field)?;
    }

    let problem = required_payload_value(object, "problem_and_people")?
        .as_object()
        .ok_or_else(|| {
            AgentHostError::Authority("charter problem_and_people must be an object".to_owned())
        })?;
    reject_unknown_payload_keys(
        problem,
        &[
            "problem_or_opportunity",
            "target_users",
            "beneficiaries",
            "jobs_pains_opportunity",
            "current_alternatives",
            "stakeholders",
            "excluded_audiences",
        ],
        "charter.draft.content.problem_and_people",
    )?;
    required_payload_string(problem, "problem_or_opportunity")?;
    for field in [
        "target_users",
        "beneficiaries",
        "jobs_pains_opportunity",
        "current_alternatives",
        "stakeholders",
        "excluded_audiences",
    ] {
        validate_string_array_field(problem, field)?;
    }

    let core = required_payload_value(object, "core_experience")?
        .as_object()
        .ok_or_else(|| {
            AgentHostError::Authority("charter core_experience must be an object".to_owned())
        })?;
    reject_unknown_payload_keys(
        core,
        &["primary_outcome", "core_loop", "principal_journeys"],
        "charter.draft.content.core_experience",
    )?;
    required_payload_string(core, "primary_outcome")?;
    optional_payload_string(core, "core_loop")?;
    validate_string_array_field(core, "principal_journeys")?;

    for (field, allowed) in [
        (
            "scope",
            &[
                "must_have_outcomes",
                "required_deliverables",
                "later_possibilities",
                "explicit_non_goals",
            ][..],
        ),
        (
            "success",
            &[
                "qualitative_outcome",
                "success_signals",
                "acceptance_statements",
                "required_evidence",
                "non_claims",
            ][..],
        ),
    ] {
        let child = required_payload_value(object, field)?
            .as_object()
            .ok_or_else(|| {
                AgentHostError::Authority(format!("charter {field} must be an object"))
            })?;
        reject_unknown_payload_keys(child, allowed, &format!("charter.draft.content.{field}"))?;
        for key in allowed.iter().copied() {
            if key == "qualitative_outcome" {
                optional_payload_string(child, key)?;
            } else {
                validate_string_array_field(child, key)?;
            }
        }
    }

    let constraints = required_payload_value(object, "constraints_and_risks")?
        .as_object()
        .ok_or_else(|| {
            AgentHostError::Authority("charter constraints_and_risks must be an object".to_owned())
        })?;
    let constraint_fields = [
        "product",
        "time_and_budget",
        "technology",
        "data",
        "integrations",
        "security_privacy_compliance",
        "accessibility",
        "operations",
        "migration",
        "launch",
        "agent_authority",
        "risks",
    ];
    reject_unknown_payload_keys(
        constraints,
        &constraint_fields,
        "charter.draft.content.constraints_and_risks",
    )?;
    for field in &constraint_fields[..constraint_fields.len() - 1] {
        validate_string_array_field(constraints, field)?;
    }
    if let Some(risks) = constraints.get("risks") {
        let risks = risks.as_array().ok_or_else(|| {
            AgentHostError::Authority("charter constraints risks must be an array".to_owned())
        })?;
        for (index, risk) in risks.iter().enumerate() {
            validate_charter_risk_payload(risk, &format!("charter.draft.content.risks[{index}]"))?;
        }
    }

    let ledger = required_payload_value(object, "knowledge_ledger")?
        .as_object()
        .ok_or_else(|| {
            AgentHostError::Authority("charter knowledge_ledger must be an object".to_owned())
        })?;
    reject_unknown_payload_keys(ledger, &["items"], "charter.draft.content.knowledge_ledger")?;
    let items = required_payload_value(ledger, "items")?
        .as_array()
        .ok_or_else(|| {
            AgentHostError::Authority("charter knowledge_ledger.items must be an array".to_owned())
        })?;
    for (index, item) in items.iter().enumerate() {
        let item = item.as_object().ok_or_else(|| {
            AgentHostError::Authority(format!("charter knowledge item {index} must be an object"))
        })?;
        reject_unknown_payload_keys(
            item,
            &[
                "id",
                "statement",
                "kind",
                "normative",
                "transfer_approved",
                "provenance",
                "confidence",
                "observed_at",
                "freshness_expires_at",
                "impact",
                "owner",
                "default_value",
                "revisit_trigger",
                "falsification_evidence",
                "blocking",
            ],
            &format!("charter.draft.content.knowledge_ledger.items[{index}]"),
        )?;
        required_payload_string(item, "id")?;
        required_payload_string(item, "statement")?;
        validate_enum_payload(
            item,
            "kind",
            &[
                "observed_fact",
                "user_decision",
                "research_finding",
                "assumption",
                "hypothesis",
                "open_decision",
                "research_queue",
            ],
        )?;
        for field in ["normative", "transfer_approved", "blocking"] {
            if item.get(field).and_then(Value::as_bool).is_none() {
                return Err(AgentHostError::Authority(format!(
                    "charter knowledge item field `{field}` must be boolean"
                )));
            }
        }
        for field in [
            "observed_at",
            "freshness_expires_at",
            "impact",
            "default_value",
            "revisit_trigger",
            "falsification_evidence",
        ] {
            optional_payload_string(item, field)?;
        }
        if let Some(owner) = item.get("owner") {
            if !owner.is_null() {
                validate_principal_payload(owner, &format!("knowledge item {index}.owner"))?;
            }
        }
        if let Some(provenance) = item.get("provenance") {
            let provenance = provenance.as_array().ok_or_else(|| {
                AgentHostError::Authority(
                    "charter knowledge item provenance must be an array".to_owned(),
                )
            })?;
            for (source_index, source) in provenance.iter().enumerate() {
                validate_provenance_ref_payload(
                    source,
                    &format!("knowledge item {index}.provenance[{source_index}]"),
                )?;
            }
        }
    }

    if let Some(note) = object.get("handoff_note") {
        if !note.is_null() {
            let note = note.as_object().ok_or_else(|| {
                AgentHostError::Authority(
                    "charter handoff_note must be an object or null".to_owned(),
                )
            })?;
            reject_unknown_payload_keys(
                note,
                &[
                    "recommended_first_action",
                    "bounded_summary",
                    "unresolved_item_ids",
                ],
                "charter.draft.content.handoff_note",
            )?;
            optional_payload_string(note, "recommended_first_action")?;
            optional_payload_string(note, "bounded_summary")?;
            validate_string_array_field(note, "unresolved_item_ids")?;
        }
    }
    Ok(())
}

fn validate_document_content_payload(kind: &str, value: &Value) -> Result<(), AgentHostError> {
    let object = value.as_object().ok_or_else(|| {
        AgentHostError::Authority("project.document.content must be an object".to_owned())
    })?;
    let (allowed, required): (&[&str], &[&str]) = match kind {
        "research" => (
            &[
                "question",
                "decision_informed",
                "scope",
                "stopping_condition",
                "sources",
                "findings",
                "evidence",
                "inferences",
                "alternatives",
                "recommendation",
                "uncertainty",
                "unresolved_questions",
                "affected_artifact_ids",
                "affected_decision_ids",
            ],
            &[
                "question",
                "decision_informed",
                "scope",
                "stopping_condition",
            ],
        ),
        "delivery_brief" => (
            &[
                "intended_deliverables",
                "boundaries",
                "plan_items",
                "acceptance_matrix",
                "risks",
                "rollback_and_recovery",
                "adaptive_envelope",
                "governing_charter_revision_id",
            ],
            &[],
        ),
        "product_spec" => (
            &[
                "problem_and_outcome",
                "actors",
                "journeys_and_flows",
                "functional_requirements",
                "loading_empty_error_recovery_states",
                "acceptance_scenarios",
                "non_functional_and_safety_requirements",
                "out_of_scope",
                "traceability",
            ],
            &["problem_and_outcome"],
        ),
        "design" => (
            &[
                "experience_principles",
                "information_architecture",
                "flows",
                "design_tokens_reference",
                "component_states",
                "responsive_behavior",
                "accessibility",
                "prototype_or_evidence_links",
                "open_decisions",
            ],
            &[],
        ),
        "architecture" => (
            &[
                "context_and_constraints",
                "system_boundary",
                "components_and_data",
                "interfaces",
                "security_and_privacy",
                "concurrency",
                "failure_and_recovery",
                "observability_and_operations",
                "migrations",
                "alternatives_and_tradeoffs",
                "validation_plan",
            ],
            &["context_and_constraints"],
        ),
        "execution_plan" => (
            &[
                "ordered_milestone_outcomes",
                "dependencies",
                "risks",
                "linked_artifact_refs",
                "task_queries_or_ids",
                "acceptance_evidence_contract",
                "release_notes",
                "known_issues",
            ],
            &[],
        ),
        _ => {
            return Err(AgentHostError::Authority(
                "project.document kind is not admitted".to_owned(),
            ));
        }
    };
    reject_unknown_payload_keys(object, allowed, "project.document.content")?;
    for field in required {
        required_payload_string(object, field)?;
    }
    for field in [
        "actors",
        "journeys_and_flows",
        "functional_requirements",
        "loading_empty_error_recovery_states",
        "non_functional_and_safety_requirements",
        "out_of_scope",
        "experience_principles",
        "information_architecture",
        "flows",
        "component_states",
        "responsive_behavior",
        "accessibility",
        "prototype_or_evidence_links",
        "open_decisions",
        "ordered_milestone_outcomes",
        "dependencies",
        "task_queries_or_ids",
        "release_notes",
        "known_issues",
        "findings",
        "evidence",
        "inferences",
        "alternatives",
        "uncertainty",
        "unresolved_questions",
        "affected_artifact_ids",
        "affected_decision_ids",
        "intended_deliverables",
        "boundaries",
        "rollback_and_recovery",
        "adaptive_envelope",
    ] {
        validate_string_array_field(object, field)?;
    }
    for field in [
        "recommendation",
        "governing_charter_revision_id",
        "design_tokens_reference",
    ] {
        optional_payload_string(object, field)?;
    }
    if let Some(refs) = object.get("traceability") {
        let refs = refs.as_array().ok_or_else(|| {
            AgentHostError::Authority("project.document.traceability must be an array".to_owned())
        })?;
        for (index, reference) in refs.iter().enumerate() {
            validate_artifact_ref_payload(
                reference,
                &format!("project.document.traceability[{index}]"),
            )?;
        }
    }
    if let Some(refs) = object.get("linked_artifact_refs") {
        let refs = refs.as_array().ok_or_else(|| {
            AgentHostError::Authority(
                "project.document.linked_artifact_refs must be an array".to_owned(),
            )
        })?;
        for (index, reference) in refs.iter().enumerate() {
            validate_artifact_ref_payload(
                reference,
                &format!("project.document.linked_artifact_refs[{index}]"),
            )?;
        }
    }
    Ok(())
}

fn validate_execution_baseline_content_payload(value: &Value) -> Result<(), AgentHostError> {
    let object = value.as_object().ok_or_else(|| {
        AgentHostError::Authority("project.execution_baseline.content must be an object".to_owned())
    })?;
    reject_unknown_payload_keys(
        object,
        &[
            "charter_revision",
            "document_revisions",
            "plan_item_ids",
            "milestone_ids",
            "milestone_definition_revision_ids",
            "primary_milestone_id",
            "release_policy_revision",
            "release_policy_digest",
            "release_policy",
            "acceptance_evidence_matrix",
            "capability_classes",
            "risk_classes",
            "reviewer_independence_rules",
            "elevated_operations",
            "adaptive_envelope",
            "rollback_and_recovery",
            "exclusions",
        ],
        "project.execution_baseline.content",
    )?;
    validate_artifact_ref_payload(
        required_payload_value(object, "charter_revision")?,
        "project.execution_baseline.content.charter_revision",
    )?;
    for field in [
        "document_revisions",
        "plan_item_ids",
        "milestone_ids",
        "milestone_definition_revision_ids",
        "capability_classes",
        "risk_classes",
        "reviewer_independence_rules",
        "elevated_operations",
        "rollback_and_recovery",
        "exclusions",
    ] {
        if field == "document_revisions" {
            let refs = required_payload_value(object, field)?
                .as_array()
                .ok_or_else(|| AgentHostError::Authority(format!("{field} must be an array")))?;
            for (index, reference) in refs.iter().enumerate() {
                validate_artifact_ref_payload(
                    reference,
                    &format!("project.execution_baseline.content.document_revisions[{index}]"),
                )?;
            }
        } else {
            validate_string_array_field(object, field)?;
        }
    }
    optional_payload_string(object, "primary_milestone_id")?;
    required_payload_string(object, "release_policy_revision")?;
    // The digest is server-derived from the frozen policy when omitted.
    optional_payload_string(object, "release_policy_digest")?;
    validate_execution_baseline_release_policy_payload(required_payload_value(
        object,
        "release_policy",
    )?)?;
    if let Some(matrix) = object.get("acceptance_evidence_matrix") {
        if !matrix.is_array() {
            return Err(AgentHostError::Authority(
                "acceptance_evidence_matrix must be an array".to_owned(),
            ));
        }
    }
    let envelope = required_payload_value(object, "adaptive_envelope")?
        .as_object()
        .ok_or_else(|| {
            AgentHostError::Authority("adaptive_envelope must be an object".to_owned())
        })?;
    reject_unknown_payload_keys(
        envelope,
        &[
            "allowed_task_operations",
            "fixed_outcomes",
            "fixed_acceptance",
            "fixed_risk_classes",
            "forbidden_side_effects",
            "elevated_operations",
        ],
        "project.execution_baseline.content.adaptive_envelope",
    )?;
    for field in [
        "allowed_task_operations",
        "fixed_outcomes",
        "fixed_acceptance",
        "fixed_risk_classes",
        "forbidden_side_effects",
        "elevated_operations",
    ] {
        validate_string_array_field(envelope, field)?;
    }
    Ok(())
}

fn validate_execution_baseline_release_policy_payload(value: &Value) -> Result<(), AgentHostError> {
    let policy = value.as_object().ok_or_else(|| {
        AgentHostError::Authority(
            "project.execution_baseline.content.release_policy must be an object".to_owned(),
        )
    })?;
    reject_unknown_payload_keys(
        policy,
        &[
            "schema_version",
            "revision",
            "required_check_definition_revisions",
            "reviewer_independence_rules",
            "manual_attestation_rules",
            "waiver_rules",
            "evidence_kinds",
            "evidence_contexts",
            "evidence_freshness_rules",
            "dependency_rules",
            "stale_input_rules",
            "forbidden_side_effects",
            "known_issue_rules",
            "correction_rules",
            "purge_rules",
        ],
        "project.execution_baseline.content.release_policy",
    )?;
    let schema = required_payload_string(policy, "schema_version")?;
    if schema != "forge.execution-baseline-release-policy/v1" {
        return Err(AgentHostError::Authority(format!(
            "release_policy.schema_version {schema:?} is not the current Forge schema; \
             use \"forge.execution-baseline-release-policy/v1\""
        )));
    }
    required_payload_string(policy, "revision")?;
    for field in [
        "required_check_definition_revisions",
        "reviewer_independence_rules",
        "manual_attestation_rules",
        "waiver_rules",
        "evidence_kinds",
        "evidence_contexts",
        "evidence_freshness_rules",
        "dependency_rules",
        "stale_input_rules",
        "forbidden_side_effects",
        "known_issue_rules",
        "correction_rules",
        "purge_rules",
    ] {
        validate_string_array_field(policy, field)?;
    }
    Ok(())
}

fn validate_milestone_content_payload(value: &Value) -> Result<(), AgentHostError> {
    let object = value.as_object().ok_or_else(|| {
        AgentHostError::Authority("project.milestone.content must be an object".to_owned())
    })?;
    reject_unknown_payload_keys(
        object,
        &[
            "name",
            "outcome",
            "included_scope",
            "excluded_scope",
            "charter_revision",
            "document_revisions",
            "task_ids",
            "dependencies",
            "risks",
            "acceptance_checks",
            "evidence_requirements",
            "known_issues",
            "target_date",
        ],
        "project.milestone.content",
    )?;
    required_payload_string(object, "name")?;
    required_payload_string(object, "outcome")?;
    for field in [
        "included_scope",
        "excluded_scope",
        "task_ids",
        "dependencies",
        "known_issues",
    ] {
        validate_string_array_field(object, field)?;
    }
    optional_payload_string(object, "target_date")?;
    if let Some(reference) = object.get("charter_revision") {
        if !reference.is_null() {
            validate_artifact_ref_payload(reference, "project.milestone.content.charter_revision")?;
        }
    }
    if let Some(references) = object.get("document_revisions") {
        let references = references.as_array().ok_or_else(|| {
            AgentHostError::Authority(
                "project.milestone.document_revisions must be an array".to_owned(),
            )
        })?;
        for (index, reference) in references.iter().enumerate() {
            validate_artifact_ref_payload(
                reference,
                &format!("project.milestone.document_revisions[{index}]"),
            )?;
        }
    }
    if let Some(risks) = object.get("risks") {
        let risks = risks.as_array().ok_or_else(|| {
            AgentHostError::Authority("project.milestone.risks must be an array".to_owned())
        })?;
        for (index, risk) in risks.iter().enumerate() {
            validate_charter_risk_payload(risk, &format!("project.milestone.risks[{index}]"))?;
        }
    }
    for field in ["acceptance_checks", "evidence_requirements"] {
        if let Some(value) = object.get(field) {
            if !value.is_array() {
                return Err(AgentHostError::Authority(format!(
                    "project.milestone.{field} must be an array"
                )));
            }
        }
    }
    Ok(())
}

fn validate_orchestration_payload(operation: &str, payload: &Value) -> Result<(), AgentHostError> {
    let object = payload_object(payload)?;
    match operation {
        MAIN_CHARTER_DRAFT_OPERATION => {
            reject_unknown_payload_keys(
                object,
                &[
                    "action",
                    "charter_id",
                    "base_revision_id",
                    "project_mode",
                    "maturity",
                    "content",
                    "rendered_view",
                    "render_version",
                    "provenance",
                ],
                "charter.draft",
            )?;
            if required_payload_string(object, "action")? != "save_revision" {
                return Err(AgentHostError::Authority(
                    "charter.draft action must be save_revision".to_owned(),
                ));
            }
            required_payload_string(object, "charter_id")?;
            optional_payload_string(object, "base_revision_id")?;
            validate_enum_payload(object, "project_mode", &["compact", "standard"])?;
            validate_enum_payload(
                object,
                "maturity",
                &["prototype", "mvp", "production", "critical"],
            )?;
            validate_charter_content_payload(required_payload_value(object, "content")?)?;
            // The server renders the canonical view itself; these fields are
            // verified only when a caller round-trips exact server values.
            optional_payload_string(object, "rendered_view")?;
            optional_payload_string(object, "render_version")?;
            validate_revision_provenance_payload(required_payload_value(object, "provenance")?)?;
        }
        MAIN_CHARTER_READINESS_OPERATION => {
            reject_unknown_payload_keys(
                object,
                &[
                    "action",
                    "charter_id",
                    "revision_id",
                    "content_digest",
                    "render_digest",
                    "expected_charter_version",
                ],
                "charter.readiness",
            )?;
            if required_payload_string(object, "action")? != "evaluate" {
                return Err(AgentHostError::Authority(
                    "charter.readiness action must be evaluate".to_owned(),
                ));
            }
            for field in [
                "charter_id",
                "revision_id",
                "content_digest",
                "render_digest",
            ] {
                required_payload_string(object, field)?;
            }
            required_payload_integer(object, "expected_charter_version")?;
        }
        MAIN_CHARTER_DIFF_OPERATION => {
            reject_unknown_payload_keys(
                object,
                &[
                    "action",
                    "charter_id",
                    "base_revision_id",
                    "candidate_revision_id",
                ],
                "charter.diff",
            )?;
            if required_payload_string(object, "action")? != "compare_revisions" {
                return Err(AgentHostError::Authority(
                    "charter.diff action must be compare_revisions".to_owned(),
                ));
            }
            for field in ["charter_id", "base_revision_id", "candidate_revision_id"] {
                required_payload_string(object, field)?;
            }
        }
        MAIN_CHARTER_APPROVAL_TARGET_OPERATION => {
            reject_unknown_payload_keys(
                object,
                &[
                    "action",
                    "charter_id",
                    "revision_id",
                    "content_digest",
                    "render_digest",
                    "expected_charter_version",
                    "approved_project_name",
                    "approved_project_slug",
                    "project_mode",
                    "selected_project_agent_identity_id",
                    "selected_project_agent_profile_revision_id",
                    "selected_project_agent_operating_skill_revision",
                    "selected_project_agent_policy_digest",
                ],
                "charter.approval_target",
            )?;
            if required_payload_string(object, "action")? != "present" {
                return Err(AgentHostError::Authority(
                    "charter.approval_target action must be present".to_owned(),
                ));
            }
            for field in [
                "charter_id",
                "revision_id",
                "content_digest",
                "render_digest",
                "approved_project_name",
                "selected_project_agent_identity_id",
                "selected_project_agent_profile_revision_id",
                "selected_project_agent_operating_skill_revision",
                "selected_project_agent_policy_digest",
            ] {
                required_payload_string(object, field)?;
            }
            optional_payload_string(object, "approved_project_slug")?;
            validate_enum_payload(object, "project_mode", &["compact", "standard"])?;
            required_payload_integer(object, "expected_charter_version")?;
        }
        MAIN_PROJECT_CREATE_OPERATION => {
            reject_unknown_payload_keys(object, &["action", "approval_id"], "project.create")?;
            if required_payload_string(object, "action")? != "create_from_approval" {
                return Err(AgentHostError::Authority(
                    "project.create action must be create_from_approval".to_owned(),
                ));
            }
            required_payload_string(object, "approval_id")?;
        }
        PROJECT_CHARTER_ADOPTION_OPERATION => {
            reject_unknown_payload_keys(
                object,
                &[
                    "action",
                    "charter_id",
                    "base_revision_id",
                    "expected_charter_version",
                    "project_mode",
                    "maturity",
                    "content",
                    "rendered_view",
                    "render_version",
                    "provenance",
                ],
                "project.charter.adoption",
            )?;
            if required_payload_string(object, "action")? != "draft_revision" {
                return Err(AgentHostError::Authority(
                    "project.charter.adoption action must be draft_revision".to_owned(),
                ));
            }
            // A reference to the Project's existing adoption Charter, not the
            // id it will be stored under: omit it to start one.
            optional_payload_string(object, "charter_id")?;
            optional_payload_string(object, "base_revision_id")?;
            required_payload_nonnegative_integer(object, "expected_charter_version")?;
            validate_enum_payload(object, "project_mode", &["compact", "standard"])?;
            validate_enum_payload(
                object,
                "maturity",
                &["prototype", "mvp", "production", "critical"],
            )?;
            validate_charter_content_payload(required_payload_value(object, "content")?)?;
            // Server-rendered, like the Main Charter draft path: a model
            // cannot reproduce the renderer byte-for-byte, so these are
            // accepted only as an optional round-trip of a server value.
            optional_payload_string(object, "rendered_view")?;
            optional_payload_string(object, "render_version")?;
            validate_revision_provenance_payload(required_payload_value(object, "provenance")?)?;
        }
        PROJECT_DOCUMENT_OPERATION => {
            let action = validate_enum_payload(
                object,
                "action",
                &["draft_revision", "propose_approval", "approve"],
            )?;
            required_payload_string(object, "document_id")?;
            let kind = validate_enum_payload(
                object,
                "kind",
                &[
                    "research",
                    "delivery_brief",
                    "product_spec",
                    "design",
                    "architecture",
                    "execution_plan",
                ],
            )?;
            required_payload_string(object, "title")?;
            required_payload_integer(object, "expected_document_version")?;
            if action == "approve" {
                reject_unknown_payload_keys(
                    object,
                    &[
                        "action",
                        "document_id",
                        "kind",
                        "title",
                        "revision_id",
                        "content_digest",
                        "render_digest",
                        "expected_document_version",
                        "baseline_id",
                        "baseline_revision_id",
                        "envelope_digest",
                    ],
                    "project.document",
                )?;
                required_payload_string(object, "revision_id")?;
                required_payload_string(object, "content_digest")?;
                required_payload_string(object, "render_digest")?;
                optional_nonempty_payload_string(object, "baseline_id")?;
                optional_nonempty_payload_string(object, "baseline_revision_id")?;
                optional_nonempty_payload_string(object, "envelope_digest")?;
                let baseline_id = object
                    .get("baseline_id")
                    .is_some_and(|value| !value.is_null());
                let baseline_revision_id = object
                    .get("baseline_revision_id")
                    .is_some_and(|value| !value.is_null());
                let envelope_digest = object
                    .get("envelope_digest")
                    .is_some_and(|value| !value.is_null());
                if baseline_id != baseline_revision_id || baseline_id != envelope_digest {
                    return Err(AgentHostError::Authority(
                        "project.document approval baseline_id, baseline_revision_id, and envelope_digest must be supplied together"
                            .to_owned(),
                    ));
                }
            } else {
                reject_unknown_payload_keys(
                    object,
                    &[
                        "action",
                        "document_id",
                        "kind",
                        "title",
                        "base_revision_id",
                        "expected_document_version",
                        "content",
                    ],
                    "project.document",
                )?;
                optional_payload_string(object, "base_revision_id")?;
                validate_document_content_payload(
                    &kind,
                    required_payload_value(object, "content")?,
                )?;
            }
        }
        PROJECT_DECISION_OPERATION => {
            reject_unknown_payload_keys(
                object,
                &[
                    "action",
                    "question",
                    "options",
                    "selected_outcome",
                    "rationale",
                    "decision_class",
                    "baseline_id",
                    "baseline_revision_id",
                    "expected_project_version",
                    "decision_id",
                    "affected_artifact_refs",
                    "affected_task_ids",
                    "affected_milestone_ids",
                ],
                "project.decision",
            )?;
            validate_enum_payload(object, "action", &["record_candidate", "record_effective"])?;
            required_payload_string(object, "question")?;
            validate_enum_payload(object, "decision_class", &["project_implementation"])?;
            required_payload_string(object, "baseline_id")?;
            required_payload_string(object, "baseline_revision_id")?;
            required_payload_integer(object, "expected_project_version")?;
            optional_payload_string(object, "selected_outcome")?;
            optional_payload_string(object, "rationale")?;
            optional_payload_string(object, "decision_id")?;
            let action = required_payload_string(object, "action")?;
            if action == "record_effective" {
                required_payload_string(object, "decision_id")?;
            }
            for field in ["options", "affected_task_ids", "affected_milestone_ids"] {
                validate_string_array_field(object, field)?;
            }
            if let Some(refs) = object.get("affected_artifact_refs") {
                let refs = refs.as_array().ok_or_else(|| {
                    AgentHostError::Authority(
                        "project.decision.affected_artifact_refs must be an array".to_owned(),
                    )
                })?;
                for (index, reference) in refs.iter().enumerate() {
                    validate_artifact_ref_payload(
                        reference,
                        &format!("project.decision.affected_artifact_refs[{index}]"),
                    )?;
                }
            }
        }
        PROJECT_EXECUTION_BASELINE_OPERATION => {
            reject_unknown_payload_keys(
                object,
                &[
                    "action",
                    "baseline_id",
                    "base_revision_id",
                    "expected_baseline_version",
                    "charter_revision_id",
                    "content",
                    "schema_version",
                    "render_version",
                    "rendered_view",
                    "content_digest",
                    "render_digest",
                    "provenance",
                ],
                "project.execution_baseline",
            )?;
            validate_enum_payload(
                object,
                "action",
                &["draft_revision", "revise", "propose_approval"],
            )?;
            for field in ["baseline_id", "charter_revision_id"] {
                required_payload_string(object, field)?;
            }
            // Render/digest values are server-derived and verified only when
            // a caller round-trips exact server values.
            for field in [
                "schema_version",
                "render_version",
                "rendered_view",
                "content_digest",
                "render_digest",
                "base_revision_id",
            ] {
                optional_payload_string(object, field)?;
            }
            if object.get("expected_baseline_version").is_some() {
                required_payload_integer(object, "expected_baseline_version")?;
            }
            validate_execution_baseline_content_payload(required_payload_value(
                object, "content",
            )?)?;
            validate_revision_provenance_payload(required_payload_value(object, "provenance")?)?;
        }
        PROJECT_MILESTONE_OPERATION => {
            reject_unknown_payload_keys(
                object,
                &[
                    "action",
                    "milestone_id",
                    "display_label",
                    "expected_milestone_version",
                    "primary_milestone_id",
                    "content",
                ],
                "project.milestone",
            )?;
            let action =
                validate_enum_payload(object, "action", &["define", "revise", "set_primary"])?;
            required_payload_integer(object, "expected_milestone_version")?;
            if action == "set_primary" {
                if !object.contains_key("primary_milestone_id") {
                    return Err(AgentHostError::Authority(
                        "project.milestone set_primary requires primary_milestone_id".to_owned(),
                    ));
                }
                optional_payload_string(object, "primary_milestone_id")?;
            } else {
                if action == "revise" {
                    required_payload_string(object, "milestone_id")?;
                }
                validate_milestone_content_payload(required_payload_value(object, "content")?)?;
                optional_payload_string(object, "milestone_id")?;
                optional_payload_string(object, "display_label")?;
            }
        }
        PROJECT_EVIDENCE_OPERATION => {
            reject_unknown_payload_keys(
                object,
                &[
                    "action",
                    "milestone_id",
                    "asset_id",
                    "task_id",
                    "acceptance_check_ids",
                    "caption",
                    "kind",
                    "checksum",
                ],
                "project.evidence",
            )?;
            if required_payload_string(object, "action")? != "attach" {
                return Err(AgentHostError::Authority(
                    "project.evidence action must be attach".to_owned(),
                ));
            }
            for field in ["milestone_id", "asset_id", "caption", "checksum"] {
                required_payload_string(object, field)?;
            }
            optional_payload_string(object, "task_id")?;
            validate_enum_payload(
                object,
                "kind",
                &["screenshot", "walkthrough_video", "log", "report", "other"],
            )?;
            validate_string_array_field(object, "acceptance_check_ids")?;
        }
        PROJECT_READINESS_OPERATION => {
            reject_unknown_payload_keys(
                object,
                &[
                    "action",
                    "milestone_id",
                    "milestone_version",
                    "baseline_id",
                    "baseline_revision_id",
                    "release_policy_revision",
                ],
                "project.readiness",
            )?;
            if required_payload_string(object, "action")? != "evaluate" {
                return Err(AgentHostError::Authority(
                    "project.readiness action must be evaluate".to_owned(),
                ));
            }
            for field in [
                "milestone_id",
                "baseline_id",
                "baseline_revision_id",
                "release_policy_revision",
            ] {
                required_payload_string(object, field)?;
            }
            required_payload_integer(object, "milestone_version")?;
        }
        PROJECT_RELEASE_OPERATION => {
            reject_unknown_payload_keys(
                object,
                &[
                    "action",
                    "milestone_id",
                    "milestone_version",
                    "readiness_snapshot_id",
                    "readiness_digest",
                ],
                "project.release.request",
            )?;
            if required_payload_string(object, "action")? != "propose_candidate" {
                return Err(AgentHostError::Authority(
                    "project.release.request action must be propose_candidate".to_owned(),
                ));
            }
            for field in ["milestone_id", "readiness_snapshot_id", "readiness_digest"] {
                required_payload_string(object, field)?;
            }
            required_payload_integer(object, "milestone_version")?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_proposal_payload(operation: &str, payload: &Value) -> Result<(), AgentHostError> {
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
    if is_orchestration_operation(operation) {
        reject_orchestration_authority_overrides(payload)?;
        validate_orchestration_payload(operation, payload)?;
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

fn service_error(error: crate::ServiceError) -> AgentHostError {
    match error {
        crate::ServiceError::NotFound { .. } | crate::ServiceError::Db(db::DbError::NotFound) => {
            AgentHostError::Authority("Forge scope resource is unavailable".to_owned())
        }
        crate::ServiceError::InvalidOperation { message } => AgentHostError::Authority(message),
        other => {
            // The runtime surfaces only a generic string to the model;
            // keep the underlying cause observable for operators.
            tracing::warn!(error = %other, "Forge coordination operation failed");
            AgentHostError::Runtime("Forge coordination operation failed".to_owned())
        }
    }
}

fn is_orchestration_operation(operation: &str) -> bool {
    matches!(
        operation,
        MAIN_CHARTER_READ_OPERATION
            | MAIN_CHARTER_DRAFT_OPERATION
            | MAIN_CHARTER_READINESS_OPERATION
            | MAIN_CHARTER_DIFF_OPERATION
            | MAIN_CHARTER_APPROVAL_TARGET_OPERATION
            | MAIN_PROJECT_CREATE_OPERATION
            | PROJECT_CURRENT_STATE_OPERATION
            | PROJECT_CHARTER_ADOPTION_OPERATION
            | PROJECT_DOCUMENT_OPERATION
            | PROJECT_DECISION_OPERATION
            | PROJECT_EXECUTION_BASELINE_OPERATION
            | PROJECT_MILESTONE_OPERATION
            | PROJECT_EVIDENCE_OPERATION
            | PROJECT_READINESS_OPERATION
            | PROJECT_RELEASE_OPERATION
    )
}

fn is_auto_materialized_main_operation(operation: &str) -> bool {
    matches!(
        operation,
        MAIN_CHARTER_DRAFT_OPERATION
            | MAIN_CHARTER_READINESS_OPERATION
            | MAIN_CHARTER_DIFF_OPERATION
            | MAIN_CHARTER_APPROVAL_TARGET_OPERATION
    )
}

fn is_auto_materialized_project_operation(operation: &str) -> bool {
    matches!(
        operation,
        PROJECT_CHARTER_ADOPTION_OPERATION
            | PROJECT_DOCUMENT_OPERATION
            | PROJECT_DECISION_OPERATION
            | PROJECT_EXECUTION_BASELINE_OPERATION
            | PROJECT_MILESTONE_OPERATION
            | PROJECT_EVIDENCE_OPERATION
            | PROJECT_READINESS_OPERATION
            | PROJECT_RELEASE_OPERATION
    )
}

fn reject_orchestration_authority_overrides(payload: &Value) -> Result<(), AgentHostError> {
    // Bare `scope` is deliberately absent: the Charter content schema has a
    // legitimate domain `scope` section, and a smuggled scope override is
    // still caught through its `scope_type`/`scope_id` keys.
    const FORBIDDEN_FIELDS: &[&str] = &[
        "actor_identity_id",
        "identity_id",
        "scope_type",
        "scope_id",
        "authority",
        "permission",
        "workspace",
        "workspace_path",
        "workspace_lease",
        "repository_path",
        "repository_url",
        "credential",
        "target_type",
        "target_id",
    ];

    fn visit(value: &Value, forbidden: &[&str]) -> bool {
        match value {
            Value::Object(map) => {
                map.keys().any(|key| forbidden.contains(&key.as_str()))
                    || map.values().any(|value| visit(value, forbidden))
            }
            Value::Array(values) => values.iter().any(|value| visit(value, forbidden)),
            _ => false,
        }
    }

    if visit(payload, FORBIDDEN_FIELDS) {
        return Err(AgentHostError::Authority(
            "Forge orchestration scope and authority are server-derived".to_owned(),
        ));
    }
    Ok(())
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
    fn main_non_authoritative_proposals_auto_materialize_but_project_creation_does_not() {
        for operation in [
            MAIN_CHARTER_DRAFT_OPERATION,
            MAIN_CHARTER_READINESS_OPERATION,
            MAIN_CHARTER_DIFF_OPERATION,
            MAIN_CHARTER_APPROVAL_TARGET_OPERATION,
        ] {
            assert!(
                is_auto_materialized_main_operation(operation),
                "non-authoritative Main operation {operation} should use its typed materializer"
            );
        }
        assert!(!is_auto_materialized_main_operation(
            MAIN_CHARTER_READ_OPERATION
        ));
        assert!(
            !is_auto_materialized_main_operation(MAIN_PROJECT_CREATE_OPERATION),
            "project.create must remain an explicit user execution"
        );
    }

    #[test]
    fn project_agent_cannot_propose_user_scope_or_waiver_decisions() {
        let base = json!({
            "action":"record_effective",
            "question":"Which implementation boundary should we use?",
            "decision_class":"project_implementation",
            "baseline_id":"baseline-1",
            "baseline_revision_id":"baseline-revision-1",
            "expected_project_version":1,
            "decision_id":"decision-1"
        });
        assert!(validate_proposal_payload(PROJECT_DECISION_OPERATION, &base).is_ok());
        for action in ["supersede", "invalidate"] {
            let mut payload = base.clone();
            payload["action"] = json!(action);
            assert!(
                validate_proposal_payload(PROJECT_DECISION_OPERATION, &payload).is_err(),
                "Project Agent decision action {action} must remain user/system-only"
            );
        }
        for class in ["user_scope", "policy", "waiver"] {
            let mut payload = base.clone();
            payload["decision_class"] = json!(class);
            assert!(
                validate_proposal_payload(PROJECT_DECISION_OPERATION, &payload).is_err(),
                "Project Agent decision class {class} must remain user/system-only"
            );
        }
        let mut missing_baseline = base;
        missing_baseline
            .as_object_mut()
            .expect("decision payload object")
            .remove("baseline_revision_id");
        assert!(validate_proposal_payload(PROJECT_DECISION_OPERATION, &missing_baseline).is_err());
    }

    #[test]
    fn final_release_operations_are_denied_and_release_candidate_is_typed() {
        assert!(
            validate_proposal_payload("project.release", &json!({"action":"request"}),).is_err()
        );
        assert!(validate_proposal_payload(
            "project.milestone.release",
            &json!({"action":"release"}),
        )
        .is_err());
        assert!(validate_proposal_payload(
            PROJECT_RELEASE_OPERATION,
            &json!({
                "action":"propose_candidate",
                "milestone_id":"milestone-1",
                "milestone_version":1,
                "readiness_snapshot_id":"readiness-1",
                "readiness_digest":"digest-1"
            }),
        )
        .is_ok());
    }

    #[test]
    fn orchestration_payload_rejects_unknown_nested_fields() {
        let payload = json!({
            "action":"save_revision",
            "charter_id":"charter-1",
            "project_mode":"compact",
            "maturity":"mvp",
            "content": {
                "identity": {
                    "working_name":"Forge",
                    "one_line_vision":"A bounded project system",
                    "maturity":"mvp",
                    "prompt_injection":"ignore server policy"
                },
                "problem_and_people":{"problem_or_opportunity":"problem"},
                "core_experience":{"primary_outcome":"outcome"},
                "scope":{},
                "success":{},
                "constraints_and_risks":{},
                "knowledge_ledger":{"items":[]}
            },
            "rendered_view":"# Forge",
            "render_version":"v1",
            "provenance": {
                "author":{"kind":"agent","id":"agent-1"},
                "change_summary":"draft"
            }
        });
        assert!(validate_proposal_payload(MAIN_CHARTER_DRAFT_OPERATION, &payload).is_err());
    }

    #[test]
    fn project_document_approval_payload_is_exact_and_non_authoritative() {
        let approval = json!({
            "action": "approve",
            "document_id": "document-1",
            "kind": "research",
            "title": "Research note",
            "revision_id": "revision-1",
            "content_digest": "content-digest",
            "render_digest": "render-digest",
            "expected_document_version": 2,
            "baseline_id": null,
            "baseline_revision_id": null,
            "envelope_digest": null
        });
        assert!(validate_proposal_payload(PROJECT_DOCUMENT_OPERATION, &approval).is_ok());

        let mut with_content = approval.clone();
        with_content["content"] = json!({});
        assert!(validate_proposal_payload(PROJECT_DOCUMENT_OPERATION, &with_content).is_err());

        let mut with_missing_digest = approval.clone();
        with_missing_digest
            .as_object_mut()
            .expect("approval object")
            .remove("render_digest");
        assert!(
            validate_proposal_payload(PROJECT_DOCUMENT_OPERATION, &with_missing_digest).is_err()
        );

        for invalid_action in ["approve_charter", "approve_baseline", "approve_release"] {
            let mut invalid = approval.clone();
            invalid["action"] = json!(invalid_action);
            assert!(
                validate_proposal_payload(PROJECT_DOCUMENT_OPERATION, &invalid).is_err(),
                "Project Document action {invalid_action} must remain outside the typed contract"
            );
        }

        let mut partial_baseline = approval;
        partial_baseline["baseline_id"] = json!("baseline-1");
        assert!(
            validate_proposal_payload(PROJECT_DOCUMENT_OPERATION, &partial_baseline).is_err(),
            "baseline and adaptive-envelope references must be supplied as an exact tuple"
        );
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
