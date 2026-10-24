//! Native summary, memory, discovery, portfolio, and work projections.

use super::*;

impl CoordinationToolProvider {
    pub(super) async fn summary(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
    ) -> Result<Value, AgentHostError> {
        let row = match scope.scope_type {
            CanonicalScopeType::Account => {
                self.db
                    .native_account_scope_summary(actor_identity_id)
                    .await
            }
            CanonicalScopeType::Project => {
                self.db.native_project_scope_summary(&scope.scope_id).await
            }
            CanonicalScopeType::AgentChat => {
                self.db.native_chat_scope_summary(&scope.scope_id).await
            }
            CanonicalScopeType::Task => self.db.native_task_scope_summary(&scope.scope_id).await,
        }
        .map_err(|_| AgentHostError::ProtectedPersistence)?
        .ok_or_else(|| AgentHostError::Authority("current Forge scope is unavailable".into()))?;
        let mut result = serde_json::Map::new();
        for column in [
            db::NativeScopeSummaryField::Id,
            db::NativeScopeSummaryField::Name,
            db::NativeScopeSummaryField::Title,
            db::NativeScopeSummaryField::Status,
            db::NativeScopeSummaryField::Paused,
            db::NativeScopeSummaryField::Visibility,
            db::NativeScopeSummaryField::ScopeType,
            db::NativeScopeSummaryField::ScopeId,
            db::NativeScopeSummaryField::ProjectId,
            db::NativeScopeSummaryField::Priority,
        ] {
            if let Ok(value) = row.text(column) {
                result.insert(column.as_str().to_owned(), Value::String(value));
            } else if let Ok(value) = row.integer(column) {
                result.insert(column.as_str().to_owned(), Value::Number(value.into()));
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
                result.insert("available_actions_informational".to_owned(), json!(true));
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

    pub(super) async fn memory_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: operation_registry::task_reads::MemoryArguments,
        decision_only: bool,
    ) -> Result<Value, AgentHostError> {
        let query = arguments
            .query
            .as_deref()
            .unwrap_or_default()
            .chars()
            .take(512)
            .collect::<String>();
        let limit = arguments.limit.unwrap_or(10).clamp(1, 20) as u32;
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

    pub(super) async fn discovery_read(
        &self,
        _actor_identity_id: &str,
        scope: &CanonicalScope,
        input: operation_registry::main_reads::BoundedListQuery,
    ) -> Result<Value, AgentHostError> {
        let account_id = self
            .authorization
            .main_account_target(scope)
            .await
            .map_err(native_scope_error)?;
        let limit = input.limit.unwrap_or(10).clamp(1, 20) as i64;
        let rows = self
            .db
            .native_discovery_sessions(&account_id, limit)
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        Ok(json!({
            "items": rows.into_iter().map(|row| json!({
                "id": row.id().unwrap_or_default(),
                "maturity": row.maturity().unwrap_or_default(),
                "lifecycle": row.lifecycle().unwrap_or_default(),
                "project_id": row.project_id().ok().flatten(),
                "handoff_id": row.handoff_id().ok().flatten(),
                "version": row.version().unwrap_or_default(),
                "created_at": row.created_at().unwrap_or_default(),
                "updated_at": row.updated_at().unwrap_or_default(),
            })).collect::<Vec<_>>()
        }))
    }

    pub(super) async fn portfolio_read(
        &self,
        _actor_identity_id: &str,
        scope: &CanonicalScope,
        input: operation_registry::main_reads::BoundedListQuery,
    ) -> Result<Value, AgentHostError> {
        let account_id = self
            .authorization
            .main_account_target(scope)
            .await
            .map_err(native_scope_error)?;
        let limit = input.limit.unwrap_or(20).clamp(1, 20) as i64;
        let rows = self
            .db
            .native_portfolio_projects(&account_id, limit)
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        Ok(json!({
            "items": rows.into_iter().map(|row| json!({
                "id": row.id().unwrap_or_default(),
                "name": row.name().unwrap_or_default(),
                "paused": row.paused_at().ok().flatten().is_some(),
                "created_at": row.created_at().unwrap_or_default(),
                "updated_at": row.updated_at().unwrap_or_default(),
            })).collect::<Vec<_>>()
        }))
    }

    #[cfg(test)]
    pub(super) async fn read_work(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        limit: i64,
    ) -> Result<Value, AgentHostError> {
        self.read_work_for_admission(actor_identity_id, scope, limit, None)
            .await
    }
    pub(super) async fn read_work_for_admission(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        limit: i64,
        admitted: Option<&operation_registry::authority::EffectiveAuthority>,
    ) -> Result<Value, AgentHostError> {
        // A Project Agent works from its Chat scope, so resolve that binding to
        // the Project whose Tasks it proposes; reading them is how it avoids
        // proposing the same work twice.
        let project_scope_id = match scope.scope_type {
            CanonicalScopeType::Project => Some(scope.scope_id.clone()),
            CanonicalScopeType::AgentChat => Some(match admitted.map(|a| &a.principal) {
                Some(operation_registry::authority::Principal::ProjectAgent {
                    project_id, ..
                }) => project_id.clone(),
                _ => self
                    .authorization
                    .project_orchestration_target(actor_identity_id, scope)
                    .await
                    .map_err(native_scope_error)?,
            }),
            _ => None,
        };
        let rows = match (scope.scope_type, project_scope_id.as_deref()) {
            (CanonicalScopeType::Project | CanonicalScopeType::AgentChat, Some(project_id)) => self
                .db
                .native_project_work_tasks(project_id, limit)
                .await
                .map_err(|_| AgentHostError::ProtectedPersistence)?,
            (CanonicalScopeType::Task, _) => self
                .db
                .native_scope_work_task(&scope.scope_id)
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
            .filter_map(|row| row.id().ok())
            .collect::<Vec<_>>();
        // Dependencies are what distinguish a wired chain from stray Tasks, so
        // the reader that reconciles work has to see the edges, not just rows.
        let mut dependencies: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        if !ids.is_empty() {
            for row in self
                .db
                .native_work_task_dependencies(&ids)
                .await
                .map_err(|_| AgentHostError::ProtectedPersistence)?
            {
                let task_id: String = row.task_id().unwrap_or_default();
                let depends_on: String = row.depends_on_id().unwrap_or_default();
                dependencies.entry(task_id).or_default().push(depends_on);
            }
        }
        let mut latest_executions: std::collections::HashMap<String, Value> =
            std::collections::HashMap::new();
        if !ids.is_empty() {
            for row in self
                .db
                .native_work_latest_task_executions(&ids)
                .await
                .map_err(|_| AgentHostError::ProtectedPersistence)?
            {
                let task_id: String = row.task_id().unwrap_or_default();
                let execution_id: String = row.id().unwrap_or_default();
                let status: String = row.status().unwrap_or_default();
                let role: String = row.role_required().unwrap_or_default();
                let agent_session_id = row
                    .agent_session_id()
                    .ok()
                    .flatten()
                    .map(|value| truncate(&value, 256));
                let logs_path = row
                    .logs_path()
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
        let action_capable =
            admitted.map_or(scope.scope_type != CanonicalScopeType::Task, |authority| {
                authority
                    .evaluate(
                        operation_registry::PROPOSAL_CATALOG
                            .lookup(TASK_ACTION_OPERATION)
                            .unwrap(),
                    )
                    .is_ok()
            });
        let items = rows
            .into_iter()
            .map(|row| {
                let id = row.id().unwrap_or_default();
                let depends_on = dependencies.remove(&id).unwrap_or_default();
                let condition = row.condition_json().ok()
                    .map(|raw| db::task_condition::decode_or_unknown(&raw).public());
                json!({
                    "id": id,
                    "parent_task_id": row.parent_task_id().ok().flatten(),
                    "subtask_order": row.subtask_order().ok().flatten(),
                    "version": row.version().unwrap_or_default(),
                    "title": row.title().unwrap_or_default(),
                    "status": row.status().unwrap_or_default(),
                    "priority": row.priority().unwrap_or_default(),
                    "assignee_type": row.assignee_type().ok().flatten(),
                    "assignee_id": row.assignee_id().ok().flatten(),
                    "condition": condition,
                    "depends_on": depends_on,
                    "available_actions": offers_by_task.get(&id).cloned().unwrap_or_default(),
                    "available_actions_informational": !action_capable,
                    "action_schema": if !action_capable { Value::Null } else {
                        operation_registry::task_proposals::offered_action_schema(offers_by_task.get(&id).map(Vec::as_slice).unwrap_or_default(), false)
                    },
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

    pub(super) async fn read_events(
        &self,
        scope: &CanonicalScope,
        limit: i64,
    ) -> Result<Value, AgentHostError> {
        let rows = self
            .db
            .native_scope_events(scope_type_name(scope.scope_type), &scope.scope_id, limit)
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?;
        let items = rows
            .into_iter()
            .map(|row| {
                json!({
                    "sequence": row.sequence().unwrap_or_default(),
                    "id": row.id().unwrap_or_default(),
                    "event_type": row.event_type().unwrap_or_default(),
                    "entity_type": row.entity_type().unwrap_or_default(),
                    "entity_id": row.entity_id().unwrap_or_default(),
                    "actor_type": row.actor_type().unwrap_or_default(),
                    "correlation_id": row.correlation_id().unwrap_or_default(),
                    "causation_id": row.causation_id().ok().flatten(),
                    "causation_depth": row.causation_depth().unwrap_or_default(),
                    "created_at": row.created_at().unwrap_or_default(),
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({"items": items}))
    }

    pub(super) async fn read_inbox(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        limit: i64,
    ) -> Result<Value, AgentHostError> {
        let items = AgentInboxRepo::list_inbox_items(
            &*self.db,
            AgentInboxListQuery {
                visible_to_user_id: None,
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

    pub(super) async fn read_commitments(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        limit: i64,
    ) -> Result<Value, AgentHostError> {
        let items = AgentCommitmentRepo::list_commitments(
            &*self.db,
            AgentCommitmentListQuery {
                visible_to_user_id: None,
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

    pub(super) async fn read_delivery(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        limit: i64,
    ) -> Result<Value, AgentHostError> {
        let inbox = AgentInboxRepo::list_inbox_items(
            &*self.db,
            AgentInboxListQuery {
                visible_to_user_id: None,
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
                visible_to_user_id: None,
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
}
