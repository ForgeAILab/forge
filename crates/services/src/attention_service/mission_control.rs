//! Mission Control home, agent detail, and summary queries.

use super::*;

impl AttentionService {
    pub async fn mission_control_home(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        limit: i64,
    ) -> Result<MissionControlHomeResponse> {
        if let Some(project_id) = project_id {
            self.require_project_access(user_id, project_id).await?;
        }
        let limit = limit.clamp(1, 50);
        let (attention, health) = self
            .list_for_user(
                user_id,
                project_id,
                None,
                false,
                PageRequest {
                    cursor: None,
                    limit,
                    include_total: false,
                    sort_by: db::SortBy::Priority,
                    sort_order: db::SortOrder::Desc,
                },
            )
            .await?;
        let coordination_activity = self
            .coordination_activity(user_id, project_id, limit)
            .await?;
        let review_ready = self
            .work_items(user_id, project_id, &[], true, limit)
            .await?;
        let active_work = self
            .work_items(
                user_id,
                project_id,
                &["in_progress", "merging"],
                false,
                limit,
            )
            .await?;
        let agent_health = self.agent_health(user_id, project_id, limit).await?;
        let recent_outcomes = self.recent_outcomes(user_id, project_id, limit).await?;
        let capacity = self
            .capacity(
                user_id,
                project_id,
                !health
                    .as_ref()
                    .is_some_and(|health| health.stale || health.last_error_code.is_some()),
            )
            .await?;
        Ok(MissionControlHomeResponse {
            needs_attention: attention
                .items
                .into_iter()
                .map(attention_item)
                .collect::<Result<Vec<_>>>()?,
            coordination_activity,
            review_ready,
            active_work,
            agent_health,
            recent_outcomes,
            capacity,
            consumer_health: health,
            computed_at: now_rfc3339(),
        })
    }

    /// Project the two operator-visible coordination histories into one
    /// bounded feed. Direct command receipts are the committed history; only
    /// pending/approved approval actions are included from the action queue.
    /// The queries deliberately select digests and typed outcomes, never the
    /// action payload body. Visibility is enforced in SQL so an unauthorized
    /// project row cannot be loaded and filtered after the fact.
    async fn coordination_activity(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<MissionControlCoordinationActivity>> {
        let direct_rows = self
            .db
            .attention_direct_coordination_receipts(user_id, project_id, limit)
            .await?;

        let mut activities = direct_rows
            .into_iter()
            .map(|row| {
                let outcome_json: Option<String> = row.outcome_json?;
                Ok(MissionControlCoordinationActivity {
                    id: row.id?,
                    activity_kind: "direct_command".to_owned(),
                    actor_type: row.principal_type?,
                    actor_id: row.principal_id?,
                    scope_type: row.scope_type?,
                    scope_id: row.scope_id?,
                    operation: row.operation?,
                    input_digest: row.input_digest?,
                    policy_result: row.policy_result?,
                    status: "committed".to_owned(),
                    correlation_id: row.correlation_id?,
                    outcome: outcome_json.and_then(|value| serde_json::from_str(&value).ok()),
                    occurred_at: row.committed_at?,
                })
            })
            .collect::<std::result::Result<Vec<_>, db::DbError>>()
            .map_err(ServiceError::from)?;

        let action_rows = self
            .db
            .attention_approval_coordination_actions(user_id, project_id, limit)
            .await?;
        activities.extend(
            action_rows
                .into_iter()
                .map(|row| {
                    let outcome_json: Option<String> = row.outcome_json?;
                    Ok(MissionControlCoordinationActivity {
                        id: row.id?,
                        activity_kind: "approval_action".to_owned(),
                        actor_type: "agent".to_owned(),
                        actor_id: row.actor_identity_id?,
                        scope_type: row.scope_type?,
                        scope_id: row.scope_id?,
                        operation: row.operation?,
                        input_digest: row.payload_hash?,
                        policy_result: row.policy_result?,
                        status: row.status?,
                        correlation_id: row.correlation_id?,
                        outcome: outcome_json.and_then(|value| serde_json::from_str(&value).ok()),
                        occurred_at: row.updated_at?,
                    })
                })
                .collect::<std::result::Result<Vec<_>, db::DbError>>()?,
        );

        activities.sort_by(|left, right| {
            parse_rfc3339(&right.occurred_at)
                .cmp(&parse_rfc3339(&left.occurred_at))
                .then_with(|| right.occurred_at.cmp(&left.occurred_at))
                .then_with(|| right.id.cmp(&left.id))
        });
        activities.truncate(limit as usize);
        Ok(activities)
    }

    pub async fn agent_detail(
        &self,
        user_id: &str,
        identity_id: &str,
        limit: i64,
    ) -> Result<AgentDetailResponse> {
        let agent = AgentRepo::get_by_id(&*self.db, identity_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent", identity_id.to_owned()))?;
        self.require_agent_access(user_id, identity_id).await?;
        let bindings = self
            .db
            .visible_attention_agent_bindings(db::VisibleAttentionAgentBindingsInput {
                identity_id,
                account_id: user_id,
                member_id: user_id,
                project_identity_id: identity_id,
                owner_id: user_id,
                limit: limit.clamp(1, 50),
            })
            .await?
            .into_iter()
            .map(|row| {
                Ok(AgentBindingSummary {
                    binding_id: row.binding_id?,
                    binding_type: row.binding_type?,
                    project_id: row.project_id?,
                    chat_id: row.chat_id?,
                    state: row.state?,
                    subscription_count: subscription_count(&row.subscriptions_json?),
                    wake_budget: row.wake_budget?,
                })
            })
            .collect::<std::result::Result<Vec<_>, db::DbError>>()?;

        let mut scopes = Vec::new();
        for scope in AgentContextScopeRepo::list_context_scopes(&*self.db, identity_id).await? {
            if let Err(error) = self
                .require_scope_access(user_id, &scope.scope_type, &scope.scope_id)
                .await
            {
                if !is_visibility_miss(&error) {
                    return Err(error);
                }
                continue;
            }
            scopes.push(AgentScopeSummary {
                scope_type: scope.scope_type,
                scope_id: scope.scope_id,
                task_role: scope.task_role,
                workspace_access: scope.workspace_access,
                updated_at: scope.updated_at,
            });
            if scopes.len() >= limit.clamp(1, 50) as usize {
                break;
            }
        }

        let all_sessions = self
            .db
            .attention_agent_sessions(identity_id)
            .await?
            .into_iter()
            .map(|row| {
                Ok(AgentSessionSummary {
                    session_id: row.session_id?,
                    scope_type: row.scope_type?,
                    scope_id: row.scope_id?,
                    backend_kind: row.backend_kind?,
                    status: row.status?,
                    connection_status: row.connection_status?,
                    last_activity_at: row.last_activity_at?,
                    updated_at: row.updated_at?,
                })
            })
            .collect::<std::result::Result<Vec<_>, db::DbError>>()?;
        let mut sessions = Vec::new();
        for session in all_sessions {
            if let Err(error) = self
                .require_scope_access(user_id, &session.scope_type, &session.scope_id)
                .await
            {
                if !is_visibility_miss(&error) {
                    return Err(error);
                }
                continue;
            }
            sessions.push(session);
            if sessions.len() >= limit.clamp(1, 50) as usize {
                break;
            }
        }

        let current_focus = match self.focus_for_agent(identity_id).await? {
            Some(item)
                if self
                    .require_project_access(user_id, &item.project_id)
                    .await
                    .is_ok() =>
            {
                Some(mission_work_item(item))
            }
            _ => None,
        };
        let mut checkpoint_present = false;
        for session in &sessions {
            let has_checkpoint = self
                .db
                .session_persistence_flags(&session.session_id)
                .await?
                .checkpoint_present;
            checkpoint_present |= has_checkpoint;
        }
        let continuity_status = if sessions
            .iter()
            .any(|session| session.status == "failed" || session.connection_status == "unavailable")
        {
            "degraded"
        } else if checkpoint_present || !sessions.is_empty() {
            "healthy"
        } else {
            "unknown"
        };
        let last_activity_at = sessions
            .iter()
            .find_map(|session| session.last_activity_at.clone());

        Ok(AgentDetailResponse {
            identity_id: agent.id,
            name: agent.name,
            description: agent.description,
            backend_kind: Some(agent.backend_kind),
            provider: agent.provider,
            model: agent.model,
            identity_status: agent.status.to_string(),
            paused: agent.paused,
            bindings,
            scopes,
            sessions,
            current_focus,
            open_commitment_count: self
                .visible_coordination_count(
                    "agent_commitment",
                    "owner_identity_id",
                    &["proposed", "open", "accepted", "in_progress", "blocked"],
                    user_id,
                    identity_id,
                )
                .await?,
            open_inbox_count: self
                .visible_coordination_count(
                    "agent_inbox_item",
                    "recipient_identity_id",
                    &["unread", "read", "acknowledged"],
                    user_id,
                    identity_id,
                )
                .await?,
            memory_namespace_count: self
                .visible_memory_namespace_count(user_id, identity_id)
                .await?,
            usage: self.agent_usage_summary(identity_id).await?,
            continuity: AgentContinuityHealth {
                status: continuity_status.to_owned(),
                checkpoint_present,
                last_activity_at,
            },
        })
    }

    pub async fn consumer_health(&self) -> Result<Option<AttentionConsumerHealthResponse>> {
        let row = self.db.attention_worker_health(CONSUMER_NAME).await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let lag = self.db.domain_event_consumer_lag(&[CONSUMER_NAME]).await?;
        let stale = lag
            .first()
            .is_some_and(|lag| lag.stalled(Utc::now(), CONSUMER_STALE_SECONDS));
        let mut last_error_code: Option<String> = row.last_error_kind?;
        let mut last_error_message: Option<String> = row.last_error?;
        if last_error_message.is_none() {
            let recent: Option<(String, String)> = self
                .db
                .recent_attention_worker_error(
                    CONSUMER_NAME,
                    (Utc::now() - Duration::hours(1)).to_rfc3339(),
                )
                .await?;
            if let Some((kind, message)) = recent {
                last_error_code = Some(kind);
                last_error_message = Some(message);
            }
        }
        Ok(Some(AttentionConsumerHealthResponse {
            consumer_name: CONSUMER_NAME.to_owned(),
            last_sequence: row.last_sequence?,
            last_success_at: row.last_success_at?,
            last_error_code,
            last_error_message,
            stale,
            updated_at: row.updated_at?,
        }))
    }

    async fn work_items(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        statuses: &[&str],
        review: bool,
        limit: i64,
    ) -> Result<Vec<MissionControlWorkItem>> {
        let mut review_states = Vec::new();
        if review {
            for (project, raw) in self
                .db
                .attention_visible_project_workflows(user_id, project_id)
                .await?
            {
                let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow(&raw);
                review_states.extend(
                    workflow
                        .states
                        .iter()
                        .filter(|state| db::budget::is_review_state(state))
                        .map(|state| (project.clone(), state.name.clone())),
                );
            }
        }
        let rows = self
            .db
            .attention_visible_work_items(user_id, project_id, statuses, &review_states, limit)
            .await?;
        rows.into_iter()
            .map(|row| {
                Ok(MissionControlWorkItem {
                    task_id: row.id?,
                    project_id: row.project_id?,
                    title: bounded_text(row.title?),
                    status: row.status?,
                    priority: row.priority?,
                    updated_at: row.updated_at?,
                    primary_action: if review {
                        "review".to_owned()
                    } else {
                        "inspect".to_owned()
                    },
                })
            })
            .collect::<std::result::Result<Vec<_>, db::DbError>>()
            .map_err(ServiceError::from)
    }

    async fn agent_health(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<MissionControlAgentHealth>> {
        let rows = self
            .db
            .attention_visible_agent_health(user_id, project_id, limit)
            .await?;
        rows.into_iter()
            .map(|row| {
                Ok(MissionControlAgentHealth {
                    identity_id: row.id?,
                    name: bounded_text(row.name?),
                    backend_kind: row.backend_kind?,
                    provider: row.provider?,
                    model: row.model?,
                    identity_status: row.status?,
                    paused: row.paused? != 0,
                    connection_status: row.connection_status?,
                    last_activity_at: row.last_heartbeat_at?,
                    active_session_count: row.active_session_count?,
                    project_count: row.project_count?,
                })
            })
            .collect::<std::result::Result<Vec<_>, db::DbError>>()
            .map_err(ServiceError::from)
    }

    async fn recent_outcomes(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<MissionControlRecentOutcome>> {
        let rows = self
            .db
            .attention_visible_recent_outcomes(user_id, project_id, limit)
            .await?;
        rows.into_iter()
            .map(|row| {
                Ok(MissionControlRecentOutcome {
                    task_id: row.id?,
                    project_id: row.project_id?,
                    title: bounded_text(row.title?),
                    outcome: row.status?,
                    occurred_at: row.updated_at?,
                })
            })
            .collect::<std::result::Result<Vec<_>, db::DbError>>()
            .map_err(ServiceError::from)
    }

    async fn capacity(
        &self,
        user_id: &str,
        project_id: Option<&str>,
        healthy: bool,
    ) -> Result<MissionControlCapacity> {
        let active = self
            .db
            .attention_visible_active_execution_count(user_id, project_id)
            .await?
            .and_then(|row| row.count.ok())
            .unwrap_or(0);

        let queued = self
            .db
            .attention_visible_queued_task_count(user_id, project_id)
            .await?
            .count?;
        let active_sessions = self
            .db
            .attention_visible_active_session_count(user_id, project_id)
            .await?;
        Ok(MissionControlCapacity {
            active_executions: active,
            queued_tasks: queued,
            active_sessions,
            healthy,
        })
    }

    async fn focus_for_agent(&self, identity_id: &str) -> Result<Option<db::Task>> {
        // A Project/Main binding owns the obligation, not a Task Worker
        // assignment.  Keep the worker path for identities assigned directly
        // to a Task, then include the current Task attached to an unfinished
        // commitment owned by this identity.  The identity predicate is
        // deliberately applied in both branches so replacing a binding never
        // leaks the previous owner's focus into the replacement detail.
        let task_id = self
            .db
            .attention_agent_focus_task_id(identity_id, identity_id)
            .await?;
        match task_id {
            Some(task_id) => Ok(db::TaskRepo::get_by_id(&*self.db, &task_id, false).await?),
            None => Ok(None),
        }
    }

    /// Count only immutable memory bindings whose canonical scope is visible
    /// to the requesting user.  The query never reads memory bodies or FTS
    /// rows, and inaccessible scope existence is discarded before counting.
    async fn visible_memory_namespace_count(
        &self,
        user_id: &str,
        identity_id: &str,
    ) -> Result<i64> {
        let rows = self
            .db
            .attention_identity_memory_scopes(identity_id)
            .await?;
        let mut visible = BTreeSet::new();
        for row in rows {
            let scope_type: String = row.scope_type?;
            let scope_id: String = row.scope_id?;
            match self
                .require_scope_access(user_id, &scope_type, &scope_id)
                .await
            {
                Ok(()) => {
                    visible.insert((scope_type, scope_id));
                }
                Err(error) if is_visibility_miss(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(visible.len() as i64)
    }

    async fn agent_usage_summary(&self, identity_id: &str) -> Result<UsageAggregate> {
        crate::usage_projection::usage_aggregate_for_agent(&self.db, identity_id).await
    }

    async fn visible_coordination_count(
        &self,
        table: &str,
        identity_column: &str,
        statuses: &[&str],
        user_id: &str,
        identity_id: &str,
    ) -> Result<i64> {
        let (table, identity_column) = match (table, identity_column) {
            ("agent_commitment", "owner_identity_id") => (table, identity_column),
            ("agent_inbox_item", "recipient_identity_id") => (table, identity_column),
            _ => return Ok(0),
        };
        let exists = self.db.attention_coordination_table_count(table).await?;
        if exists == 0 {
            return Ok(0);
        }
        let rows = self
            .db
            .attention_coordination_scopes(table, identity_column, statuses, identity_id)
            .await?;
        let mut visible = 0;
        for row in rows {
            let scope_type: String = row.scope_type?;
            let scope_id: String = row.scope_id?;
            match self
                .require_scope_access(user_id, &scope_type, &scope_id)
                .await
            {
                Ok(()) => visible += 1,
                Err(error) if is_visibility_miss(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(visible)
    }
}
