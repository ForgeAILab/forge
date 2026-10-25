//! Attention event preparation, projection commits, and scope resolution.

use super::*;

impl AttentionService {
    /// A Task entering `review` is only attention when a person must decide
    /// the gate. A review run by the workflow's reviewer Agent settles itself;
    /// waking the Project Agent for it sends the Agent to a `task.action`
    /// action the gate rejects, which wastes the turn and reports a blocker
    /// that does not exist. A Task that no longer exists is nobody's review.
    async fn review_needs_a_person(&self, event: &DomainEvent) -> Result<bool> {
        if event.entity_type != "task" {
            return Ok(true);
        }
        let Some(task) = db::TaskRepo::get_by_id(&*self.db, &event.entity_id, false).await? else {
            return Ok(false);
        };
        let Some(project) = ProjectRepo::get_by_id(&*self.db, &task.project_id).await? else {
            return Ok(false);
        };
        let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
            &task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        Ok(crate::task_service::task_review_requires_user_decision(
            &task, &workflow,
        ))
    }

    pub(super) async fn resolve_superseded_turn_incidents(&self) -> Result<()> {
        let rows: Vec<db::FailedTurnIncidentRow> =
            self.db.unresolved_failed_turn_incidents().await?;
        for db::FailedTurnIncidentRow {
            dedupe_key: key,
            entity_id: turn_id,
            id: event_id,
            payload_json: payload,
        } in rows
        {
            if let Some(turn) =
                db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, &turn_id).await?
            {
                let version = serde_json::from_str::<Value>(&payload)
                    .ok()
                    .and_then(|value| value.get("version").and_then(Value::as_i64));
                if turn.retry_action().is_none() || version.is_some_and(|v| v != turn.version) {
                    AttentionRepo::resolve_attention_by_dedupe(
                        &*self.db,
                        &key,
                        &event_id,
                        &now_rfc3339(),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    pub(super) async fn prepare_projection(
        &self,
        event: &DomainEvent,
    ) -> Result<Outcome<PreparedAttention>> {
        let transition_attention = if event.event_type.eq_ignore_ascii_case("task.transitioned") {
            Some(task_transition_attention(event))
        } else {
            None
        };
        let category = match transition_attention {
            Some(transition) => transition.category,
            None => match self.event_category(event).await? {
                EventCategoryDecision::Ready(category) => category,
                EventCategoryDecision::Deferred => {
                    return Ok(Outcome::Defer {
                        after: StdDuration::from_secs(1),
                        reason: "terminal Task disposition has not settled".to_owned(),
                    })
                }
            },
        };
        let mut incident = None;
        if let Some(category) = category {
            let review_is_current =
                category != "review_ready" || self.review_needs_a_person(event).await?;
            let (scope_type, scope_id) = self.event_scope(event).await?;
            // Attention's historical materialization table accepts the
            // account/project scope vocabulary, while Agent Chat events use
            // the chat as their canonical wake scope.  Keep the raw chat
            // scope in the incident key and wake payload (so delivery can
            // route to that chat), but materialize the Attention row under
            // its owning account/project scope.
            let (attention_scope_type, attention_scope_id) = self
                .attention_projection_scope(&scope_type, &scope_id)
                .await?;
            let identity_id = self.wake_identity_for_event(event).await?;
            let incident_key = attention_incident_key(category, event, &scope_type, &scope_id);
            let (priority, summary, recommended_action) = category_metadata(category);
            let failed_turn = if event.event_type == "agent_chat.turn.failed" {
                db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, &event.entity_id)
                    .await?
            } else {
                None
            };
            // Re-drive or completion may precede this consumer's projection.
            if failed_turn.as_ref().is_some_and(|turn| {
                turn.retry_action().is_none()
                    || serde_json::from_str::<Value>(&event.payload_json)
                        .ok()
                        .and_then(|value| value.get("version").and_then(Value::as_i64))
                        .is_some_and(|v| v != turn.version)
            }) {
                return Ok(Outcome::Skip);
            }
            let retry_action = failed_turn
                .as_ref()
                .and_then(db::AgentChatTurnJob::retry_action);
            let cause = failed_turn
                .as_ref()
                .and_then(|turn| turn.failure_class.as_ref());
            let summary = if category == "review_needs_owner" {
                let reason = event
                    .task_material_blocker()
                    .and_then(|m| m.interruption)
                    .and_then(|i| i.get("reason").and_then(Value::as_str).map(str::to_owned));
                match reason.filter(|reason| !reason.trim().is_empty()) {
                    Some(reason) => format!("Review needs the owner: {reason}"),
                    None => summary.to_owned(),
                }
            } else if category == "conflict_hotspot" {
                let payload =
                    serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
                format!(
                    "{} conflicted in {} Tasks this week",
                    payload
                        .get("path")
                        .and_then(Value::as_str)
                        .unwrap_or("Shared file"),
                    payload
                        .get("handoff_count")
                        .and_then(Value::as_u64)
                        .unwrap_or_default()
                )
            } else if retry_action.is_some() {
                format!(
                    "Agent Chat turn failed: {}",
                    cause
                        .map(api_types::TurnFailure::code)
                        .unwrap_or("backend_failed")
                )
            } else {
                summary.to_owned()
            };
            let recommended_action = if retry_action.is_some() {
                "retry_turn"
            } else {
                recommended_action
            };
            let task_context = if event.entity_type == "task" {
                self.db
                    .attention_task_context(&event.entity_id)
                    .await?
                    .map(|row| {
                        json!({
                            "task_title": bounded_text(row.title.unwrap_or_default()),
                            "task_status": row.status.unwrap_or_default(),
                            "task_version": row.version.unwrap_or_default(),
                        })
                    })
            } else {
                None
            };
            let event_payload =
                serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
            // What the user decided, so the wake prompt can say it back to
            // the Agent instead of making it rediscover the decision.
            let decision_context = user_decision_outcome(event).map(|outcome| {
                let payload_text = |key: &str| {
                    event_payload
                        .get(key)
                        .and_then(Value::as_str)
                        .map(|value| bounded_text(value.to_owned()))
                };
                json!({
                    "outcome": outcome,
                    "target_type": event.entity_type,
                    "target_id": event.entity_id,
                    "revision_id": payload_text("revision_id"),
                    "lifecycle": payload_text("lifecycle"),
                    "event_type": event.event_type,
                    "decided_by": "user",
                })
            });
            let interruption = event
                .task_material_blocker()
                .and_then(|m| m.interruption)
                .unwrap_or(Value::Null);
            let available_actions = if event.entity_type == "task" {
                if let Some(task) =
                    db::TaskRepo::get_by_id(&*self.db, &event.entity_id, false).await?
                {
                    if let Some(project) =
                        db::ProjectRepo::get_by_id(&*self.db, &task.project_id).await?
                    {
                        let actor = if category == "review_needs_owner" {
                            api_types::Actor::user(api_types::UserActionSource::Api)
                        } else {
                            match db::ProjectAgentBindingRepo::get_active_project_binding(
                                &*self.db,
                                &task.project_id,
                            )
                            .await?
                            .filter(|binding| binding.state == "active")
                            .and_then(|binding| binding.identity_id)
                            {
                                Some(agent_id) => api_types::Actor::agent(agent_id),
                                None => {
                                    api_types::Actor::system(api_types::SystemComponent::Workflow)
                                }
                            }
                        };
                        let workflow =
                            crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
                                &task,
                                &project.workflow_definition,
                                &actor,
                            );
                        let snapshot = crate::task_actions::load_snapshot(
                            &self.db,
                            task,
                            workflow,
                            &actor,
                            self.action_connections.as_deref(),
                        )
                        .await?;
                        crate::available_actions(&snapshot)
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };
            let requires_intervention = event
                .task_material_blocker()
                .map(|m| m.requires_intervention)
                .unwrap_or(category == "execution_failed");
            let mut details = json!({
                "source_event_id": event.id,
                "source_event_type": event.event_type,
                "source_sequence": event.sequence,
                "entity_type": event.entity_type,
                "entity_id": event.entity_id,
                "scope_type": scope_type,
                "scope_id": scope_id,
                "task": task_context,
                "failure_class": cause,
                "retry_decision": failed_turn.as_ref().and_then(|turn| turn.retry_decision.as_ref()),
                "retry_action": retry_action,
                "decision": decision_context,
                "role": event_payload.get("role").and_then(Value::as_str).map(|value| bounded_text(value.to_owned())),
                "stop_reason": event_payload.get("stop_reason").and_then(Value::as_str).map(|value| bounded_text(value.to_owned())),
                "error": event_payload.get("error").and_then(Value::as_str).map(|value| bounded_text(value.to_owned())),
                "interruption": interruption,
                "recovery": {
                    "requires_intervention": requires_intervention,
                    "actions": available_actions,
                    "automatic_retry": false,
                },
            });
            if event.event_type == "project.escalation.answered" {
                details["answer"] = event_payload.get("answer").cloned().unwrap_or(Value::Null);
                details["task_ids"] = event_payload
                    .get("task_ids")
                    .cloned()
                    .unwrap_or(Value::Null);
            }
            if category == "conflict_hotspot" {
                details["conflict_hotspot"] = event_payload.clone();
            }
            let details_json = serde_json::to_string(&details)
                .map_err(|error| ServiceError::Domain(error.to_string()))?;
            let projection = CreateAttentionProjection {
                id: new_uuid_v4(),
                attention_type: category.to_owned(),
                scope_type: attention_scope_type,
                scope_id: attention_scope_id,
                identity_id: identity_id.clone(),
                source_event_id: event.id.clone(),
                priority,
                status: "open".to_owned(),
                summary: bounded_summary(&summary),
                details_json,
                dedupe_key: incident_key.clone(),
                occurred_at: event.created_at.clone(),
                updated_at: now_rfc3339(),
                acknowledged_at: None,
                snoozed_until: None,
                resolved_at: None,
                updated_by_user_id: None,
                recommended_action: recommended_action.to_owned(),
                source_sequence: Some(event.sequence),
            };
            let request = WakeAdmissionRequest {
                identity_id: identity_id.clone().unwrap_or_default(),
                scope_type: scope_type.clone(),
                scope_id: scope_id.clone(),
                incident_key: incident_key.clone(),
                lease_owner: new_uuid_v4(),
                correlation_id: event.correlation_id.clone(),
                causation_id: Some(event.id.clone()),
                caused_by_identity_id: (event.actor_type == "agent")
                    .then(|| event.actor_id.clone())
                    .flatten(),
                reaction_depth: event.causation_depth,
                now: now_rfc3339(),
                lease_seconds: WAKE_LEASE_SECONDS,
                cooldown_seconds: WAKE_COOLDOWN_SECONDS,
            };
            // Policy terminal states are checked before responder setup.  In
            // particular, a retry-exhausted Agent Chat event must be recorded
            // as recursive suppression even when its binding has already
            // disappeared; setup-required would otherwise hide a recursion
            // decision behind a missing responder.
            let decision = if category == "review_needs_owner" {
                PreparedWakeDecision::OwnerOnly
            } else if let Some(reason) = wake_policy_suppression_reason(&request) {
                PreparedWakeDecision::Suppressed(reason)
            } else {
                let configured = self
                    .wake_responder_is_configured(&scope_type, &scope_id, None)
                    .await?;
                let eligible = match identity_id.as_deref() {
                    Some(id) => {
                        self.wake_responder_is_configured(&scope_type, &scope_id, Some(id))
                            .await?
                            && self
                                .wake_identity_is_eligible(id, &scope_type, &scope_id)
                                .await?
                    }
                    None => false,
                };
                match identity_id {
                    Some(_) if configured && eligible => PreparedWakeDecision::Admit,
                    Some(_) if configured => {
                        PreparedWakeDecision::Suppressed(WakeSuppressionReason::IneligibleScope)
                    }
                    _ => PreparedWakeDecision::SetupRequired,
                }
            };
            let turn = if matches!(decision, PreparedWakeDecision::Admit)
                && !(projection.scope_type == "project" && blocker_category(category))
            {
                let candidate = projection_snapshot(&projection);
                let context = WakeDecisionContext {
                    source_event_id: Some(event.id.clone()),
                    incident_digest: Some(wake_attention_incident_digest(&candidate)),
                    ..Default::default()
                };
                self.wake_turns
                    .prepare_attention(
                        &candidate,
                        &wake_admitted_dedupe_key(&request, &context),
                        request.causation_id.as_deref(),
                        request.reaction_depth,
                        &[],
                    )
                    .await?
                    .map(Arc::new)
            } else {
                None
            };
            incident = Some(PreparedAttentionIncident {
                projection,
                turn,
                request,
                decision,
                failed_turn,
                review_is_current,
                requires_intervention,
                orphan_execution_id: (category == "execution_failed"
                    && matches!(
                        event.event_type.as_str(),
                        "execution.failed" | "execution.cancelled"
                    ))
                .then(|| {
                    event_payload
                        .get("execution_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .flatten(),
            });
        }

        let mut resolved_categories = resolution_categories(event);
        if transition_attention.is_some_and(|transition| transition.terminal) {
            resolved_categories.extend([
                "validation_failed",
                "run_stalled",
                "progress_warning",
                "retry_exhausted",
                "review_ready",
                "review_risk",
                "review_needs_owner",
                "execution_failed",
            ]);
        }
        let mut resolutions = Vec::new();
        for category in resolved_categories {
            let (scope_type, scope_id) = self.event_scope(event).await?;
            resolutions.push(attention_incident_key(
                category,
                event,
                &scope_type,
                &scope_id,
            ));
        }
        let followup_scope = if event.event_type == "milestone.readiness.evaluated" {
            let (scope_type, scope_id) = self.event_scope(event).await?;
            Some(
                self.attention_projection_scope(&scope_type, &scope_id)
                    .await?,
            )
        } else {
            None
        };
        if incident.is_none() && resolutions.is_empty() && followup_scope.is_none() {
            return Ok(Outcome::Skip);
        }
        Ok(Outcome::Done(PreparedAttention {
            incident,
            resolutions,
            followup_scope,
        }))
    }

    pub(super) async fn commit_projection(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        event: &DomainEvent,
        prepared: &PreparedAttention,
    ) -> Result<Option<(String, String)>> {
        let mut stall = None;
        if let Some(p) = &prepared.incident {
            let admitted: i64 = self
                .db
                .source_event_has_admitted_wake_in_tx(tx, &event.id)
                .await?;
            if admitted != 0 {
                return Ok(None);
            }
            let attention = self
                .db
                .insert_attention_in_tx(tx, p.projection.clone())
                .await?;
            let stale = if let Some(turn) = &p.failed_turn {
                self.db
                    .get_agent_chat_turn_job_in_tx(tx, &turn.id)
                    .await?
                    .is_some_and(|current| {
                        current.retry_action().is_none() || current.version != turn.version
                    })
            } else {
                false
            };
            if stale || !p.review_is_current {
                self.db
                    .resolve_attention_by_dedupe_in_tx(
                        tx,
                        &p.projection.dedupe_key,
                        &event.id,
                        &now_rfc3339(),
                    )
                    .await?;
                return Ok(None);
            }
            let context = WakeDecisionContext {
                attention_id: Some(attention.id.clone()),
                source_event_id: Some(event.id.clone()),
                incident_digest: Some(wake_attention_incident_digest(&attention)),
                attention_status: Some(attention.status.clone()),
                attention_version: Some(attention.version),
                task_id: (event.event_type == "task.interruption_changed")
                    .then(|| event.entity_id.clone()),
                requires_current_task_intervention: event.event_type == "task.interruption_changed"
                    && p.requires_intervention,
                orphan_execution_id: p.orphan_execution_id.clone(),
                turn: p.turn.clone(),
                batch: vec![attention.clone()],
            };
            match &p.decision {
                PreparedWakeDecision::OwnerOnly => {}
                PreparedWakeDecision::Admit
                    if attention.scope_type == "project"
                        && blocker_category(&attention.attention_type) =>
                {
                    // A single Project wake is assembled after the projection window.
                }
                PreparedWakeDecision::Admit => {
                    let committed = self.admit_wake_in_tx(tx, &p.request, &context).await?;
                    stall = committed.stall_scope;
                }
                PreparedWakeDecision::Suppressed(reason) => {
                    self.append_wake_decision_in_tx(
                        tx,
                        &p.request,
                        &context,
                        WakeDecisionEvent::Suppressed(reason.clone()),
                        None,
                        None,
                        &p.request.now,
                    )
                    .await?;
                }
                PreparedWakeDecision::SetupRequired => {
                    self.append_wake_decision_in_tx(
                        tx,
                        &p.request,
                        &context,
                        WakeDecisionEvent::SetupRequired(WakeSetupReason::ResponderBindingMissing),
                        None,
                        None,
                        &p.request.now,
                    )
                    .await?;
                }
            }
        }
        for key in &prepared.resolutions {
            self.db
                .resolve_attention_by_dedupe_in_tx(tx, key, &event.id, &now_rfc3339())
                .await?;
        }
        if let Some((scope_type, scope_id)) = &prepared.followup_scope {
            let at = now_rfc3339();
            self.db
                .resolve_scope_progress_attention_in_tx(
                    tx,
                    db::ResolveScopeProgressAttentionInTxInput {
                        resolved_at: &at,
                        source_event_id: &event.id,
                        updated_at: &at,
                        scope_type,
                        scope_id,
                    },
                )
                .await?;
        }
        Ok(stall)
    }

    /// Decide whether a durable event represents an actionable incident.
    /// Raw terminal execution events are audit facts, not recovery commands:
    /// the atomic `task.interruption_changed` event normally owns Attention.
    /// A manual terminal attempt that never receives any Task disposition is
    /// retained as a bounded-grace orphan safety net.
    async fn event_category(&self, event: &DomainEvent) -> Result<EventCategoryDecision> {
        if event.event_type == "project.escalation.answered" {
            return Ok(EventCategoryDecision::Ready(Some(
                DECISION_RECORDED_CATEGORY,
            )));
        }
        if event
            .event_type
            .eq_ignore_ascii_case("task.interruption_changed")
            && classify_event(event).is_some()
        {
            let current = db::TaskRepo::get_by_id(&*self.db, &event.entity_id, false).await?;
            if !current.as_ref().is_some_and(task_requires_intervention) {
                return Ok(EventCategoryDecision::Ready(None));
            }
        }
        if !matches!(
            event.event_type.to_ascii_lowercase().as_str(),
            "execution.failed" | "execution.cancelled"
        ) {
            return Ok(EventCategoryDecision::Ready(classify_event(event)));
        }

        let payload = serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
        let Some(execution_id) = payload.get("execution_id").and_then(Value::as_str) else {
            return Ok(EventCategoryDecision::Ready(None));
        };
        let Some(execution) = db::ExecutionRepo::get_by_id(&*self.db, execution_id).await? else {
            return Ok(EventCategoryDecision::Ready(None));
        };
        if matches!(
            execution.resume_policy,
            Some(db::ResumePolicy::Auto | db::ResumePolicy::None)
        ) || matches!(
            execution.stop_reason,
            Some(db::StopReason::UserCancelled | db::StopReason::RoleReassigned)
        ) {
            return Ok(EventCategoryDecision::Ready(None));
        }
        // Recovery leaves the stopped attempt immutable. Its event may be
        // replayed after a successor has started or even finished, so the old
        // attempt's Manual policy alone cannot establish an orphan.
        let mut transaction = self.db.pool().begin().await?;
        let current = orphan_attempt_is_current(&self.db, &mut transaction, &execution.id).await?;
        transaction.commit().await?;
        if !current {
            return Ok(EventCategoryDecision::Ready(None));
        }

        let settled_before = Utc::now() - Duration::seconds(TERMINAL_DISPOSITION_GRACE_SECONDS);
        let terminal_at = parse_rfc3339(&event.created_at).unwrap_or(settled_before);
        if terminal_at > settled_before {
            return Ok(EventCategoryDecision::Deferred);
        }

        tracing::warn!(
            task_id = %execution.task_id,
            execution_id = %execution.id,
            event_id = %event.id,
            "terminal execution remained manually resumable without a Task disposition"
        );
        Ok(EventCategoryDecision::Ready(Some("execution_failed")))
    }

    async fn attention_projection_scope(
        &self,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<(String, String)> {
        if scope_type != "agent_chat" {
            return Ok((scope_type.to_owned(), scope_id.to_owned()));
        }
        let Some(row) = self.db.attention_chat_scope(scope_id).await? else {
            // Keep the raw scope for the existing projection error path when
            // an event references a chat that no longer exists.  Valid Chat
            // events always resolve to one of the accepted material scopes.
            return Ok((scope_type.to_owned(), scope_id.to_owned()));
        };
        let kind: String = row.kind?;
        match kind.as_str() {
            "account_main" => row
                .account_id
                .map(|account_id| ("account".to_owned(), account_id.unwrap_or_default()))
                .map_err(ServiceError::from),
            "project" => row
                .project_id
                .map(|project_id| ("project".to_owned(), project_id.unwrap_or_default()))
                .map_err(ServiceError::from),
            _ => Ok((scope_type.to_owned(), scope_id.to_owned())),
        }
    }

    pub(super) async fn event_scope(&self, event: &DomainEvent) -> Result<(String, String)> {
        if event.event_type == "task.transitioned" {
            if let Ok(payload) =
                serde_json::from_str::<api_types::TaskTransitionEventPayload>(&event.payload_json)
            {
                if payload.known_workflow_snapshot().is_some() {
                    return Ok(("project".to_owned(), payload.project_id));
                }
            }
        }
        if event.scope_type == "task" || event.entity_type == "task" {
            if let Some(project_id) = self
                .db
                .attention_event_task_project_id(&event.entity_id)
                .await?
            {
                return Ok(("project".to_owned(), project_id));
            }
        }
        let is_agent_chat_event = event.scope_type == "agent_chat"
            || event.entity_type == "agent_chat"
            || event.entity_type == "agent_chat_turn_job";
        if is_agent_chat_event && self.db.attention_scope_chat_count(&event.scope_id).await? > 0 {
            return Ok(("agent_chat".to_owned(), event.scope_id.clone()));
        }
        Ok((event.scope_type.clone(), event.scope_id.clone()))
    }

    pub(super) async fn event_identity(&self, event: &DomainEvent) -> Result<Option<String>> {
        if event.actor_type != "agent" {
            return Ok(None);
        }
        let Some(actor_id) = event.actor_id.as_deref() else {
            return Ok(None);
        };
        let exists = self.db.attention_actor_identity_count(actor_id).await? > 0;
        Ok(exists.then(|| actor_id.to_owned()))
    }

    pub(super) async fn consumer_cursor(&self) -> Result<Option<EventConsumerCursor>> {
        Ok(DomainEventRepo::get_consumer_cursor(&*self.db, CONSUMER_NAME).await?)
    }
}
