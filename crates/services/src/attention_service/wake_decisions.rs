//! Wake decision event persistence and budget lookup.

use super::*;

impl AttentionService {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn append_wake_decision_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        request: &WakeAdmissionRequest,
        context: &WakeDecisionContext,
        decision: WakeDecisionEvent,
        budget_remaining: Option<i64>,
        lease_scope: Option<(&str, &str)>,
        now: &str,
    ) -> Result<DomainEvent> {
        let source_event_id = context
            .source_event_id
            .as_deref()
            .or(request.causation_id.as_deref());
        let incident_digest = context
            .incident_digest
            .clone()
            .unwrap_or_else(|| wake_incident_digest(&request.incident_key, source_event_id));
        let attention_id = context.attention_id.clone();
        let safe_incident_key = bounded_wake_ref(&request.incident_key);
        let safe_source_event_id = source_event_id.map(bounded_wake_ref);
        let identity_id = (!request.identity_id.trim().is_empty())
            .then(|| bounded_wake_ref(&request.identity_id));
        let (
            event_type,
            decision_code,
            reason_code,
            admission_phase,
            lease_owner,
            leased_until,
            cooldown_until,
        ) = match &decision {
            WakeDecisionEvent::Admitted {
                leased_until,
                cooldown_until,
            } => (
                "agent.wake.admitted",
                "turn_admitted",
                "turn_admitted",
                "turn",
                Some(bounded_wake_ref(&request.lease_owner)),
                Some(leased_until.clone()),
                Some(cooldown_until.clone()),
            ),
            WakeDecisionEvent::Suppressed(reason) => (
                "agent.wake.suppressed",
                "deterministically_suppressed",
                reason.code(),
                "policy",
                None,
                None,
                None,
            ),
            WakeDecisionEvent::SetupRequired(reason) => (
                "agent.wake.setup_required",
                "setup_required",
                reason.code(),
                "configuration",
                None,
                None,
                None,
            ),
        };
        let source_key = source_event_id.unwrap_or(request.correlation_id.as_str());
        let decision_dedupe = if event_type == "agent.wake.admitted" {
            wake_admitted_dedupe_key(request, context)
        } else {
            // The responder is part of the decision: a changed binding records
            // a new one instead of colliding with the earlier decision's key.
            // Identity-less decisions keep their stored key.
            let responder = identity_id
                .as_deref()
                .map(|id| format!(":{id}"))
                .unwrap_or_default();
            format!(
                "agent-wake-decision:{}:{}",
                event_type.replace('.', "-"),
                wake_incident_digest(
                    &format!(
                        "{}:{}:{}:{}:{}:{}:{}{}",
                        request.scope_type,
                        request.scope_id,
                        request.incident_key,
                        source_key,
                        reason_code,
                        event_type,
                        incident_digest,
                        responder,
                    ),
                    None,
                )
            )
        };
        let causation_depth = (request.reaction_depth.max(0) + 1).min(16);
        let payload_json = json!({
            "decision": decision_code,
            "turn_job_id": (event_type == "agent.wake.admitted").then(|| context.turn.as_ref().map(|p|
                Uuid::new_v5(&Uuid::NAMESPACE_OID, format!("{}:turn", p.dedupe_key).as_bytes()).to_string())).flatten(),
            "reason": reason_code,
            "admission_phase": admission_phase,
            "identity_id": identity_id,
            "scope_type": bounded_wake_ref(&request.scope_type),
            "scope_id": bounded_wake_ref(&request.scope_id),
            "incident_key": safe_incident_key,
            "incident_digest": incident_digest,
            "attention_id": attention_id,
            "attention_status": context.attention_status.as_deref(),
            "attention_version": context.attention_version,
            "source_event_id": safe_source_event_id,
            "correlation_id": bounded_wake_ref(&request.correlation_id),
            "causation_id": request.causation_id.as_deref().map(bounded_wake_ref),
            "causation_depth": causation_depth,
            "reaction_depth": request.reaction_depth.clamp(0, 16),
            "budget_remaining": budget_remaining,
            "lease_scope_type": lease_scope.map(|value| value.0),
            "lease_scope_id": lease_scope.map(|value| value.1),
            "lease_owner": lease_owner,
            "leased_until": leased_until,
            "cooldown_until": cooldown_until,
        })
        .to_string();
        let event_scope_type = if matches!(
            request.scope_type.as_str(),
            "account" | "project" | "room" | "task" | "system" | "agent_chat"
        ) {
            request.scope_type.clone()
        } else {
            "system".to_owned()
        };
        let event_scope_id = if event_scope_type == request.scope_type {
            request.scope_id.clone()
        } else {
            wake_incident_digest(&request.scope_id, None)
        };
        let event = DomainEventRepo::append_event_in_tx(
            &*self.db,
            transaction,
            &CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: event_type.to_owned(),
                entity_type: "agent_wake".to_owned(),
                entity_id: safe_incident_key,
                actor_type: "attention_projection".to_owned(),
                actor_id: None,
                scope_type: event_scope_type,
                scope_id: event_scope_id,
                correlation_id: request.correlation_id.clone(),
                causation_id: request.causation_id.clone(),
                causation_depth,
                dedupe_key: Some(decision_dedupe),
                payload_json,
                created_at: now.to_owned(),
            },
        )
        .await?;
        let (kind, admission, expected) = match &decision {
            WakeDecisionEvent::Admitted { .. } => {
                let prepared = context
                    .turn
                    .as_ref()
                    .ok_or_else(|| ServiceError::invalid_operation("wake has no prepared turn"))?;
                let id = context
                    .attention_id
                    .as_deref()
                    .ok_or_else(|| ServiceError::invalid_operation("wake has no Attention"))?;
                let actual = self
                    .db
                    .get_attention_in_tx(transaction, id)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
                let admitted = self
                    .wake_turns
                    .build_prepared_turn(&event, prepared, &actual)?;
                (
                    db::AgentWakeDispositionKind::TurnAdmitted,
                    Some(admitted),
                    Some(expected_attention(&actual)),
                )
            }
            WakeDecisionEvent::Suppressed(_) => (
                db::AgentWakeDispositionKind::DeterministicallySuppressed,
                None,
                None,
            ),
            WakeDecisionEvent::SetupRequired(_) if context.attention_id.is_some() => {
                (db::AgentWakeDispositionKind::SetupRequired, None, None)
            }
            WakeDecisionEvent::SetupRequired(_) => (
                db::AgentWakeDispositionKind::DeterministicallySuppressed,
                None,
                None,
            ),
        };
        // The latest decision per Attention item (each batch member, with its
        // own digest) is what the sweep reads.
        let disposition_name = kind.to_string();
        let members: Vec<(String, String)> = if context.batch.is_empty() {
            context
                .attention_id
                .iter()
                .map(|id| (id.clone(), incident_digest.clone()))
                .collect()
        } else {
            context
                .batch
                .iter()
                .map(|a| (a.id.clone(), wake_attention_incident_digest(a)))
                .collect()
        };
        for (id, digest) in members {
            self.db
                .record_latest_attention_wake_in_tx(
                    transaction,
                    db::RecordLatestAttentionWakeInTxInput {
                        attention_id: &id,
                        incident_digest: &digest,
                        disposition: &disposition_name,
                        reason: reason_code,
                        identity_id: identity_id.as_deref(),
                        updated_at: now,
                        existing_attention_id: &id,
                    },
                )
                .await?;
        }
        // Audit-event dedupe is checked before persistence, making a replay a no-op.
        if self
            .db
            .get_agent_wake_disposition_in_tx(
                transaction,
                self.wake_turns.consumer_name(),
                &event.id,
            )
            .await?
            .is_some()
        {
            return Ok(event);
        }
        let turn_job_id = admission.as_ref().map(|a| a.turn.id.clone());
        let disposition = db::CreateAgentWakeDisposition {
            id: Uuid::new_v5(
                &Uuid::NAMESPACE_OID,
                format!("wake-disposition:{}", event.id).as_bytes(),
            )
            .to_string(),
            consumer_name: self.wake_turns.consumer_name().to_owned(),
            source_event_id: event.id.clone(),
            source_event_sequence: event.sequence,
            attempt_number: 1,
            max_attempts: 3,
            disposition: kind,
            reason: reason_code.to_owned(),
            turn_job_id: turn_job_id.clone(),
            attention_id: (kind == db::AgentWakeDispositionKind::SetupRequired)
                .then(|| context.attention_id.clone())
                .flatten(),
            retry_at: None,
            incident_key: Some(bounded_wake_ref(&request.incident_key)),
            incident_digest: Some(incident_digest),
            binding_id: admission
                .as_ref()
                .and_then(|a| a.turn.responder_binding_id.clone()),
            binding_version: admission
                .as_ref()
                .and_then(|a| a.turn.responder_binding_version),
            profile_id: admission.as_ref().map(|a| a.turn.profile_id.clone()),
            profile_version: admission.as_ref().and_then(|a| a.turn.profile_version),
            provenance_json: Some(json!({"attention_id": context.attention_id}).to_string()),
            parent_disposition_id: None,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        };
        self.db
            .persist_agent_wake_in_tx(
                transaction,
                &event,
                db::PersistAgentWake {
                    disposition,
                    admission,
                    expected_attention: expected,
                },
            )
            .await?;
        if let Some(turn_id) = turn_job_id {
            if context
                .batch
                .first()
                .is_some_and(|a| wake_budget_category(a) == "blocker")
            {
                for item in &context.batch {
                    let current = self
                        .db
                        .get_attention_in_tx(transaction, &item.id)
                        .await?
                        .ok_or(db::DbError::VersionConflict)?;
                    if current.status != "open"
                        || current.version != item.version
                        || wake_attention_incident_digest(&current)
                            != wake_attention_incident_digest(item)
                    {
                        return Err(db::DbError::VersionConflict.into());
                    }
                    // A re-admission replaces a failed or imported row for the digest.
                    self.db
                        .record_admitted_wake_blocker_in_tx(
                            transaction,
                            &item.id,
                            wake_attention_incident_digest(item),
                            &turn_id,
                            now,
                        )
                        .await?;
                }
                self.db
                    .record_admitted_wake_batch_in_tx(
                        transaction,
                        &context.batch[0].scope_id,
                        now,
                        cooldown_until,
                        &turn_id,
                    )
                    .await?;
            }
            // The owner's answer turn becomes the escalated blockers' turn, so
            // they are re-armed: if they persist after it, they escalate again.
            if let Some(escalation_id) = context.batch.first().and_then(answered_escalation_id) {
                self.db
                    .rearm_answered_escalation_blockers_in_tx(
                        transaction,
                        &turn_id,
                        now,
                        &escalation_id,
                    )
                    .await?;
            }
        }
        Ok(event)
    }

    pub(super) async fn wake_budget_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        identity_id: &str,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<(Option<i64>, String, String)> {
        let mut budget_scope_type = scope_type.to_owned();
        let mut budget_scope_id = scope_id.to_owned();
        let project_id = match scope_type {
            "project" => Some(scope_id.to_owned()),
            "task" => {
                self.db
                    .wake_budget_task_project_id_in_tx(transaction, scope_id)
                    .await?
            }
            "agent_chat" => self
                .db
                .wake_budget_chat_project_id_in_tx(transaction, scope_id)
                .await?
                .flatten(),
            "account" => None,
            _ => return Ok((Some(0), budget_scope_type, budget_scope_id)),
        };

        if let Some(project_id) = project_id {
            budget_scope_type = "project".to_owned();
            budget_scope_id = project_id.clone();
            let budget = self
                .db
                .active_project_identity_wake_budget_in_tx(transaction, identity_id, &project_id)
                .await?;
            return Ok((budget.or(Some(0)), budget_scope_type, budget_scope_id));
        }

        if scope_type == "agent_chat" {
            if let Some(account_id) = self
                .db
                .wake_budget_main_chat_account_id_in_tx(transaction, scope_id)
                .await?
            {
                return Ok((Some(10), "account".to_owned(), account_id));
            }
        }

        // Direct account agents have a bounded default window.  Project
        // memberships always use their explicit wake_budget above; this
        // fallback does not grant membership or any data authority.
        if scope_type == "account" {
            return Ok((Some(10), budget_scope_type, budget_scope_id));
        }
        Ok((Some(0), budget_scope_type, budget_scope_id))
    }
}
