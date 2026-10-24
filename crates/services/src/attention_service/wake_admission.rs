//! Wake admission transactions and suppression persistence.

use super::*;

impl AttentionService {
    pub(super) async fn admit_wake_with_context(
        &self,
        request: WakeAdmissionRequest,
        context: WakeDecisionContext,
    ) -> Result<WakeAdmissionResult> {
        if let Some(reason) = wake_policy_suppression_reason(&request) {
            return self
                .persist_suppressed_wake(&request, &context, reason)
                .await;
        }
        // A projection crash can replay the same source event after the
        // lease/event transaction committed. Context-bearing projection calls
        // are idempotent at the admission boundary: return the original
        // lease metadata instead of consuming budget again or turning one
        // source event into an admitted event followed by a duplicate
        // suppression.
        if context.source_event_id.is_some() {
            let dedupe_key = wake_admitted_dedupe_key(&request, &context);
            if let Some(event) =
                DomainEventRepo::get_event_by_dedupe(&*self.db, &dedupe_key).await?
            {
                let payload =
                    serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
                let leased_until = payload
                    .get("leased_until")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let cooldown_until = payload
                    .get("cooldown_until")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let budget_remaining = payload.get("budget_remaining").and_then(Value::as_i64);
                return Ok(WakeAdmissionResult::Admitted {
                    leased_until,
                    cooldown_until,
                    budget_remaining,
                });
            }
        }
        if matches!(request.scope_type.as_str(), "project" | "agent_chat")
            && !self
                .wake_responder_is_configured(&request.scope_type, &request.scope_id, None)
                .await?
        {
            if context.attention_id.is_some() {
                return self.persist_setup_required_wake(&request, &context).await;
            }
            return self
                .persist_suppressed_wake(&request, &context, WakeSuppressionReason::IneligibleScope)
                .await;
        }
        if !matches!(
            request.scope_type.as_str(),
            "account" | "project" | "agent_chat" | "task"
        ) {
            return self
                .persist_suppressed_wake(&request, &context, WakeSuppressionReason::IneligibleScope)
                .await;
        }
        if !self
            .wake_identity_is_eligible(&request.identity_id, &request.scope_type, &request.scope_id)
            .await?
        {
            return self
                .persist_suppressed_wake(&request, &context, WakeSuppressionReason::IneligibleScope)
                .await;
        }

        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let committed = self
            .admit_wake_in_tx(&mut transaction, &request, &context)
            .await?;
        transaction.commit().await?;
        if let Some((scope_type, scope_id)) = committed.stall_scope {
            self.publish_autonomy_stall(
                &scope_type,
                &scope_id,
                "the Project Agent's hourly wake budget is exhausted",
            )
            .await;
        }
        Ok(committed.result)
    }

    pub(super) async fn admit_wake_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        request: &WakeAdmissionRequest,
        context: &WakeDecisionContext,
    ) -> Result<CommittedWakeAdmission> {
        let mut stall_scope = None;
        let result = self
            .admit_wake_result_in_tx(transaction, request, context, &mut stall_scope)
            .await?;
        Ok(CommittedWakeAdmission {
            result,
            stall_scope,
        })
    }
    async fn admit_wake_result_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        request: &WakeAdmissionRequest,
        context: &WakeDecisionContext,
        stall_scope: &mut Option<(String, String)>,
    ) -> Result<WakeAdmissionResult> {
        let now = parse_rfc3339(&request.now).unwrap_or_else(Utc::now);
        let lease_seconds = request.lease_seconds.clamp(1, 300);
        let cooldown_seconds = request.cooldown_seconds.clamp(1, 86_400);
        let leased_until = (now + Duration::seconds(lease_seconds)).to_rfc3339();
        let cooldown_until = (now + Duration::seconds(cooldown_seconds)).to_rfc3339();
        let now = now.to_rfc3339();

        // Attention is authoritative at the admission boundary.  A source
        // event can be delivered after an operator resolves its incident;
        // that event receives a terminal suppression rather than waking a
        // stale responder.
        let attention_status = if let Some(attention_id) = context.attention_id.as_deref() {
            self.db
                .wake_attention_status_in_tx(transaction, attention_id)
                .await?
        } else {
            self.db
                .wake_incident_status_in_tx(transaction, &request.incident_key)
                .await?
        };
        if attention_status.as_deref() == Some("resolved") {
            self.append_wake_decision_in_tx(
                transaction,
                request,
                context,
                WakeDecisionEvent::Suppressed(WakeSuppressionReason::ResolvedIncident),
                None,
                None,
                &now,
            )
            .await?;
            return Ok(WakeAdmissionResult::Suppressed {
                reason: WakeSuppressionReason::ResolvedIncident,
            });
        }

        // The Task row is the action authority. A recovery that wins after
        // projection but before admission must suppress the stale wake in the
        // same write transaction that would otherwise consume budget.
        if context.requires_current_task_intervention || context.orphan_execution_id.is_some() {
            let active = if let Some(execution_id) = context.orphan_execution_id.as_deref() {
                // A successor can commit between projection and admission.
                // Check again under the same writer lock as wake budgeting.
                orphan_attempt_is_current(&self.db, transaction, execution_id).await?
            } else if let Some(task_id) = context.task_id.as_deref() {
                let raw = self
                    .db
                    .wake_unsettled_task_condition_in_tx(transaction, task_id)
                    .await?;
                raw.and_then(|raw| db::task_condition::decode(&raw).ok())
                    .is_some_and(|condition| {
                        db::task_condition::material_blocker(&condition).requires_intervention
                    })
            } else {
                false
            };
            if !active {
                if let Some(attention_id) = context.attention_id.as_deref() {
                    self.db
                        .resolve_stale_wake_attention_in_tx(
                            transaction,
                            &now,
                            &now,
                            attention_id,
                            context.attention_version,
                        )
                        .await?;
                }
                self.append_wake_decision_in_tx(
                    transaction,
                    request,
                    context,
                    WakeDecisionEvent::Suppressed(WakeSuppressionReason::ResolvedIncident),
                    None,
                    None,
                    &now,
                )
                .await?;
                return Ok(WakeAdmissionResult::Suppressed {
                    reason: WakeSuppressionReason::ResolvedIncident,
                });
            }
        }

        if context.attention_id.is_none() {
            self.append_wake_decision_in_tx(
                transaction,
                request,
                context,
                WakeDecisionEvent::Suppressed(WakeSuppressionReason::ResolvedIncident),
                None,
                None,
                &now,
            )
            .await?;
            return Ok(WakeAdmissionResult::Suppressed {
                reason: WakeSuppressionReason::ResolvedIncident,
            });
        }
        if let Some(id) = context.attention_id.as_deref() {
            let actual = self
                .db
                .get_attention_in_tx(transaction, id)
                .await?
                .ok_or(db::DbError::VersionConflict)?;
            if context
                .attention_version
                .is_some_and(|version| version != actual.version)
                || context.source_event_id.as_deref() != Some(actual.source_event_id.as_str())
            {
                return Err(db::DbError::VersionConflict.into());
            }
            let canonical = match request.scope_type.as_str() {
                "project" | "account" => {
                    Some((request.scope_type.clone(), request.scope_id.clone()))
                }
                "task" => self
                    .db
                    .wake_live_task_project_id_in_tx(transaction, &request.scope_id)
                    .await?
                    .map(|id| ("project".to_owned(), id)),
                "agent_chat" => {
                    let chat = self
                        .db
                        .wake_chat_canonical_scope_in_tx(transaction, &request.scope_id)
                        .await?;
                    chat.and_then(|row| {
                        row.project_id
                            .ok()
                            .flatten()
                            .map(|id| ("project".to_owned(), id))
                            .or_else(|| {
                                row.account_id
                                    .ok()
                                    .flatten()
                                    .map(|id| ("account".to_owned(), id))
                            })
                    })
                }
                _ => None,
            };
            if canonical.as_ref() != Some(&(actual.scope_type.clone(), actual.scope_id.clone())) {
                self.append_wake_decision_in_tx(
                    transaction,
                    request,
                    context,
                    WakeDecisionEvent::Suppressed(WakeSuppressionReason::IneligibleScope),
                    None,
                    None,
                    &now,
                )
                .await?;
                return Ok(WakeAdmissionResult::Suppressed {
                    reason: WakeSuppressionReason::IneligibleScope,
                });
            }
        }
        if context.turn.is_none() {
            self.append_wake_decision_in_tx(
                transaction,
                request,
                context,
                WakeDecisionEvent::SetupRequired(WakeSetupReason::ResponderBindingMissing),
                None,
                None,
                &now,
            )
            .await?;
            return Ok(WakeAdmissionResult::SetupRequired {
                reason: WakeSetupReason::ResponderBindingMissing,
            });
        }
        let category = context
            .batch
            .first()
            .map(wake_budget_category)
            .unwrap_or("delivery");
        if category == "delivery" {
            let repeated: bool = self
                .db
                .failed_wake_matches_current_profile_in_tx(
                    transaction,
                    &request.incident_key,
                    context.incident_digest.as_deref(),
                )
                .await?;
            if repeated {
                self.append_wake_decision_in_tx(
                    transaction,
                    request,
                    context,
                    WakeDecisionEvent::Suppressed(WakeSuppressionReason::RepeatedFailure),
                    None,
                    None,
                    &now,
                )
                .await?;
                return Ok(WakeAdmissionResult::Suppressed {
                    reason: WakeSuppressionReason::RepeatedFailure,
                });
            }
        }
        // A blocker digest gets one completed turn, and no member gets a second
        // turn while any linked turn is queued or running. Owner escalation
        // owns any later response.
        if category == "blocker" {
            let at = parse_rfc3339(&now).unwrap_or_else(Utc::now);
            for attention in &context.batch {
                let state = crate::wake_blocker::blocker_turn_state(
                    transaction,
                    &attention.id,
                    &wake_attention_incident_digest(attention),
                    None,
                    at,
                )
                .await?;
                if matches!(
                    state,
                    BlockerTurnState::InFlight | BlockerTurnState::Completed { .. }
                ) {
                    self.append_wake_decision_in_tx(
                        transaction,
                        request,
                        context,
                        WakeDecisionEvent::Suppressed(WakeSuppressionReason::DuplicateIncident),
                        None,
                        None,
                        &now,
                    )
                    .await?;
                    return Ok(WakeAdmissionResult::Suppressed {
                        reason: WakeSuppressionReason::DuplicateIncident,
                    });
                }
            }
            if let Some(project_id) = context
                .batch
                .first()
                .filter(|a| a.scope_type == "project")
                .map(|a| &a.scope_id)
            {
                let cooling: bool = self
                    .db
                    .wake_project_batch_is_cooling_in_tx(transaction, project_id, &now)
                    .await?;
                if cooling {
                    self.append_wake_decision_in_tx(
                        transaction,
                        request,
                        context,
                        WakeDecisionEvent::Suppressed(WakeSuppressionReason::Cooldown),
                        None,
                        None,
                        &now,
                    )
                    .await?;
                    return Ok(WakeAdmissionResult::Suppressed {
                        reason: WakeSuppressionReason::Cooldown,
                    });
                }
            }
        }
        let (total_budget, budget_scope_type, budget_scope_id) = self
            .wake_budget_in_tx(
                transaction,
                &request.identity_id,
                &request.scope_type,
                &request.scope_id,
            )
            .await?;
        // The owner initiated an answer wake; it never spends autonomy budget.
        let total_budget = if context.batch.first().is_some_and(is_owner_answer) {
            None
        } else {
            total_budget
        };
        // Small budgets share one pool rather than starving a bucket.
        let shared_pool = budget_scope_type == "project"
            && total_budget.is_some_and(|total| total < MIN_SPLIT_WAKE_BUDGET);
        let budget = total_budget.map(|total| {
            if budget_scope_type == "project" && !shared_pool {
                category_budget(total, category)
            } else {
                total
            }
        });
        // The historical lease primary key includes identity_id.  Keep that
        // immutable schema, but make the active policy incident-global by
        // rejecting any live lease/cooldown for the canonical scope before
        // budget accounting.  This closes the binding-replacement race where
        // a new identity could otherwise analyse the same incident in
        // parallel with the old identity.
        let existing_lease = self
            .db
            .active_incident_wake_lease_in_tx(
                transaction,
                db::ActiveIncidentWakeLeaseInTxInput {
                    scope_type: &budget_scope_type,
                    scope_id: &budget_scope_id,
                    incident_key: &request.incident_key,
                    leased_after: &now,
                    cooldown_after: &now,
                },
            )
            .await?;
        if let Some(row) = existing_lease {
            let leased_until_existing: String = row.leased_until?;
            let cooldown_until_existing: Option<String> = row.cooldown_until?;
            let reason = if leased_until_existing > now {
                WakeSuppressionReason::DuplicateIncident
            } else if cooldown_until_existing
                .as_deref()
                .is_some_and(|value| value > now.as_str())
            {
                WakeSuppressionReason::Cooldown
            } else {
                WakeSuppressionReason::DuplicateIncident
            };
            self.append_wake_decision_in_tx(
                transaction,
                request,
                context,
                WakeDecisionEvent::Suppressed(reason.clone()),
                budget,
                Some((&budget_scope_type, &budget_scope_id)),
                &now,
            )
            .await?;
            return Ok(WakeAdmissionResult::Suppressed { reason });
        }

        // Direct callers may not carry an Attention row (and therefore miss
        // the context-bearing replay check above).  Once an existing lease or
        // cooldown is ruled out, a matching admitted decision still means the
        // source already admitted its turn; return its metadata rather than
        // incrementing the budget a second time before the domain-event
        // dedupe turns the append into a no-op.
        let dedupe_key = wake_admitted_dedupe_key(request, context);
        let admitted_payload = self
            .db
            .admitted_wake_payload_in_tx(transaction, &dedupe_key)
            .await?;
        if let Some(payload_json) = admitted_payload {
            let payload = serde_json::from_str::<Value>(&payload_json).unwrap_or(Value::Null);
            let leased_until = payload
                .get("leased_until")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let cooldown_until = payload
                .get("cooldown_until")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let budget_remaining = payload.get("budget_remaining").and_then(Value::as_i64);
            return Ok(WakeAdmissionResult::Admitted {
                leased_until,
                cooldown_until,
                budget_remaining,
            });
        }

        if budget == Some(0) {
            *stall_scope = Some((budget_scope_type.clone(), budget_scope_id.clone()));
            if budget_scope_type == "project" {
                let open_incidents: i64 = self
                    .db
                    .open_project_wake_incident_count_in_tx(transaction, &budget_scope_id)
                    .await?;
                if open_incidents > 0 {
                    DomainEventRepo::append_event_in_tx(&*self.db, transaction, &CreateDomainEvent {
                        id: new_uuid_v4(), event_type: "notification.requested".to_owned(),
                        entity_type: "project".to_owned(), entity_id: budget_scope_id.clone(),
                        actor_type: "system".to_owned(), actor_id: None,
                        scope_type: "project".to_owned(), scope_id: budget_scope_id.clone(),
                        correlation_id: request.correlation_id.clone(), causation_id: request.causation_id.clone(),
                        causation_depth: request.reaction_depth,
                        dedupe_key: Some(format!("autonomy-stalled:{}:{}:{}:{}", budget_scope_type, budget_scope_id,
                            context.turn.as_ref().and_then(|p|p.binding_id.as_deref()).unwrap_or_default(),
                            context.turn.as_ref().and_then(|p|p.binding_version).unwrap_or_default())),
                        payload_json: json!({
                            "project_id": budget_scope_id, "task_id": null,
                            "event_type": "project.autonomy_stalled",
                            "title": format!("Project Agent stopped: {open_incidents} open incident(s) unanswered"),
                            "body": "the Project Agent's hourly wake budget is exhausted"
                        }).to_string(), created_at: now.clone(),
                    }).await?;
                }
            }
            self.append_wake_decision_in_tx(
                transaction,
                request,
                context,
                WakeDecisionEvent::Suppressed(WakeSuppressionReason::BudgetExhausted),
                budget,
                Some((&budget_scope_type, &budget_scope_id)),
                &now,
            )
            .await?;
            return Ok(WakeAdmissionResult::Suppressed {
                reason: WakeSuppressionReason::BudgetExhausted,
            });
        }

        let mut budget_attempt = transaction.begin().await?;
        let window_started =
            (parse_rfc3339(&now).unwrap_or_else(Utc::now) - Duration::hours(1)).to_rfc3339();
        let budget_remaining = if let Some(budget) = budget {
            let current = self
                .db
                .wake_budget_window_in_tx(
                    &mut budget_attempt,
                    &budget_scope_type,
                    &budget_scope_id,
                    category,
                )
                .await?;
            let (admitted_count, in_window) = current
                .map(|row| {
                    let started: String = row.window_started_at?;
                    let count: i64 = row.admitted_count?;
                    Ok::<_, db::DbError>((count, started > window_started))
                })
                .transpose()?
                .unwrap_or((0, false));
            let used = if shared_pool {
                self.db
                    .shared_wake_budget_used_in_tx(
                        &mut budget_attempt,
                        &budget_scope_type,
                        &budget_scope_id,
                        &window_started,
                    )
                    .await?
            } else if in_window {
                admitted_count
            } else {
                0
            };
            if used >= budget {
                self.append_wake_decision_in_tx(
                    &mut budget_attempt,
                    request,
                    context,
                    WakeDecisionEvent::Suppressed(WakeSuppressionReason::BudgetExhausted),
                    Some(0),
                    Some((&budget_scope_type, &budget_scope_id)),
                    &now,
                )
                .await?;
                budget_attempt.commit().await?;
                return Ok(WakeAdmissionResult::Suppressed {
                    reason: WakeSuppressionReason::BudgetExhausted,
                });
            }
            if in_window {
                self.db
                    .increment_wake_budget_window_in_tx(
                        &mut budget_attempt,
                        db::IncrementWakeBudgetWindowInTxInput {
                            identity_id: &request.identity_id,
                            updated_at: &now,
                            scope_type: &budget_scope_type,
                            scope_id: &budget_scope_id,
                            category,
                        },
                    )
                    .await?;
            } else {
                self.db
                    .start_wake_budget_window_in_tx(
                        &mut budget_attempt,
                        db::StartWakeBudgetWindowInTxInput {
                            identity_id: &request.identity_id,
                            scope_type: &budget_scope_type,
                            scope_id: &budget_scope_id,
                            category,
                            window_started_at: &now,
                            updated_at: &now,
                        },
                    )
                    .await?;
            }
            Some((budget - used - 1).max(0))
        } else {
            None
        };

        let lease_result = self
            .db
            .admit_incident_wake_lease_in_tx(
                &mut budget_attempt,
                db::AdmitIncidentWakeLeaseInTxInput {
                    identity_id: &request.identity_id,
                    scope_type: &budget_scope_type,
                    scope_id: &budget_scope_id,
                    incident_key: &request.incident_key,
                    lease_owner: &request.lease_owner,
                    leased_until: &leased_until,
                    reaction_depth: request.reaction_depth.clamp(0, 8),
                    updated_at: &now,
                    cooldown_until: &cooldown_until,
                    last_admitted_at: &now,
                    correlation_id: &request.correlation_id,
                    causation_id: request.causation_id.as_deref(),
                },
            )
            .await?;
        if lease_result == 0 {
            // This is normally covered by the global pre-check.  Keep the
            // compare-and-swap result authoritative for same-transaction
            // replays and classify an expired lease/cooldown conservatively.
            budget_attempt.rollback().await?;
            self.append_wake_decision_in_tx(
                transaction,
                request,
                context,
                WakeDecisionEvent::Suppressed(WakeSuppressionReason::DuplicateIncident),
                None,
                None,
                &now,
            )
            .await?;
            return Ok(WakeAdmissionResult::Suppressed {
                reason: WakeSuppressionReason::DuplicateIncident,
            });
        }

        budget_attempt.commit().await?;
        // The lease, budget increment, and admitted wake audit share one
        // transaction.  No budget is consumed for suppressed/setup-required
        // decision events.
        self.append_wake_decision_in_tx(
            transaction,
            request,
            context,
            WakeDecisionEvent::Admitted {
                leased_until: leased_until.clone(),
                cooldown_until: cooldown_until.clone(),
            },
            budget_remaining,
            Some((&budget_scope_type, &budget_scope_id)),
            &now,
        )
        .await?;
        Ok(WakeAdmissionResult::Admitted {
            leased_until,
            cooldown_until,
            budget_remaining,
        })
    }

    pub(super) async fn persist_suppressed_wake(
        &self,
        request: &WakeAdmissionRequest,
        context: &WakeDecisionContext,
        reason: WakeSuppressionReason,
    ) -> Result<WakeAdmissionResult> {
        let now = parse_rfc3339(&request.now)
            .unwrap_or_else(Utc::now)
            .to_rfc3339();
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        self.append_wake_decision_in_tx(
            &mut transaction,
            request,
            context,
            WakeDecisionEvent::Suppressed(reason.clone()),
            None,
            None,
            &now,
        )
        .await?;
        transaction.commit().await?;
        Ok(WakeAdmissionResult::Suppressed { reason })
    }

    pub(super) async fn persist_setup_required_wake(
        &self,
        request: &WakeAdmissionRequest,
        context: &WakeDecisionContext,
    ) -> Result<WakeAdmissionResult> {
        let now = parse_rfc3339(&request.now)
            .unwrap_or_else(Utc::now)
            .to_rfc3339();
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        self.append_wake_decision_in_tx(
            &mut transaction,
            request,
            context,
            WakeDecisionEvent::SetupRequired(WakeSetupReason::ResponderBindingMissing),
            None,
            None,
            &now,
        )
        .await?;
        transaction.commit().await?;
        Ok(WakeAdmissionResult::SetupRequired {
            reason: WakeSetupReason::ResponderBindingMissing,
        })
    }
}
