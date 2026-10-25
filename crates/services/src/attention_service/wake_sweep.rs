//! Attention sweep candidates and blocker wake batches.

use super::*;

impl AttentionService {
    /// Level-triggered reconsideration of open Attention. Each scope is
    /// isolated: a failing Project is logged and skipped, never the tick.
    /// Within a Project, blockers are batched first, then budget-suppressed
    /// decisions, then items waiting on setup.
    pub async fn sweep_once_at(&self, now: &str) -> Result<usize> {
        let rows = self.db.open_wake_sweep_candidates(now).await?;
        let mut scopes = std::collections::BTreeMap::<(String, String), Vec<SweepCandidate>>::new();
        for row in rows {
            let disposition: Option<String> = row.latest_disposition?;
            let latest = match disposition {
                Some(disposition) => Some(LatestWakeDecision {
                    digest: row.latest_digest?,
                    disposition,
                    reason: row.latest_reason?,
                    identity_id: row.latest_identity?,
                }),
                None => None,
            };
            scopes
                .entry((row.scope_type?, row.scope_id?))
                .or_default()
                .push(SweepCandidate {
                    id: row.id?,
                    latest,
                });
        }
        let mut admitted = 0;
        for ((scope_type, scope_id), candidates) in scopes {
            match self
                .sweep_scope(&scope_type, &scope_id, candidates, now)
                .await
            {
                Ok(count) => admitted += count,
                Err(error) => {
                    tracing::warn!(%scope_type, %scope_id, %error, "wake sweep skipped a scope after an error");
                }
            }
        }
        Ok(admitted)
    }

    async fn sweep_scope(
        &self,
        scope_type: &str,
        scope_id: &str,
        candidates: Vec<SweepCandidate>,
        now: &str,
    ) -> Result<usize> {
        let at = parse_rfc3339(now).unwrap_or_else(Utc::now);
        let responder = if scope_type == "project" {
            let mut conn = self.db.pool().acquire().await?;
            crate::wake_blocker::current_project_responder(&mut conn, scope_id).await?
        } else {
            None
        };
        let mut blockers = Vec::new();
        let mut decisions = Vec::new();
        let mut others = Vec::new();
        for candidate in candidates {
            let Some(attention) = self.db.get_attention(&candidate.id).await? else {
                continue;
            };
            // This incident already asks the owner. An Agent cannot resolve
            // it, so neither a blocker turn nor its later escalation helps.
            if attention.attention_type == "review_needs_owner" {
                continue;
            }
            if attention.scope_type == "project" && blocker_category(&attention.attention_type) {
                // The one lead filter: a subtask's blocker that the lead of
                // its coordination root handles (the root is about to
                // re-plan for it, or its lead is planning) wakes nobody.
                // Level state: once the lead left the park in place, or the
                // root itself parked, the next sweep admits the wake.
                if self.lead_handles_blocker(&attention).await {
                    tracing::debug!(attention_id = %attention.id, "subtask blocker left to its root's lead");
                    continue;
                }
                let digest = wake_attention_incident_digest(&attention);
                let state = {
                    let mut conn = self.db.pool().acquire().await?;
                    crate::wake_blocker::blocker_turn_state(
                        &mut conn,
                        &attention.id,
                        &digest,
                        responder.as_ref(),
                        at,
                    )
                    .await?
                };
                match state {
                    BlockerTurnState::InFlight
                    | BlockerTurnState::Held
                    | BlockerTurnState::Completed { escalated: true } => {}
                    BlockerTurnState::Completed { escalated: false } => {
                        crate::project_escalation::ProjectEscalationService::new(Arc::clone(
                            &self.db,
                        ))
                        .escalate_blocker(&attention)
                        .await?;
                    }
                    BlockerTurnState::Eligible => {
                        let suppressed = candidate.latest.as_ref().is_some_and(|latest| {
                            crate::wake_blocker::is_policy_suppression(&latest.reason)
                                && latest.digest.as_deref() == Some(digest.as_str())
                                && latest.identity_id.as_deref()
                                    == responder.as_ref().map(|r| r.identity_id.as_str())
                        });
                        if !suppressed {
                            blockers.push(attention);
                        }
                    }
                }
            } else if let Some(latest) = &candidate.latest {
                if matches!(latest.disposition.as_str(), "setup_required" | "deferred") {
                    others.push(attention);
                } else if attention.attention_type == DECISION_RECORDED_CATEGORY
                    && matches!(
                        latest.reason.as_str(),
                        "budget_exhausted" | "cooldown" | "duplicate_incident"
                    )
                {
                    // An unanswered decision is level state: retry it once the
                    // bucket or cooldown allows.
                    decisions.push(attention);
                }
            }
        }
        let mut admitted = 0;
        if !blockers.is_empty() && self.admit_blocker_batch(blockers, now).await? {
            admitted += 1;
        }
        for attention in decisions.into_iter().chain(others) {
            let result: Result<WakeAdmissionResult> = async {
                let request = self.request_for_attention(&attention, now).await?;
                self.admit_wake(request).await
            }
            .await;
            match result {
                Ok(WakeAdmissionResult::Admitted { .. }) => admitted += 1,
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(attention_id=%attention.id,%error,"wake reconsideration failed");
                }
            }
        }
        Ok(admitted)
    }

    /// Whether an open blocker is a subtask's park that its root's lead
    /// handles (`task_hierarchy::lead_handles_subtask_park`).
    async fn lead_handles_blocker(&self, attention: &AttentionProjection) -> bool {
        let details = serde_json::from_str::<Value>(&attention.details_json).unwrap_or(Value::Null);
        if details.get("entity_type").and_then(Value::as_str) != Some("task") {
            return false;
        }
        let Some(task_id) = details.get("entity_id").and_then(Value::as_str) else {
            return false;
        };
        crate::task_hierarchy::subtask_park_is_the_leads(&self.db, task_id).await
    }

    /// One wake per Project window for every eligible blocker, with one directive.
    async fn admit_blocker_batch(
        &self,
        batch: Vec<AttentionProjection>,
        now: &str,
    ) -> Result<bool> {
        let mut members = Vec::with_capacity(batch.len());
        for attention in batch {
            let request = self.request_for_attention(&attention, now).await?;
            if let Some(reason) = wake_policy_suppression_reason(&request) {
                let context = self.wake_decision_context_for_incident(&request).await?;
                self.persist_suppressed_wake(&request, &context, reason)
                    .await?;
                continue;
            }
            members.push((attention, request));
        }
        let Some((first, request)) = members.first().cloned() else {
            return Ok(false);
        };
        let batch = members.into_iter().map(|(a, _)| a).collect::<Vec<_>>();
        let mut context = self.wake_decision_context_for_incident(&request).await?;
        // A re-admission (after a failed or imported turn) must not replay the
        // earlier admission of the same digest: name the turns it replaces.
        let mut parts = Vec::with_capacity(batch.len());
        for attention in &batch {
            let prior: Vec<String> = self.db.prior_blocker_turn_ids(&attention.id).await?;
            let digest = wake_attention_incident_digest(attention);
            parts.push(if prior.is_empty() {
                digest
            } else {
                format!("{digest}@{}", prior.join(","))
            });
        }
        let combined = parts.join(":");
        context.incident_digest = Some(wake_incident_digest(&combined, None));
        context.turn = self
            .wake_turns
            .prepare_attention(
                &first,
                &wake_admitted_dedupe_key(&request, &context),
                request.causation_id.as_deref(),
                request.reaction_depth,
                &batch,
            )
            .await?
            .map(Arc::new);
        if context.turn.is_none() {
            // Setup is an incident decision, not a turn admission. The event
            // projection records each member's canonical digest; retain that
            // identity when the startup sweep finds several unconfigured
            // blockers together. A batch digest would create another decision
            // for the first member even though no incident or setup changed.
            for attention in &batch {
                let request = self.request_for_attention(attention, now).await?;
                let context = self.wake_decision_context_for_incident(&request).await?;
                let mut tx = db::begin_immediate(self.db.pool()).await?;
                self.admit_wake_in_tx(&mut tx, &request, &context).await?;
                tx.commit().await?;
            }
            return Ok(false);
        }
        context.batch = batch;
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let outcome = self.admit_wake_in_tx(&mut tx, &request, &context).await?;
        tx.commit().await?;
        if let Some((scope_type, scope_id)) = outcome.stall_scope {
            self.publish_autonomy_stall(
                &scope_type,
                &scope_id,
                "the Project Agent's hourly wake budget is exhausted",
            )
            .await;
        }
        Ok(matches!(
            outcome.result,
            WakeAdmissionResult::Admitted { .. }
        ))
    }

    async fn request_for_attention(
        &self,
        attention: &AttentionProjection,
        now: &str,
    ) -> Result<WakeAdmissionRequest> {
        let event = self.db.get_event(&attention.source_event_id).await?;
        let identity = if attention.scope_type == "project" {
            db::ProjectAgentBindingRepo::get_active_project_binding(&*self.db, &attention.scope_id)
                .await?
                .and_then(|b| b.identity_id)
        } else {
            attention.identity_id.clone()
        };
        Ok(WakeAdmissionRequest {
            identity_id: identity.unwrap_or_default(),
            scope_type: attention.scope_type.clone(),
            scope_id: attention.scope_id.clone(),
            incident_key: attention.dedupe_key.clone(),
            lease_owner: new_uuid_v4(),
            correlation_id: event
                .as_ref()
                .map(|e| e.correlation_id.clone())
                .unwrap_or_else(|| attention.id.clone()),
            causation_id: Some(attention.source_event_id.clone()),
            caused_by_identity_id: event
                .as_ref()
                .filter(|e| e.actor_type == "agent")
                .and_then(|e| e.actor_id.clone()),
            reaction_depth: event.as_ref().map_or(0, |e| e.causation_depth),
            now: now.to_owned(),
            lease_seconds: WAKE_LEASE_SECONDS,
            cooldown_seconds: WAKE_COOLDOWN_SECONDS,
        })
    }

    pub(super) async fn wake_decision_context_for_incident(
        &self,
        request: &WakeAdmissionRequest,
    ) -> Result<WakeDecisionContext> {
        let row = self.db.wake_incident_context(&request.incident_key).await?;
        let Some(row) = row else {
            return Ok(WakeDecisionContext::default());
        };
        let id: String = row.id?;
        let attention_type: String = row.attention_type?;
        let scope_type: String = row.scope_type?;
        let scope_id: String = row.scope_id?;
        let status: String = row.status?;
        let source_event_id: String = row.source_event_id?;
        let source_sequence: Option<i64> = row.source_sequence?;
        let details_json: String = row.details_json?;
        let recommended_action: String = row.recommended_action?;
        let version: i64 = row.version?;
        let details = serde_json::from_str::<Value>(&details_json).unwrap_or(Value::Null);
        let requires_current_task_intervention =
            matches!(
                attention_type.as_str(),
                "execution_failed" | "review_needs_owner"
            ) && details.get("source_event_type").and_then(Value::as_str)
                == Some("task.interruption_changed")
                && details
                    .pointer("/recovery/requires_intervention")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
        let task_id = requires_current_task_intervention
            .then(|| {
                details
                    .get("entity_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .flatten();
        let orphan_execution_id = if attention_type == "execution_failed"
            && matches!(
                details.get("source_event_type").and_then(Value::as_str),
                Some("execution.failed" | "execution.cancelled")
            ) {
            DomainEventRepo::get_event(&*self.db, &source_event_id)
                .await?
                .and_then(|event| serde_json::from_str::<Value>(&event.payload_json).ok())
                .and_then(|payload| {
                    payload
                        .get("execution_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
        } else {
            None
        };
        Ok(WakeDecisionContext {
            attention_id: Some(id),
            source_event_id: Some(source_event_id.clone()),
            incident_digest: Some(wake_attention_state_digest(
                &attention_type,
                &scope_type,
                &scope_id,
                &status,
                &source_event_id,
                source_sequence,
                &details_json,
                &recommended_action,
                version,
            )),
            attention_status: Some(status),
            attention_version: Some(version),
            task_id,
            requires_current_task_intervention,
            orphan_execution_id,
            turn: None,
            batch: Vec::new(),
        })
    }
}
