//! Expired and parked chat turn recovery.

use super::*;

impl AgentChatTurnWorker {
    pub(super) async fn recover_expired(&self) -> Result<()> {
        let now = now_rfc3339();
        let failure_json = serde_json::to_string(&TurnFailure::Unclassified)
            .map_err(|error| ServiceError::Domain(error.to_string()))?;
        let expired = self.db.expired_chat_turn_leases(&now).await?;
        let decision_time = Utc::now();
        for row in expired {
            let id: String = row.id()?;
            let attempt_count: i64 = row.attempt_count()?;
            let max_attempts: i64 = row.max_attempts()?;
            let refunds: i64 = row.lease_refund_count()?;
            let version: i64 = row.version()?;
            let Some(decision) = crate::agent_chat_turn_policy::recover_expired(
                api_types::AgentChatTurnStatus::Leased,
                Some(decision_time - ChronoDuration::seconds(1)),
                attempt_count,
                max_attempts,
                refunds,
                decision_time,
            ) else {
                continue;
            };
            let refunded = refunds < crate::agent_chat_turn_policy::MAX_LEASE_REFUNDS;
            let status = match decision.status {
                api_types::AgentChatTurnStatus::Failed => "failed",
                _ => "retry_wait",
            };
            let updated = async {
                let mut transaction = db::begin_immediate(self.db.pool()).await?;
                let updated = self
                    .db
                    .recover_expired_chat_turn_lease_in_tx(
                        &mut transaction,
                        db::RecoverExpiredChatTurnLeaseInTxInput {
                            status,
                            next_attempt_at: decision.next_attempt_at.map(|at| at.to_rfc3339()),
                            error_message: "Agent Chat lease expired",
                            failure_json: &failure_json,
                            retry_decision: decision.retry_decision.as_str(),
                            attempt_count: decision.attempt_count,
                            refund_increment: i64::from(refunded),
                            updated_at: &now,
                            turn_id: &id,
                            version,
                            expired_at: &now,
                        },
                    )
                    .await?;
                if updated.rows_affected() != 0 {
                    self.db
                        .append_agent_chat_turn_status_in_tx(&mut transaction, &id)
                        .await?;
                }
                transaction.commit().await?;
                Ok::<_, ServiceError>(updated)
            }
            .await?;
            if updated.rows_affected() == 1 {
                // A lease expiry is the recovery boundary for an invocation
                // whose provider result is no longer replayable. Preserve an
                // explicit coverage gap instead of allowing the next retry's
                // fresh attempt to absorb the old provider call.
                for invocation in
                    UsageLedgerRepo::list_usage_invocations_for_source(&*self.db, &id).await?
                {
                    if matches!(
                        invocation.lifecycle,
                        UsageInvocationLifecycle::Started
                            | UsageInvocationLifecycle::PendingSettlement
                    ) {
                        let _ = UsageLedgerRepo::mark_usage_invocation_unsettled(
                            &*self.db,
                            db::MarkUsageInvocationUnsettled {
                                id: invocation.id,
                                expected_version: invocation.version,
                                terminal_reason: "chat lease expired without replayable result"
                                    .to_owned(),
                                settled_at: now.clone(),
                                updated_at: now.clone(),
                            },
                        )
                        .await;
                    }
                }
            }
        }
        self.recover_parked().await?;
        Ok(())
    }

    async fn recover_parked(&self) -> Result<()> {
        let now = now_rfc3339();
        let failure_json = serde_json::to_string(&TurnFailure::Unclassified)
            .map_err(|error| ServiceError::Domain(error.to_string()))?;
        let parked_rows = self.db.parked_chat_turn_interactions().await?;

        for row in parked_rows {
            let id: String = row.id()?;
            let pending_interaction_id: Option<String> = row.pending_interaction_id()?;
            let version: i64 = row.version()?;

            let Some(interaction_id) = pending_interaction_id else {
                let _ = async {
                    let mut transaction = db::begin_immediate(self.db.pool()).await?;
                    let updated = self
                        .db
                        .fail_chat_turn_without_interaction_in_tx(
                            &mut transaction,
                            db::FailChatTurnWithoutInteractionInTxInput {
                                failure_json: &failure_json,
                                retry_decision: api_types::TurnRetryDecision::Fail.as_str(),
                                updated_at: &now,
                                turn_id: &id,
                                version,
                            },
                        )
                        .await?;
                    if updated.rows_affected() != 0 {
                        self.db
                            .append_agent_chat_turn_status_in_tx(&mut transaction, &id)
                            .await?;
                    }
                    transaction.commit().await?;
                    Ok::<_, ServiceError>(updated)
                }
                .await;
                continue;
            };

            let interaction = self
                .db
                .chat_turn_protected_interaction(&interaction_id)
                .await?;

            match interaction {
                None => {
                    let _ = async {
                        let mut transaction = db::begin_immediate(self.db.pool()).await?;
                        let updated = self
                            .db
                            .fail_chat_turn_missing_interaction_in_tx(
                                &mut transaction,
                                db::FailChatTurnMissingInteractionInTxInput {
                                    failure_json: &failure_json,
                                    retry_decision: api_types::TurnRetryDecision::Fail.as_str(),
                                    updated_at: &now,
                                    turn_id: &id,
                                    version,
                                },
                            )
                            .await?;
                        if updated.rows_affected() != 0 {
                            self.db
                                .append_agent_chat_turn_status_in_tx(&mut transaction, &id)
                                .await?;
                        }
                        transaction.commit().await?;
                        Ok::<_, ServiceError>(updated)
                    }
                    .await;
                }
                Some(irow) => {
                    let istatus: String = irow.status()?;
                    let expires_at: Option<String> = irow.expires_at()?;
                    if istatus == "answered" {
                        let _ = async {
                            let mut transaction = db::begin_immediate(self.db.pool()).await?;
                            let updated = self
                                .db
                                .resume_chat_turn_answered_interaction_in_tx(
                                    &mut transaction,
                                    &now,
                                    &id,
                                    version,
                                )
                                .await?;
                            if updated.rows_affected() != 0 {
                                self.db
                                    .append_agent_chat_turn_status_in_tx(&mut transaction, &id)
                                    .await?;
                            }
                            transaction.commit().await?;
                            Ok::<_, ServiceError>(updated)
                        }
                        .await;
                    } else if istatus == "cancelled" {
                        let _ = async {
                            let mut transaction = db::begin_immediate(self.db.pool()).await?;
                            let updated = self
                                .db
                                .cancel_chat_turn_cancelled_interaction_in_tx(
                                    &mut transaction,
                                    &now,
                                    &id,
                                    version,
                                )
                                .await?;
                            if updated.rows_affected() != 0 {
                                self.db
                                    .append_agent_chat_turn_status_in_tx(&mut transaction, &id)
                                    .await?;
                            }
                            transaction.commit().await?;
                            Ok::<_, ServiceError>(updated)
                        }
                        .await;
                    } else if istatus == "expired"
                        || (expires_at.as_deref().is_some()
                            && expires_at.as_deref().unwrap() <= now.as_str())
                    {
                        let _ = async {
                            let mut transaction = db::begin_immediate(self.db.pool()).await?;
                            let updated = self
                                .db
                                .fail_chat_turn_expired_interaction_in_tx(
                                    &mut transaction,
                                    db::FailChatTurnExpiredInteractionInTxInput {
                                        failure_json: &failure_json,
                                        retry_decision: api_types::TurnRetryDecision::Fail.as_str(),
                                        updated_at: &now,
                                        turn_id: &id,
                                        version,
                                    },
                                )
                                .await?;
                            if updated.rows_affected() != 0 {
                                self.db
                                    .append_agent_chat_turn_status_in_tx(&mut transaction, &id)
                                    .await?;
                            }
                            transaction.commit().await?;
                            Ok::<_, ServiceError>(updated)
                        }
                        .await;
                    }
                }
            }
        }
        Ok(())
    }
}
