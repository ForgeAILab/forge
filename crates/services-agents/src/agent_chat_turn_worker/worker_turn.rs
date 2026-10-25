//! Claimed chat turn processing, settlement, and lease renewal.

use super::*;

impl AgentChatTurnWorker {
    /// Re-run the immutable responder/scope checks before creating an
    /// accounting invocation.  A forged or partially-corrupted queued row
    /// must fail closed without allowing its chat/account/project lookup to
    /// become a pricing write authority.
    async fn validate_usage_admission_authority(&self, job: &AgentChatTurnJob) -> Result<()> {
        // The production runner authenticates the immutable responder/binding
        // snapshot before this helper performs any lookup used by pricing
        // admission.  Keeping this call first also gives forged jobs a
        // fail-closed boundary when a runner has a stronger authority source.
        self.runner.validate_admission_authority(job).await?;
        if job.canonical_scope_type != "agent_chat" || job.canonical_scope_id != job.chat_id {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn has a mismatched canonical scope",
            ));
        }
        let input =
            AgentChatMessageRepo::get_agent_chat_message(&*self.db, &job.triggering_message_id)
                .await?
                .filter(|message| message.chat_id == job.chat_id)
                .ok_or_else(|| {
                    ServiceError::not_found("agent_chat_message", job.triggering_message_id.clone())
                })?;
        let chat = AgentChatRepo::get_agent_chat(&*self.db, &job.chat_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_chat", job.chat_id.clone()))?;
        let identity_id = job
            .responder_identity_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("Agent Chat job has no responder"))?;
        let agent = AgentRepo::get_by_id(&*self.db, identity_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_identity", identity_id.to_owned()))?;
        let profile_id = job
            .profile_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("Agent Chat job has no profile"))?;
        let profile = AgentProfileRepo::get_profile(&*self.db, profile_id)
            .await?
            .filter(|profile| profile.identity_id == agent.id)
            .ok_or_else(|| ServiceError::not_found("agent_profile", profile_id.to_owned()))?;
        if input.chat_id != chat.id
            || agent.owner_id.is_none()
            || profile.identity_id != agent.id
            || job.responder_identity_id.as_deref() != Some(agent.id.as_str())
            || job.profile_id.as_deref() != Some(profile.id.as_str())
        {
            return Err(ServiceError::invalid_operation(
                "Agent Chat responder provenance is invalid",
            ));
        }
        match chat.kind.as_str() {
            "account_main" => {
                if chat.project_id.is_some() {
                    return Err(ServiceError::invalid_operation(
                        "Main Agent Chat unexpectedly has a Project scope",
                    ));
                }
                let account_id = chat.account_id.as_deref().ok_or_else(|| {
                    ServiceError::invalid_operation("Main Agent Chat has no account scope")
                })?;
                if agent.owner_id.as_deref() != Some(account_id) {
                    return Err(ServiceError::invalid_operation(
                        "Main Agent identity is not owned by the Chat account",
                    ));
                }
            }
            "project" => {
                if chat.account_id.is_some() {
                    return Err(ServiceError::invalid_operation(
                        "Project Agent Chat unexpectedly has an account scope",
                    ));
                }
                let project_id = chat.project_id.as_deref().ok_or_else(|| {
                    ServiceError::invalid_operation("Project Agent Chat has no Project scope")
                })?;
                let project = ProjectRepo::get_by_id(&*self.db, project_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
                if project.owner_id.is_none()
                    || agent.owner_id.as_deref() != project.owner_id.as_deref()
                {
                    return Err(ServiceError::invalid_operation(
                        "Project Agent identity is not owned by the Project account",
                    ));
                }
            }
            _ => {
                return Err(ServiceError::invalid_operation(
                    "usage admission requires a canonical Agent Chat",
                ));
            }
        }
        Ok(())
    }

    async fn release_shutdown_lease(&self, job: &AgentChatTurnJob) -> Result<()> {
        async {
            let mut transaction = db::begin_immediate(self.db.pool()).await?;
            let updated = self
                .db
                .release_shutdown_chat_turn_lease_in_tx(
                    &mut transaction,
                    now_rfc3339(),
                    &job.id,
                    job.version,
                    &self.lease_owner,
                )
                .await?;
            if updated.rows_affected() != 0 {
                self.db
                    .append_agent_chat_turn_status_in_tx(&mut transaction, &job.id)
                    .await?;
            }
            transaction.commit().await?;
            Ok::<_, ServiceError>(updated)
        }
        .await?;
        Ok(())
    }

    pub(super) async fn process_claimed(
        &self,
        job: AgentChatTurnJob,
        cancellation: CancellationToken,
    ) {
        // RetryWait is a retry of this same logical admission, not a new turn.
        // Keep the claimed job (including its frozen responder/Profile/policy
        // provenance) as the runner input; claim/lease bookkeeping may change
        // status, attempt count, and version, but must never re-resolve the
        // current binding or Profile here.
        if cancellation.is_cancelled() {
            let _ = self.release_shutdown_lease(&job).await;
            return;
        }
        // A deferred capacity window has its own deadline, independent of
        // charged attempts. Expiry must not make one more provider call.
        let capacity_expired = job
            .usage_limit_first_deferred_at
            .as_deref()
            .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
            .is_some_and(|first| Utc::now() >= first + ChronoDuration::hours(24));
        if matches!(job.failure_class, Some(TurnFailure::UsageLimit { .. }))
            && (capacity_expired
                || job.usage_limit_deferral_count
                    >= crate::agent_chat_turn_policy::MAX_USAGE_LIMIT_DEFERRALS)
        {
            let _ = self
                .chat_service
                .append_failure(
                    &job,
                    &self.lease_owner,
                    job.failure_class
                        .as_ref()
                        .expect("usage-limit evidence checked"),
                    "Agent Chat usage-limit deferral budget exhausted",
                )
                .await;
            return;
        }
        if let Err(error) = self.validate_usage_admission_authority(&job).await {
            tracing::warn!(job_id = %job.id, error = %error, "Agent Chat authority validation failed before usage admission");
            let _ = self
                .chat_service
                .append_failure(
                    &job,
                    &self.lease_owner,
                    &match &error {
                        ServiceError::AgentPaused { .. }
                        | ServiceError::ProjectPaused { .. }
                        | ServiceError::Db(
                            db::DbError::AgentPaused { .. } | db::DbError::ProjectPaused { .. },
                        ) => TurnFailure::Authority,
                        _ => TurnFailure::PreProviderAdmission,
                    },
                    "Agent Chat authority could not be verified before provider admission",
                )
                .await;
            return;
        }
        if let Err(error) = self.runner.validate_provider_availability(&job).await {
            tracing::warn!(job_id = %job.id, error = %error, "Agent Chat provider unavailable before usage admission");
            let _ =
                self.chat_service
                    .append_failure(
                        &job,
                        &self.lease_owner,
                        &match &error {
                            ServiceError::InvalidOperation { .. }
                            | ServiceError::NotFound { .. } => TurnFailure::Configuration,
                            ServiceError::AgentPaused { .. }
                            | ServiceError::ProjectPaused { .. }
                            | ServiceError::Db(
                                db::DbError::AgentPaused { .. } | db::DbError::ProjectPaused { .. },
                            ) => TurnFailure::Authority,
                            _ => TurnFailure::PreProviderAdmission,
                        },
                        "Agent Chat provider is unavailable before provider admission",
                    )
                    .await;
            return;
        }
        let admission = match crate::chat_usage::admit_chat_usage(&self.db, &job).await {
            Ok(invocation) => invocation,
            Err(error) => {
                tracing::warn!(job_id = %job.id, error = %error, "Agent Chat usage admission failed");
                let _ = self
                    .chat_service
                    .append_failure(
                        &job,
                        &self.lease_owner,
                        &TurnFailure::PreProviderAdmission,
                        "Agent Chat usage admission could not be committed",
                    )
                    .await;
                return;
            }
        };
        if matches!(
            admission.lifecycle,
            db::UsageInvocationLifecycle::Settled | db::UsageInvocationLifecycle::Unsettled
        ) {
            tracing::warn!(job_id = %job.id, "Agent Chat provider invocation is already terminal");
            return;
        }
        let stop = CancellationToken::new();
        let turn_cancellation = cancellation.child_token();
        // Hard aborts and panics cannot reach stop.cancel() below. Both tokens
        // follow the turn future's lifetime, so its driver and renewer stop too.
        let _stop_renewal_on_drop = stop.clone().drop_guard();
        let _cancel_turn_on_drop = turn_cancellation.clone().drop_guard();
        let renewal =
            self.spawn_lease_renewal(job.id.clone(), stop.clone(), turn_cancellation.clone());
        let baseline_turn = job
            .operating_skill_revision_id
            .as_deref()
            .is_some_and(|revision| {
                canonical_main_baseline_operating_skill_body_for_revision(revision).is_some()
            });
        let mut control_transfer = None;
        let mut result = None;
        if baseline_turn {
            let run = self
                .runner
                .run_turn_with_usage(&job, turn_cancellation.clone());
            tokio::pin!(run);
            tokio::select! {
                completed = &mut run => result = Some(completed),
                    transfer = self.wait_for_genesis_control_transfer(&job.id) => {
                        control_transfer = Some(transfer);
                        // The typed command has already committed the durable
                        // continuation. Stop the baseline provider loop so it
                        // cannot add a redundant conversational response.
                        turn_cancellation.cancel();
                        if let Ok(outcome) = tokio::time::timeout(Duration::from_secs(5), &mut run).await {
                            result = Some(outcome);
                        }
                    }
            }
        } else {
            result = Some(
                self.runner
                    .run_turn_with_usage(&job, turn_cancellation)
                    .await,
            );
        }
        stop.cancel();
        let _ = renewal.await;
        let provider_outcome = match result.as_ref() {
            Some(AgentChatTurnRunOutcome::Completed { .. }) => Some(Ok(())),
            Some(AgentChatTurnRunOutcome::Failed { failure, .. }) => Some(Err(failure)),
            None => None,
        };
        if let (Some(outcome), Some(profile)) = (provider_outcome, job.profile_id.as_deref()) {
            let resume_at = outcome.err().and_then(|failure| {
                crate::agent_chat_turn_policy::failure_after_claim(
                    failure,
                    job.attempt_count,
                    job.max_attempts,
                    job.pre_provider_failure_count,
                    crate::agent_chat_turn_policy::UsageLimitDeferrals {
                        count: job.usage_limit_deferral_count,
                        first_at: job
                            .usage_limit_first_deferred_at
                            .as_deref()
                            .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
                            .map(|at| at.with_timezone(&Utc)),
                        fail_fast: crate::project_escalation::is_autonomous_wake_turn(&job),
                    },
                    Utc::now(),
                    "",
                )
                .next_attempt_at
            });
            if !cancellation.is_cancelled() {
                if let Err(error) = crate::provider_health::record_chat_outcome(
                    &self.db, profile, outcome, resume_at,
                )
                .await
                {
                    tracing::warn!(job_id = %job.id, error = %error, "provider health could not be recorded");
                }
            }
        }
        // Renewal is versioned. Re-read after the backend stops so a long
        // native turn commits against the current lease version rather than
        // the snapshot that was originally claimed.
        let commit_job = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, &job.id)
            .await
            .ok()
            .flatten()
            .unwrap_or(job.clone());
        if cancellation.is_cancelled() && commit_job.status == db::AgentChatTurnState::Leased {
            if let Some(
                AgentChatTurnRunOutcome::Completed { usage_reports, .. }
                | AgentChatTurnRunOutcome::Failed { usage_reports, .. },
            ) = result.as_ref()
            {
                if let Err(error) = crate::chat_usage::settle_late_chat_usage(
                    &self.db,
                    &job.id,
                    usage_reports,
                    &now_rfc3339(),
                )
                .await
                {
                    tracing::warn!(job_id = %job.id, %error, "shutdown usage settlement failed");
                }
            }
            let _ = self.release_shutdown_lease(&commit_job).await;
            return;
        }
        if baseline_turn && control_transfer.is_none() {
            control_transfer = self
                .find_genesis_control_transfer(&commit_job.id)
                .await
                .ok()
                .flatten();
        }
        if let Some(transfer) = control_transfer {
            let (provider_finished, usage_reports) = match result.take() {
                Some(AgentChatTurnRunOutcome::Completed { usage_reports, .. })
                | Some(AgentChatTurnRunOutcome::Failed { usage_reports, .. }) => {
                    (true, usage_reports)
                }
                None => (false, Vec::new()),
            };
            let settlements = if provider_finished {
                match crate::chat_usage::build_chat_usage_settlements(
                    &self.db,
                    &commit_job.id,
                    &usage_reports,
                    &now_rfc3339(),
                )
                .await
                {
                    Ok(settlements) => settlements,
                    Err(error) => {
                        let _ = self
                            .settle_reports_if_cancelled(&commit_job.id, &usage_reports)
                            .await;
                        tracing::warn!(job_id = %commit_job.id, error = %error, "Genesis control-transfer usage settlement preparation failed");
                        return;
                    }
                }
            } else {
                Vec::new()
            };
            if let Err(error) =
                AgentChatTransactionRepo::complete_agent_chat_control_transfer_with_usage(
                    &*self.db,
                    db::CompleteAgentChatControlTransferWithUsage {
                        terminal: CompleteAgentChatControlTransfer {
                            turn_job_id: commit_job.id.clone(),
                            expected_version: commit_job.version,
                            lease_owner: self.lease_owner.clone(),
                            command_receipt_id: transfer.command_receipt_id,
                            continuation_turn_id: transfer.continuation_turn_id,
                            genesis_session_id: transfer.genesis_session_id,
                            updated_at: now_rfc3339(),
                        },
                        settlements,
                        provider_finished,
                    },
                )
                .await
            {
                // Cancellation can win after the baseline provider observes
                // the Genesis handoff but before this composite acquires the
                // turn CAS.  The cancelled terminal state must remain the
                // authority; drain any reports that were already observed
                // without attempting to rewrite it.
                let _ = self
                    .settle_reports_if_cancelled(&commit_job.id, &usage_reports)
                    .await;
                tracing::warn!(job_id = %commit_job.id, error = %error, "Agent Chat Genesis control transfer commit failed");
            }
            return;
        }
        let Some(result) = result else {
            tracing::warn!(job_id = %commit_job.id, "Agent Chat runner stopped without a Genesis control transfer");
            return;
        };
        match result {
            AgentChatTurnRunOutcome::Completed {
                turn,
                usage_reports,
            } => {
                if commit_job.status == db::AgentChatTurnState::Cancelled {
                    let _ = self
                        .settle_reports_if_cancelled(&commit_job.id, &usage_reports)
                        .await;
                    return;
                }
                let settlements = match crate::chat_usage::build_chat_usage_settlements(
                    &self.db,
                    &commit_job.id,
                    &usage_reports,
                    &now_rfc3339(),
                )
                .await
                {
                    Ok(settlements) => settlements,
                    Err(error) => {
                        let _ = self
                            .settle_reports_if_cancelled(&commit_job.id, &usage_reports)
                            .await;
                        tracing::warn!(job_id = %commit_job.id, error = %error, "Agent Chat usage settlement preparation failed");
                        return;
                    }
                };
                if let Some(pending_interaction_id) = turn.pending_interaction_id {
                    if let Err(error) = self
                        .chat_service
                        .append_awaiting_input_with_usage(
                            &commit_job,
                            &self.lease_owner,
                            &pending_interaction_id,
                            settlements.clone(),
                        )
                        .await
                    {
                        tracing::warn!(job_id = %commit_job.id, error = %error, "Agent Chat awaiting input commit failed");
                        if self
                            .settle_reports_if_cancelled(&commit_job.id, &usage_reports)
                            .await
                        {
                            return;
                        }
                        if let Err(fallback_error) = self
                            .chat_service
                            .append_failure_with_usage(
                                &commit_job,
                                &self.lease_owner,
                                &TurnFailure::Unclassified,
                                "Agent Chat awaiting input state could not be committed",
                                settlements.clone(),
                            )
                            .await
                        {
                            let _ = self
                                .settle_reports_if_cancelled(&commit_job.id, &usage_reports)
                                .await;
                            tracing::warn!(job_id = %commit_job.id, error = %fallback_error, "Agent Chat awaiting input fallback failure commit failed");
                        }
                    }
                } else if turn.content.trim().is_empty() {
                    tracing::warn!(job_id = %commit_job.id, "Agent Chat provider returned no text response");
                    if let Err(error) = self
                        .chat_service
                        .append_failure_with_usage(
                            &commit_job,
                            &self.lease_owner,
                            &TurnFailure::EmptyResponse,
                            "Agent returned no text response",
                            settlements.clone(),
                        )
                        .await
                    {
                        let _ = self
                            .settle_reports_if_cancelled(&commit_job.id, &usage_reports)
                            .await;
                        tracing::warn!(job_id = %commit_job.id, error = %error, "Agent Chat empty-response failure commit failed");
                    }
                } else {
                    match self.turn_postcondition_satisfied(&commit_job).await {
                        Ok(true) => {
                            if let Err(error) = self
                                .commit_success(&commit_job, turn, settlements.clone())
                                .await
                            {
                                tracing::warn!(job_id = %commit_job.id, error = %error, "Agent Chat response commit failed");
                                if self
                                    .settle_reports_if_cancelled(&commit_job.id, &usage_reports)
                                    .await
                                {
                                    return;
                                }
                                if let Err(fallback_error) = self
                                    .chat_service
                                    .append_failure_with_usage(
                                        &commit_job,
                                        &self.lease_owner,
                                        &TurnFailure::Unclassified,
                                        "Agent Chat response could not be committed",
                                        settlements.clone(),
                                    )
                                    .await
                                {
                                    let _ = self
                                        .settle_reports_if_cancelled(&commit_job.id, &usage_reports)
                                        .await;
                                    tracing::warn!(job_id = %commit_job.id, error = %fallback_error, "Agent Chat response fallback failure commit failed");
                                }
                            }
                        }
                        Ok(false) => {
                            tracing::warn!(
                                job_id = %commit_job.id,
                                "Agent Chat delivery follow-up postcondition was not committed"
                            );
                            if let Err(error) = self
                                .chat_service
                                .append_failure_with_usage(
                                    &commit_job,
                                    &self.lease_owner,
                                    &TurnFailure::PostconditionUnmet {
                                        event: self
                                            .required_postcondition_event(&commit_job)
                                            .await
                                            .unwrap_or_default(),
                                    },
                                    DELIVERY_FOLLOWUP_POSTCONDITION_MESSAGE,
                                    settlements.clone(),
                                )
                                .await
                            {
                                let _ = self
                                    .settle_reports_if_cancelled(&commit_job.id, &usage_reports)
                                    .await;
                                tracing::warn!(job_id = %commit_job.id, error = %error, "Agent Chat delivery postcondition failure commit failed");
                            }
                        }
                        Err(error) => {
                            tracing::warn!(job_id = %commit_job.id, error = %error, "Agent Chat turn postcondition check failed");
                            if let Err(commit_error) = self
                                .chat_service
                                .append_failure_with_usage(
                                    &commit_job,
                                    &self.lease_owner,
                                    &TurnFailure::Unclassified,
                                    "Agent Chat turn postcondition could not be verified",
                                    settlements.clone(),
                                )
                                .await
                            {
                                let _ = self
                                    .settle_reports_if_cancelled(&commit_job.id, &usage_reports)
                                    .await;
                                tracing::warn!(job_id = %commit_job.id, error = %commit_error, "Agent Chat postcondition failure commit failed");
                            }
                        }
                    }
                }
            }
            AgentChatTurnRunOutcome::Failed {
                failure,
                error,
                usage_reports,
            } => {
                if commit_job.status == db::AgentChatTurnState::Cancelled {
                    let _ = self
                        .settle_reports_if_cancelled(&commit_job.id, &usage_reports)
                        .await;
                    return;
                }
                let message = bounded_error_message(&error.to_string());
                let settlements = match crate::chat_usage::build_chat_usage_settlements(
                    &self.db,
                    &commit_job.id,
                    &usage_reports,
                    &now_rfc3339(),
                )
                .await
                {
                    Ok(settlements) => settlements,
                    Err(error) => {
                        let _ = self
                            .settle_reports_if_cancelled(&commit_job.id, &usage_reports)
                            .await;
                        tracing::warn!(job_id = %commit_job.id, error = %error, "Agent Chat failure usage settlement preparation failed");
                        return;
                    }
                };
                match self
                    .chat_service
                    .append_failure_with_usage(
                        &commit_job,
                        &self.lease_owner,
                        &failure,
                        &message,
                        settlements,
                    )
                    .await
                {
                    Ok(failed) => {
                        // A database error has no failure class of its own,
                        // so the stored turn says only `backend_failed`.
                        if failed.status == db::AgentChatTurnState::Failed
                            && matches!(error, ServiceError::Db(_))
                        {
                            tracing::error!(chat_id = %commit_job.chat_id, job_id = %commit_job.id, failure_code = failure.code(), error = %error, "Agent Chat turn failed terminally on a database error");
                        }
                    }
                    Err(commit_error) => {
                        let _ = self
                            .settle_reports_if_cancelled(&commit_job.id, &usage_reports)
                            .await;
                        tracing::warn!(job_id = %commit_job.id, error = %commit_error, "Agent Chat failure could not be persisted");
                    }
                }
            }
        }
        if let Err(error) =
            crate::project_escalation::ProjectEscalationService::new(Arc::clone(&self.db))
                .after_wake_turn(&commit_job.id)
                .await
        {
            tracing::warn!(turn_id=%commit_job.id,%error,"wake turn follow-up will be reconsidered by sweep");
        }
    }

    async fn settle_reports_if_cancelled(
        &self,
        source_id: &str,
        reports: &[executors::UsageReport],
    ) -> bool {
        let cancelled = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, source_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|job| job.status == db::AgentChatTurnState::Cancelled);
        if !cancelled {
            return false;
        }
        if let Err(error) =
            crate::chat_usage::settle_late_chat_usage(&self.db, source_id, reports, &now_rfc3339())
                .await
        {
            tracing::warn!(source_id, error = %error, "cancelled Agent Chat usage late drain failed");
        }
        true
    }

    async fn wait_for_genesis_control_transfer(
        &self,
        source_turn_id: &str,
    ) -> GenesisControlTransfer {
        let mut interval = tokio::time::interval(Duration::from_millis(25));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            match self.find_genesis_control_transfer(source_turn_id).await {
                Ok(Some(transfer)) => return transfer,
                Ok(None) => {}
                Err(error) => tracing::debug!(
                    turn_id = source_turn_id,
                    error = %error,
                    "Genesis control-transfer watch read failed"
                ),
            }
        }
    }

    async fn find_genesis_control_transfer(
        &self,
        source_turn_id: &str,
    ) -> Result<Option<GenesisControlTransfer>> {
        let row = self
            .db
            .genesis_control_transfer_receipt(MAIN_GENESIS_START_OPERATION, source_turn_id)
            .await?;
        row.map(|row| {
            Ok(GenesisControlTransfer {
                command_receipt_id: row.id()?,
                continuation_turn_id: row.continuation_turn_id()?,
                genesis_session_id: row.genesis_session_id()?,
            })
        })
        .transpose()
    }

    async fn commit_success(
        &self,
        job: &AgentChatTurnJob,
        turn: CompletedAgentChatTurn,
        settlements: Vec<db::UsageLedgerSettlement>,
    ) -> Result<CommittedAgentChatResponse> {
        self.chat_service
            .append_success_with_usage(
                job,
                &self.lease_owner,
                AppendAgentChatSuccessInput {
                    content: turn.content,
                    model: turn.model,
                    session_id: Some(turn.session_id),
                    context_manifest_id: turn.context_manifest_id,
                    // The typed ledger is the accounting authority for newly
                    // admitted turns.  Do not copy the legacy JSON payload
                    // back into the message row, even when a runner/test
                    // supplies one on the internal completion value.
                    token_usage_json: None,
                    duration_ms: Some(turn.duration_ms),
                },
                settlements,
            )
            .await
    }

    async fn required_postcondition_event(&self, job: &AgentChatTurnJob) -> Result<String> {
        let message =
            AgentChatMessageRepo::get_agent_chat_message(&*self.db, &job.triggering_message_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::not_found("agent_chat_message", job.triggering_message_id.clone())
                })?;
        Ok(delivery_followup_postcondition(&message)?
            .map(|condition| condition.required_event_type)
            .unwrap_or_default())
    }

    async fn turn_postcondition_satisfied(&self, job: &AgentChatTurnJob) -> Result<bool> {
        let message =
            AgentChatMessageRepo::get_agent_chat_message(&*self.db, &job.triggering_message_id)
                .await?
                .filter(|message| message.chat_id == job.chat_id)
                .ok_or_else(|| {
                    ServiceError::not_found("agent_chat_message", job.triggering_message_id.clone())
                })?;
        let Some(postcondition) = delivery_followup_postcondition(&message)? else {
            return Ok(true);
        };
        let source_event_id = message.source_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation("Delivery follow-up has no source wake event")
        })?;
        // Attention admits the turn and its audit event together; the event
        // names the turn it admitted. That turn, or a manual retry of it,
        // answers the same triggering message.
        let wake_turn = self
            .db
            .delivery_followup_wake_turn(
                source_event_id,
                postcondition.after_event_sequence,
                &postcondition.required_scope_id,
            )
            .await?;
        let Some(wake_turn) = wake_turn else {
            return Err(ServiceError::invalid_operation(
                "Delivery follow-up postcondition does not match its source wake event",
            ));
        };
        let caused_by_wake = self
            .db
            .delivery_followup_source_turn(wake_turn.as_deref(), &job.triggering_message_id)
            .await?
            .is_some();
        if !caused_by_wake {
            return Err(ServiceError::invalid_operation(
                "Delivery follow-up turn is not caused by its source wake event",
            ));
        }
        let canonical_project_chat = self
            .db
            .delivery_followup_canonical_project_chat(
                &job.chat_id,
                &postcondition.required_scope_id,
            )
            .await?
            .is_some();
        if !canonical_project_chat {
            return Err(ServiceError::invalid_operation(
                "Delivery follow-up postcondition does not match its canonical Project Chat",
            ));
        }
        Ok(self
            .db
            .delivery_followup_postcondition_event(
                &postcondition.required_event_type,
                &postcondition.required_scope_type,
                &postcondition.required_scope_id,
                postcondition.after_event_sequence,
            )
            .await?
            .is_some())
    }

    fn spawn_lease_renewal(
        &self,
        job_id: String,
        stop: CancellationToken,
        turn_cancellation: CancellationToken,
    ) -> JoinHandle<()> {
        let db = Arc::clone(&self.db);
        let owner = self.lease_owner.clone();
        let renew_every = self.lease_renew_interval;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(renew_every);
            interval.tick().await;
            loop {
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => break,
                    _ = interval.tick() => {
                        let now = now_rfc3339();
                        let result = db.renew_chat_turn_worker_lease(lease_deadline(), &now, &job_id, &owner).await;
                        if result.map(|result| result.rows_affected() == 0).unwrap_or(true) {
                            turn_cancellation.cancel();
                            break;
                        }
                    }
                }
            }
        })
    }
}
