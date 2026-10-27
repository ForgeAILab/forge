//! Repository-scoped Solo session reads and commands.
use super::*;

#[derive(Clone)]
pub struct SoloSessionService {
    scope: SoloSessionScope,
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    agent_chat_service: Arc<AgentChatService<SqliteDb>>,
    task_service: Arc<TaskService>,
    turn_logs: AgentChatTurnLogRoot,
    interaction_broker: Option<InteractionBrokerHandle>,
    forge_session_id: Option<String>,
    execution_logs_root: Option<PathBuf>,
}

impl fmt::Debug for SoloSessionService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SoloSessionService")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl SoloSessionService {
    /// Bind one immutable scope to the shared runtime services.
    pub fn new(scope: SoloSessionScope, dependencies: SoloSessionDependencies) -> Result<Self> {
        scope.validate()?;
        Ok(Self {
            scope,
            db: dependencies.db,
            event_bus: dependencies.event_bus,
            agent_chat_service: dependencies.agent_chat_service,
            task_service: dependencies.task_service,
            turn_logs: dependencies.turn_logs,
            interaction_broker: dependencies.interaction_broker,
            forge_session_id: dependencies.forge_session_id,
            execution_logs_root: dependencies.execution_logs_root,
        })
    }

    #[must_use]
    pub fn scope(&self) -> &SoloSessionScope {
        &self.scope
    }

    /// Subscribe to invalidation hints for this Project Chat.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<ForgeEvent> {
        self.event_bus.subscribe()
    }

    /// Read every visible Solo surface from current authoritative records.
    /// Event delivery never substitutes for this read.
    pub async fn refresh(&self) -> Result<SoloSessionSnapshot> {
        self.refresh_with_limits(SoloRefreshLimits::default()).await
    }

    pub async fn refresh_with_limits(
        &self,
        limits: SoloRefreshLimits,
    ) -> Result<SoloSessionSnapshot> {
        let limits = limits.bounded()?;
        let records = self.load_scope_records().await?;
        let (messages, messages_has_more) = self.load_messages(limits.messages).await?;
        let turns = self.load_turns(limits.turns).await?;
        let tasks = self.load_tasks(limits.tasks).await?;
        let attention = self.load_attention(limits.attention).await?;
        let interactions = self.load_interactions(&turns).await?;
        let charter = self
            .load_charter_snapshot(&records.project, records.binding.as_ref())
            .await?;

        let project = SoloProjectSnapshot {
            id: safe_identifier(&records.project.id),
            name: safe_diagnostic(&records.project.name).unwrap_or_else(|| "[redacted]".to_owned()),
            version: records.project.version,
            readiness: project_readiness(&records.project, records.binding.as_ref()),
            paused_at: records
                .project
                .paused_at
                .as_deref()
                .map(|value| safe_text(value, SOLO_MAX_SHORT_TEXT_CHARS)),
            charter_status: safe_identifier(&records.project.charter_status),
            charter_setup_required: records.project.charter_setup_required,
            binding: records.binding.as_ref().map(binding_snapshot),
            charter,
        };

        Ok(SoloSessionSnapshot {
            contract_revision: SOLO_SESSION_CONTRACT_REVISION.to_owned(),
            scope: self.scope.clone(),
            project,
            repository: repository_snapshot(&records.repo),
            chat: chat_snapshot(&records.chat),
            messages,
            messages_has_more,
            turns,
            tasks,
            attention,
            interactions,
            capabilities: self.capabilities(),
            authoritative_at: db::now_rfc3339(),
        })
    }

    /// Admit one user message and one finite turn through the canonical
    /// AgentChatService.  The caller must provide a stable dedupe key so an
    /// interrupted response can safely replay the command.
    pub async fn send_message(&self, input: SoloSendMessageInput) -> Result<SoloSendMessageResult> {
        self.ensure_scope_ready_for_chat().await?;
        bounded_required("message content", &input.content, SOLO_MAX_TEXT_CHARS)?;
        bounded_required("message dedupe_key", &input.dedupe_key, SOLO_MAX_ID_CHARS)?;
        let admitted = self
            .agent_chat_service
            .send_message(SendAgentChatMessageInput {
                actor_user_id: self.scope.owner_id.clone(),
                chat_id: self.scope.project_chat_id.clone(),
                content: input.content,
                dedupe_key: Some(input.dedupe_key),
            })
            .await?;
        Ok(SoloSendMessageResult {
            message: message_snapshot(&admitted.message),
            turn: turn_snapshot(&admitted.turn_job),
        })
    }

    /// Cancel one exact non-terminal turn with the current optimistic version
    /// and a stable cancellation idempotency key.
    pub async fn cancel_turn(
        &self,
        turn_job_id: impl Into<String>,
        expected_version: i64,
        idempotency_key: impl Into<String>,
    ) -> Result<SoloTurnSnapshot> {
        let turn_job_id = bounded_owned("turn_job_id", turn_job_id.into(), SOLO_MAX_ID_CHARS)?;
        let idempotency_key = bounded_owned(
            "turn cancellation idempotency_key",
            idempotency_key.into(),
            SOLO_MAX_ID_CHARS,
        )?;
        if expected_version < 1 {
            return Err(ServiceError::invalid_operation(
                "turn expected_version must be positive",
            ));
        }
        let job = self.authorized_turn(&turn_job_id).await?;
        if turn_snapshot(&job).status.is_terminal() {
            return Err(ServiceError::conflict(
                "Agent Chat turn is already terminal",
            ));
        }
        let cancelled = self
            .agent_chat_service
            .cancel_turn(CancelAgentChatTurnInput {
                actor_user_id: self.scope.owner_id.clone(),
                chat_id: self.scope.project_chat_id.clone(),
                turn_job_id,
                expected_version,
                idempotency_key,
            })
            .await?;
        Ok(turn_snapshot(&cancelled))
    }

    /// Admit a fresh retry through the canonical versioned/idempotent command.
    pub async fn retry_turn(&self, input: SoloRetryTurnInput) -> Result<SoloTurnSnapshot> {
        let (turn_job_id, idempotency_key, expected_version) = validate_retry_input(input)?;
        self.authorized_turn(&turn_job_id).await?;
        let turn = self
            .agent_chat_service
            .retry_turn(RetryAgentChatTurnInput {
                actor_user_id: self.scope.owner_id.clone(),
                chat_id: self.scope.project_chat_id.clone(),
                turn_job_id,
                expected_version,
                idempotency_key,
            })
            .await?;
        Ok(turn_snapshot(&turn))
    }

    /// Answer a protected runtime questionnaire only when the exact pending
    /// interaction is referenced by a turn in this bound Project Chat.
    pub async fn answer_interaction(
        &self,
        input: SoloInteractionAnswerInput,
    ) -> Result<SoloOperationOutcome<SoloInteractionSnapshot>> {
        const OPERATION: &str = "solo.answer_interaction";
        let interaction_id =
            bounded_owned("interaction_id", input.interaction_id, SOLO_MAX_ID_CHARS)?;
        if input.expected_version < 1 {
            return Err(ServiceError::invalid_operation(
                "interaction expected_version must be positive",
            ));
        }
        self.validate_interaction_values(&input.values)?;
        let turns = self.load_turns(SOLO_MAX_TURNS).await?;
        let Some(turn) = turns
            .iter()
            .find(|turn| turn.pending_interaction_id.as_deref() == Some(interaction_id.as_str()))
        else {
            return Err(ServiceError::not_found("agent_interaction", interaction_id));
        };
        if turn.status != SoloTurnStatus::AwaitingInput {
            return Ok(SoloOperationOutcome::not_ready(
                OPERATION,
                "the referenced turn is not awaiting typed input",
            ));
        }
        let Some(broker) = self.interaction_broker.as_ref() else {
            return Ok(SoloOperationOutcome::unsupported(
                OPERATION,
                "the protected runtime interaction broker is not attached",
            ));
        };
        let Some(forge_session_id) = self
            .forge_session_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(SoloOperationOutcome::not_ready(
                OPERATION,
                "the owning Agent Runtime session is not available",
            ));
        };
        let values = input
            .values
            .into_iter()
            .map(interaction_answer_value)
            .collect();
        let answered = broker
            .answer_for_session(
                &self.scope.owner_id,
                forge_session_id,
                InteractionAnswer::new(interaction_id, input.expected_version, values),
            )
            .await
            .map_err(map_interaction_error)?;
        Ok(SoloOperationOutcome::Applied(interaction_snapshot(
            &answered,
        )))
    }

    /// Execute one exact Project Charter adoption/amendment target through the
    /// existing typed command service.  The target is reloaded immediately
    /// before command execution so an open TUI card can never approve a stale
    /// digest, revision, binding, or Project version.
    pub async fn approve_charter(
        &self,
        input: SoloCharterApprovalInput,
    ) -> Result<SoloOperationOutcome<SoloCharterApprovalResult>> {
        const OPERATION: &str = "solo.approve_charter";
        validate_approval_input(&input)?;
        if input.target.project_id != self.scope.project_id {
            return Err(ServiceError::not_found(
                "project",
                input.target.project_id.clone(),
            ));
        }
        let records = self.load_scope_records().await?;
        let current = self
            .load_charter_snapshot(&records.project, records.binding.as_ref())
            .await?
            .and_then(|charter| charter.approval_target);
        let Some(current) = current else {
            return Ok(SoloOperationOutcome::not_ready(
                OPERATION,
                "no exact pending Project Charter approval target is available",
            ));
        };
        if current != input.target {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let authorization_json = serde_json::to_string(&json!({
            "principal_type": "user",
            "principal_id": self.scope.owner_id.clone(),
            "authorization_basis": input.authorization.authorization_basis.clone(),
            "action": PROJECT_CHARTER_APPROVAL_ACTION,
            "event_id": input.authorization.authorization_event_id.clone(),
            "occurred_at": input.authorization.authorization_occurred_at.clone(),
        }))
        .map_err(|_| {
            ServiceError::invalid_operation("Charter approval authorization is invalid")
        })?;
        let authorization = ProjectCommandAuthorization {
            principal_type: "user".to_owned(),
            principal_id: self.scope.owner_id.clone(),
            policy_result: "allowed".to_owned(),
            policy_revision: Some(PROJECT_AGENT_POLICY_REVISION.to_owned()),
            policy_digest: Some(input.target.selected_project_agent_policy_digest.clone()),
            requested_permission: Some(APPROVE_PROJECT_CHARTER_PERMISSION.to_owned()),
            correlation_id: input.idempotency_key.clone(),
            causation_id: None,
            causation_depth: 0,
            authorization_event_id: input.authorization.authorization_event_id.clone(),
            authorization_basis: input.authorization.authorization_basis.clone(),
            authorization_action: PROJECT_CHARTER_APPROVAL_ACTION.to_owned(),
            authorization_occurred_at: input.authorization.authorization_occurred_at.clone(),
            authorization_json,
        };
        let outcome = ProjectCharterCommandService::new(Arc::clone(&self.db))
            .approve(
                ProjectCharterApprovalCommand {
                    project_id: input.target.project_id.clone(),
                    charter_id: input.target.charter_id.clone(),
                    revision_id: input.target.revision_id.clone(),
                    content_digest: input.target.content_digest.clone(),
                    rendered_digest: input.target.rendered_digest.clone(),
                    expected_charter_version: input.target.expected_charter_version,
                    expected_project_version: input.target.expected_project_version,
                    approved_project_name: input.target.approved_project_name.clone(),
                    approved_project_slug: input.target.approved_project_slug.clone(),
                    project_mode: input.target.project_mode.clone(),
                    selected_project_agent_identity_id: input
                        .target
                        .selected_project_agent_identity_id
                        .clone(),
                    selected_project_agent_profile_revision_id: input
                        .target
                        .selected_project_agent_profile_revision_id
                        .clone(),
                    selected_project_agent_operating_skill_revision: input
                        .target
                        .selected_project_agent_operating_skill_revision
                        .clone(),
                    selected_project_agent_policy_digest: input
                        .target
                        .selected_project_agent_policy_digest
                        .clone(),
                    idempotency_key: input.idempotency_key,
                    authorization,
                },
                None,
            )
            .await?;
        Ok(SoloOperationOutcome::Applied(SoloCharterApprovalResult {
            approval_id: outcome.approval.id,
            project_id: outcome.project_id,
            project_version: outcome.project_version,
            project_charter_id: outcome.project_charter_id,
            project_charter_revision_id: outcome.project_charter_revision_id,
            project_agent_binding_id: outcome.project_agent_binding_id,
            project_chat_id: outcome.project_chat_id,
        }))
    }

    /// Accept or request changes on the exact human review gate through the
    /// existing workflow-aware TaskService.  The task version is checked by
    /// that service and no direct state transition is duplicated here.
    pub async fn decide_review(
        &self,
        input: SoloReviewDecisionInput,
    ) -> Result<SoloReviewDecisionResult> {
        let task_id = bounded_owned("task_id", input.task_id, SOLO_MAX_ID_CHARS)?;
        let task = self.authorized_task(&task_id).await?;
        if input.expected_task_version < 1 {
            return Err(ServiceError::invalid_operation(
                "expected_task_version must be positive",
            ));
        }
        let action = solo_review_action(input.decision, input.reason)?;
        let result = self
            .task_service
            .perform_task_action(task.id.clone(), action, input.expected_task_version)
            .await?;
        Ok(SoloReviewDecisionResult {
            task_id: result.task.id,
            decision: input.decision,
            committed_task_version: result.task.version,
        })
    }

    /// Read bounded activity for one Project Chat turn using its shared JSONL
    /// log root.  A file shrink/rotation resets the sequence cursor and is
    /// reported explicitly to the caller.
    pub async fn read_turn_activity(
        &self,
        turn_job_id: impl Into<String>,
        cursor: SoloActivityCursor,
        limit: usize,
    ) -> Result<SoloActivityPage> {
        validate_activity_limit(limit)?;
        let turn_job_id = path_component_id("turn_job_id", turn_job_id.into())?;
        let turn = self.authorized_turn(&turn_job_id).await?;
        let turn_id = path_component_id("turn id", turn.id.clone())?;
        let root = self.turn_logs.root();
        let path = self.turn_logs.path_for(&turn_id);
        read_activity_path(&path, &root, &turn_id, cursor, limit).await
    }

    /// Read bounded activity for a Task execution after proving both the Task
    /// and execution belong to this Project/repository scope.
    pub async fn read_execution_activity(
        &self,
        task_id: impl Into<String>,
        execution_id: impl Into<String>,
        cursor: SoloActivityCursor,
        limit: usize,
    ) -> Result<SoloActivityPage> {
        validate_activity_limit(limit)?;
        let task_id = path_component_id("task_id", task_id.into())?;
        let execution_id = path_component_id("execution_id", execution_id.into())?;
        let task = self.authorized_task(&task_id).await?;
        let execution = ExecutionRepo::get_by_id(&*self.db, &execution_id)
            .await?
            .filter(|execution| execution.task_id == task.id);
        let Some(execution) = execution else {
            return Err(ServiceError::not_found("execution", execution_id));
        };
        let Some(path) = execution.logs_path else {
            return Ok(SoloActivityPage {
                entries: Vec::new(),
                cursor,
                has_more: false,
                reset: false,
            });
        };
        let Some(root) = self.execution_logs_root.as_deref() else {
            return Err(ServiceError::invalid_operation(
                "execution log root is not attached to the Solo session",
            ));
        };
        let path = scoped_execution_log_path(
            root,
            &self.scope.project_id,
            &task.id,
            &execution.id,
            &path,
        )?;
        read_activity_path(&path, root, &execution.id, cursor, limit).await
    }

    fn capabilities(&self) -> SoloCapabilities {
        let runtime_interactions = match (&self.interaction_broker, &self.forge_session_id) {
            (None, _) => SoloCapability::Unsupported {
                reason: "the protected runtime interaction broker is not attached".to_owned(),
            },
            (Some(_), Some(session_id)) if !session_id.trim().is_empty() => {
                SoloCapability::Available
            }
            (Some(_), _) => SoloCapability::NotReady {
                reason: "the owning Agent Runtime session is not available".to_owned(),
            },
        };
        SoloCapabilities {
            runtime_interactions,
        }
    }

    async fn ensure_scope_ready_for_chat(&self) -> Result<()> {
        let records = self.load_scope_records().await?;
        if records.chat.status.trim().is_empty() {
            return Err(ServiceError::conflict(
                "Project Chat has no lifecycle status",
            ));
        }
        Ok(())
    }

    async fn load_scope_records(&self) -> Result<AuthorizedScopeRecords> {
        let project = ProjectRepo::get_by_id(&*self.db, &self.scope.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", self.scope.project_id.clone()))?;
        let owner_matches = project.owner_id.as_deref() == Some(self.scope.owner_id.as_str());
        if !owner_matches {
            return Err(ServiceError::not_found(
                "project",
                self.scope.project_id.clone(),
            ));
        }
        if project.primary_repo_id.as_deref() != Some(self.scope.repo_id.as_str()) {
            return Err(ServiceError::not_found("repo", self.scope.repo_id.clone()));
        }
        let repo = RepoRepo::get_by_id(&*self.db, &self.scope.repo_id)
            .await?
            .filter(|repo| repo.project_id == self.scope.project_id)
            .ok_or_else(|| ServiceError::not_found("repo", self.scope.repo_id.clone()))?;
        let chat = AgentChatRepo::get_agent_chat(&*self.db, &self.scope.project_chat_id)
            .await?
            .filter(|chat| {
                chat.kind == PROJECT_CHAT_KIND
                    && chat.project_id.as_deref() == Some(self.scope.project_id.as_str())
            })
            .ok_or_else(|| {
                ServiceError::not_found("agent_chat", self.scope.project_chat_id.clone())
            })?;
        let binding =
            ProjectAgentBindingRepo::get_active_project_binding(&*self.db, &self.scope.project_id)
                .await?
                .filter(|binding| binding.project_id == self.scope.project_id);
        if let Some(binding) = binding.as_ref() {
            if let Some(identity_id) = binding.identity_id.as_deref() {
                if let Some(identity) = AgentRepo::get_by_id(&*self.db, identity_id).await? {
                    if identity.owner_id.as_deref() != Some(self.scope.owner_id.as_str())
                        || binding.profile_id.as_deref() != Some(identity.profile_id.as_str())
                    {
                        return Err(ServiceError::not_found(
                            "project_agent_binding",
                            binding.id.clone(),
                        ));
                    }
                } else {
                    return Err(ServiceError::not_found(
                        "agent_identity",
                        identity_id.to_owned(),
                    ));
                }
            }
        }
        Ok(AuthorizedScopeRecords {
            project,
            repo,
            chat,
            binding,
        })
    }

    async fn authorized_turn(&self, turn_job_id: &str) -> Result<AgentChatTurnJob> {
        let job = AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, turn_job_id)
            .await?
            .filter(|job| turn_belongs_to_scope(job, &self.scope))
            .ok_or_else(|| ServiceError::not_found("agent_chat_turn", turn_job_id.to_owned()))?;
        // This second check ensures a tampered/legacy chat row cannot make a
        // turn look Project-scoped merely by sharing the chat id.
        self.load_scope_records().await?;
        Ok(job)
    }

    async fn authorized_task(&self, task_id: &str) -> Result<Task> {
        let task = TaskRepo::get_by_id(&*self.db, task_id, false)
            .await?
            .filter(|task| task.project_id == self.scope.project_id)
            .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
        self.load_scope_records().await?;
        Ok(task)
    }

    async fn load_messages(&self, limit: i64) -> Result<(Vec<SoloChatMessageSnapshot>, bool)> {
        let page = AgentChatMessageRepo::list_agent_chat_messages(
            &*self.db,
            db::AgentChatMessageListQuery {
                chat_id: self.scope.project_chat_id.clone(),
                before_sequence: None,
                page: PageRequest {
                    cursor: None,
                    limit,
                    include_total: false,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Asc,
                },
            },
        )
        .await?;
        let messages = page
            .items
            .into_iter()
            .filter(|message| message.chat_id == self.scope.project_chat_id)
            .map(|message| message_snapshot(&message))
            .collect();
        Ok((messages, page.next_cursor.is_some()))
    }

    async fn load_turns(&self, limit: i64) -> Result<Vec<SoloTurnSnapshot>> {
        let mut jobs =
            AgentChatTurnJobRepo::list_agent_chat_turn_jobs(&*self.db, &self.scope.project_chat_id)
                .await?;
        jobs.retain(|job| turn_belongs_to_scope(job, &self.scope));
        jobs.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        // The UI needs the newest turn as its retry/progress fence. Keeping
        // the oldest page would hide every new turn once the bound is full.
        let omitted = jobs.len().saturating_sub(limit as usize);
        jobs.drain(..omitted);
        Ok(jobs.iter().map(turn_snapshot).collect())
    }

    async fn load_tasks(&self, limit: i64) -> Result<Vec<SoloTaskSnapshot>> {
        let page = TaskRepo::list(
            &*self.db,
            TaskListQuery {
                project_id: self.scope.project_id.clone(),
                q: None,
                statuses: Vec::new(),
                agent_ids: Vec::new(),
                assignee_types: Vec::new(),
                assignee_ids: Vec::new(),
                priority: None,
                include_archived: false,
                include_cancelled: true,
                include_deleted: false,
                page: PageRequest {
                    cursor: None,
                    limit,
                    include_total: false,
                    sort_by: SortBy::UpdatedAt,
                    sort_order: SortOrder::Desc,
                },
            },
        )
        .await?;
        let mut tasks = Vec::new();
        for task in page.items {
            if task.project_id != self.scope.project_id {
                continue;
            }
            tasks.push(self.task_snapshot(task).await?);
        }
        Ok(tasks)
    }

    async fn task_snapshot(&self, task: Task) -> Result<SoloTaskSnapshot> {
        let task_id = task.id.clone();
        let interruption = task_interruption(&task);
        let mut executions = ExecutionRepo::list_by_task(
            &*self.db,
            &task_id,
            PageRequest {
                cursor: None,
                limit: SOLO_MAX_EXECUTIONS_PER_TASK,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        )
        .await?
        .items;
        executions.retain(|execution| execution.task_id == task_id);
        executions.truncate(SOLO_MAX_EXECUTIONS_PER_TASK as usize);
        let roles = TaskRoleAssignmentRepo::list_by_task(&*self.db, &task_id)
            .await?
            .into_iter()
            .take(SOLO_MAX_ROLES_PER_TASK as usize)
            .map(role_snapshot)
            .collect::<Vec<_>>();
        let review = ReviewRepo::list_by_task(&*self.db, &task_id)
            .await?
            .into_iter()
            .max_by(|left, right| {
                left.attempt_number
                    .cmp(&right.attempt_number)
                    .then_with(|| left.created_at.cmp(&right.created_at))
                    .then_with(|| left.id.cmp(&right.id))
            });
        let available_actions = self
            .task_service
            .task_action_offers(
                &task_id,
                &api_types::Actor::user(api_types::UserActionSource::Api),
            )
            .await?
            .available_actions;
        Ok(SoloTaskSnapshot {
            id: safe_identifier(&task.id),
            project_id: safe_identifier(&task.project_id),
            title: safe_diagnostic(&task.title).unwrap_or_else(|| "[redacted]".to_owned()),
            status: safe_identifier(&task.status),
            version: task.version,
            assignee_type: task.assignee_type.as_deref().map(safe_identifier),
            assignee_id: task.assignee_id.as_deref().map(safe_identifier),
            priority: task.priority,
            interruption,
            available_actions,
            roles,
            executions: executions
                .iter()
                .map(execution_snapshot)
                .collect::<Vec<_>>(),
            latest_review: review.as_ref().map(review_snapshot),
            created_at: safe_text(&task.created_at, SOLO_MAX_SHORT_TEXT_CHARS),
            updated_at: safe_text(&task.updated_at, SOLO_MAX_SHORT_TEXT_CHARS),
        })
    }

    async fn load_attention(&self, limit: i64) -> Result<Vec<SoloAttentionSnapshot>> {
        let page = AttentionRepo::list_attention(
            &*self.db,
            AttentionListQuery {
                account_id: Some(self.scope.owner_id.clone()),
                project_id: Some(self.scope.project_id.clone()),
                scope_type: Some("project".to_owned()),
                status: None,
                include_snoozed: false,
                page: PageRequest {
                    cursor: None,
                    limit,
                    include_total: false,
                    sort_by: SortBy::Priority,
                    sort_order: SortOrder::Desc,
                },
            },
        )
        .await?;
        Ok(page
            .items
            .into_iter()
            .filter(|item| item.scope_type == "project" && item.scope_id == self.scope.project_id)
            .map(attention_snapshot)
            .collect())
    }

    async fn load_interactions(
        &self,
        turns: &[SoloTurnSnapshot],
    ) -> Result<Vec<SoloInteractionSnapshot>> {
        let Some(broker) = self.interaction_broker.as_ref() else {
            return Ok(Vec::new());
        };
        let Some(session_id) = self
            .forge_session_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(Vec::new());
        };
        let referenced: HashSet<&str> = turns
            .iter()
            .filter_map(|turn| turn.pending_interaction_id.as_deref())
            .collect();
        if referenced.is_empty() {
            return Ok(Vec::new());
        }
        let summaries = broker
            .list_pending_for_owner(&self.scope.owner_id, session_id)
            .await
            .map_err(map_interaction_error)?;
        Ok(summaries
            .into_iter()
            .filter(|summary| referenced.contains(summary.id.as_str()))
            .map(|summary| interaction_snapshot(&summary))
            .collect())
    }

    async fn load_charter_snapshot(
        &self,
        project: &Project,
        binding: Option<&ProjectAgentBinding>,
    ) -> Result<Option<SoloCharterSnapshot>> {
        let Some(charter) = ProjectOrchestrationRepo::get_project_charter_by_project_id(
            &*self.db,
            &self.scope.project_id,
        )
        .await?
        else {
            return Ok(None);
        };
        if charter.project_id.as_deref() != Some(self.scope.project_id.as_str())
            || charter.account_id != self.scope.owner_id
        {
            return Err(ServiceError::not_found(
                "project_charter",
                charter.id.clone(),
            ));
        }
        let current_draft = self
            .load_charter_revision(&charter, charter.current_draft_revision_id.as_deref())
            .await?;
        let current_approved = self
            .load_charter_revision(&charter, charter.current_approved_revision_id.as_deref())
            .await?;
        let approval_target = self
            .charter_approval_target(project, binding, &charter, current_draft.as_ref())
            .await?;
        Ok(Some(SoloCharterSnapshot {
            id: safe_identifier(&charter.id),
            project_id: charter.project_id.as_deref().map(safe_identifier),
            version: charter.version,
            project_mode: safe_identifier(&charter.project_mode),
            maturity: safe_identifier(&charter.maturity),
            lifecycle: safe_identifier(&charter.lifecycle),
            current_draft_revision_id: charter
                .current_draft_revision_id
                .as_deref()
                .map(safe_identifier),
            current_approved_revision_id: charter
                .current_approved_revision_id
                .as_deref()
                .map(safe_identifier),
            current_draft,
            current_approved,
            approval_target,
        }))
    }

    async fn load_charter_revision(
        &self,
        charter: &ProjectCharterRecord,
        revision_id: Option<&str>,
    ) -> Result<Option<SoloCharterRevisionSnapshot>> {
        let Some(revision_id) = revision_id else {
            return Ok(None);
        };
        let revision =
            ProjectOrchestrationRepo::get_project_charter_revision(&*self.db, revision_id)
                .await?
                .filter(|revision| revision.charter_id == charter.id)
                .ok_or_else(|| {
                    ServiceError::not_found("project_charter_revision", revision_id.to_owned())
                })?;
        Ok(Some(charter_revision_snapshot(&revision)))
    }

    async fn charter_approval_target(
        &self,
        project: &Project,
        binding: Option<&ProjectAgentBinding>,
        charter: &ProjectCharterRecord,
        draft: Option<&SoloCharterRevisionSnapshot>,
    ) -> Result<Option<SoloCharterApprovalTarget>> {
        let kind = match project.charter_status.as_str() {
            LEGACY_UNVERIFIED_STATUS => SoloCharterApprovalKind::Adoption,
            CHARTER_BACKED_STATUS => SoloCharterApprovalKind::Amendment,
            _ => return Ok(None),
        };
        let pointer_matches = match kind {
            SoloCharterApprovalKind::Adoption => {
                project.charter_setup_required
                    && project.current_charter_id.is_none()
                    && project.current_charter_revision_id.is_none()
                    && charter.current_approved_revision_id.is_none()
            }
            SoloCharterApprovalKind::Amendment => {
                !project.charter_setup_required
                    && project.current_charter_id.as_deref() == Some(charter.id.as_str())
                    && project.current_charter_revision_id.as_deref()
                        == charter.current_approved_revision_id.as_deref()
                    && charter.current_approved_revision_id.is_some()
            }
        };
        if !pointer_matches {
            return Ok(None);
        }
        let Some(draft) = draft else {
            return Ok(None);
        };
        let Some(binding) = binding else {
            return Ok(None);
        };
        if !matches!(
            binding.state.as_str(),
            READY_BINDING_STATE | SETUP_BINDING_STATE
        ) {
            return Ok(None);
        }
        let (Some(identity_id), Some(profile_id), Some(skill_revision)) = (
            binding.identity_id.as_ref(),
            binding.profile_id.as_ref(),
            binding.operating_skill_revision_id.as_ref(),
        ) else {
            return Ok(None);
        };
        if binding.policy_digest.trim().is_empty() {
            return Ok(None);
        }
        let Some(working_name) = draft
            .working_name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
        else {
            return Ok(None);
        };
        bounded_required(
            "Charter working name",
            working_name,
            SOLO_MAX_SHORT_TEXT_CHARS,
        )?;
        if let Some(slug) = draft.slug_proposal.as_deref() {
            bounded_required("Charter slug proposal", slug, SOLO_MAX_SHORT_TEXT_CHARS)?;
        }
        let charter_id = path_component_id("charter_id", charter.id.clone())?;
        let revision_id = path_component_id("revision_id", draft.id.clone())?;
        let identity_id = path_component_id("project agent identity_id", identity_id.clone())?;
        let profile_id = path_component_id("project agent profile_id", profile_id.clone())?;
        // This is an opaque revision token (for example `forge.project.orchestration/v1@1`),
        // not a filename. Keep it bounded without rejecting its punctuation.
        let skill_revision = bounded_owned(
            "project agent skill revision",
            skill_revision.clone(),
            SOLO_MAX_ID_CHARS,
        )?;
        bounded_required(
            "Charter content digest",
            &draft.content_digest,
            SOLO_MAX_ID_CHARS,
        )?;
        bounded_required(
            "Charter rendered digest",
            &draft.rendered_digest,
            SOLO_MAX_ID_CHARS,
        )?;
        let policy_digest =
            path_component_id("project agent policy digest", binding.policy_digest.clone())?;
        Ok(Some(SoloCharterApprovalTarget {
            kind,
            project_id: self.scope.project_id.clone(),
            charter_id,
            revision_id,
            content_digest: draft.content_digest.clone(),
            rendered_digest: draft.rendered_digest.clone(),
            expected_charter_version: charter.version,
            expected_project_version: project.version,
            approved_project_name: working_name.to_owned(),
            approved_project_slug: draft.slug_proposal.clone(),
            project_mode: safe_identifier(&charter.project_mode),
            selected_project_agent_identity_id: identity_id,
            selected_project_agent_profile_revision_id: profile_id,
            selected_project_agent_operating_skill_revision: skill_revision,
            selected_project_agent_policy_digest: policy_digest,
        }))
    }

    fn validate_interaction_values(&self, values: &[SoloInteractionAnswerValue]) -> Result<()> {
        if values.len() > SOLO_MAX_INTERACTION_VALUES {
            return Err(ServiceError::invalid_operation(
                "interaction answer exceeds the bounded question limit",
            ));
        }
        for value in values {
            match value {
                SoloInteractionAnswerValue::Choice {
                    question_id,
                    choice_id,
                } => {
                    bounded_required("interaction question_id", question_id, SOLO_MAX_ID_CHARS)?;
                    bounded_required("interaction choice_id", choice_id, SOLO_MAX_ID_CHARS)?;
                }
                SoloInteractionAnswerValue::FreeForm { question_id, value } => {
                    bounded_required("interaction question_id", question_id, SOLO_MAX_ID_CHARS)?;
                    bounded_required("interaction answer", value, SOLO_MAX_TEXT_CHARS)?;
                }
            }
        }
        Ok(())
    }
}
