//! Embedded session creation, resumption, rotation, control, and permissions.

use super::*;

impl EmbeddedAgentService {
    pub async fn create_or_resume_session(
        &self,
        input: CreateScopedSession,
    ) -> Result<AgentSession> {
        let identity = self
            .require_owned_identity(&input.identity_id, &input.actor_user_id)
            .await?;
        if identity.paused {
            return Err(ServiceError::AgentPaused {
                agent_id: identity.id,
            });
        }
        let profile_id = input
            .profile_id
            .unwrap_or_else(|| identity.profile_id.clone());
        let profile = AgentProfileRepo::get_profile(&*self.db, &profile_id)
            .await?
            .filter(|profile| profile.identity_id == identity.id)
            .ok_or_else(|| ServiceError::not_found("agent_profile", profile_id.clone()))?;
        self.require_profile_source_enabled(&profile).await?;
        let canonical = self
            .authorize_scope(&input.actor_user_id, &identity, &input.scope)
            .await?;
        let project_id = self.scope_project_id(&input.scope).await?;
        self.persist_authorized_session(
            identity,
            profile,
            canonical,
            project_id,
            canonical_task_id(&input.scope),
            canonical_task_role(&input.scope),
            scope_authority_json(&input.actor_user_id, &input.scope),
        )
        .await
    }

    /// Create or resume the session an ephemeral inquiry sub-agent runs in.
    ///
    /// This deliberately does not go through [`Self::authorize_scope`]: the
    /// scope is host-derived rather than caller-requested, and it is the one
    /// Account scope that carries a scratch directory. Ownership is still
    /// enforced, through the identity that dispatched the inquiry.
    ///
    /// One authority/session record serves every inquiry an account runs. Two existing
    /// constraints force that shape: `main_account_id` requires an Account
    /// scope's id to be the owner's id, so the id cannot vary per inquiry,
    /// and `create_context_scope` keys on `(identity, scope_type, scope_id,
    /// task_role)`, so a per-inquiry scope row is not expressible either.
    /// The native backend omits snapshot/checkpoint/LCM stores for this scope,
    /// so each inquiry starts with empty history and usage. [`crate::agent_inquiry_runner`]
    /// serializes inquiries per account so two turns never share one live
    /// runtime session.
    pub async fn create_inquiry_session(
        &self,
        actor_user_id: &str,
        identity_id: &str,
    ) -> Result<AgentSession> {
        let identity = self
            .require_owned_identity(identity_id, actor_user_id)
            .await?;
        if identity.paused {
            return Err(ServiceError::AgentPaused {
                agent_id: identity.id,
            });
        }
        let profile = AgentProfileRepo::get_profile(&*self.db, &identity.profile_id)
            .await?
            .filter(|profile| profile.identity_id == identity.id)
            .ok_or_else(|| ServiceError::not_found("agent_profile", identity.profile_id.clone()))?;
        self.require_profile_source_enabled(&profile).await?;
        if profile.backend_kind != "native" {
            return Err(ServiceError::invalid_operation(
                "inquiries require a native Agent Profile",
            ));
        }
        // Resolve the scratch directory before the scope asserts it. A scope
        // that advertises `account_scratch` without a directory is a binding
        // the runtime correctly refuses at turn time.
        if self.main_agent_workspace(actor_user_id).await.is_none() {
            return Err(ServiceError::invalid_operation(
                "the Main Agent scratch workspace is unavailable",
            ));
        }
        let canonical = CanonicalScope {
            scope_type: CanonicalScopeType::Account,
            scope_id: actor_user_id.to_owned(),
            workspace_access: WorkspaceAccess::AccountScratch,
        };
        canonical
            .validate()
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let capabilities = capabilities_for_profile(&profile, &canonical);
        let session = self
            .persist_authorized_session(
                identity,
                profile.clone(),
                canonical,
                None,
                None,
                None,
                scope_authority_json(actor_user_id, &RequestedCanonicalScope::Account),
            )
            .await?;
        if serde_json::from_str::<BackendCapabilities>(&session.capabilities_json).ok()
            == Some(capabilities)
        {
            Ok(session)
        } else {
            // Earlier inquiry sessions advertised persistent conversation
            // stores. Rotate through the normal versioned transaction so the
            // derived capabilities become current without deleting history.
            self.rotate_authorized_session(&session, &profile).await
        }
    }

    /// Create or resume the session for an already-admitted Agent Chat turn.
    ///
    /// Interactive callers must continue through [`Self::authorize_scope`],
    /// which resolves the current binding.  A queued turn is different: its
    /// binding and Profile were selected at admission, so resolving the
    /// active replacement here would mix authority revisions.  This narrow
    /// internal path validates the retained historical row instead and only
    /// permits a replacement whose version transition is the one produced by
    /// the binding repository.  Runtime command/tool authorization remains
    /// current and therefore can still reject a withdrawn Project ceiling.
    pub(crate) async fn create_or_resume_frozen_chat_session(
        &self,
        input: CreateFrozenAgentChatSession,
    ) -> Result<AgentSession> {
        let RequestedCanonicalScope::AgentChat { chat_id } = &input.scope else {
            return Err(ServiceError::invalid_operation(
                "frozen Agent Chat sessions require an Agent Chat scope",
            ));
        };
        let identity = self
            .require_owned_identity(&input.identity_id, &input.actor_user_id)
            .await?;
        if identity.paused {
            return Err(ServiceError::AgentPaused {
                agent_id: identity.id,
            });
        }
        let profile = AgentProfileRepo::get_profile(&*self.db, &input.profile_id)
            .await?
            .filter(|profile| profile.identity_id == identity.id)
            .ok_or_else(|| ServiceError::not_found("agent_profile", input.profile_id.clone()))?;
        self.require_profile_source_enabled(&profile).await?;
        if profile.version != input.profile_version {
            return Err(ServiceError::invalid_operation(
                "frozen Agent Chat Profile revision is no longer available",
            ));
        }
        if input.permission_policy_digest.trim().is_empty()
            || input.tool_policy_digest.trim().is_empty()
        {
            return Err(ServiceError::invalid_operation(
                "frozen Agent Chat policy provenance is incomplete",
            ));
        }

        let chat = AgentChatRepo::get_agent_chat(&*self.db, chat_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_chat", chat_id.clone()))?;
        let project_id = match chat.kind.as_str() {
            "account_main" => {
                if chat.account_id.as_deref() != Some(input.actor_user_id.as_str())
                    || chat.project_id.is_some()
                {
                    return Err(ServiceError::not_found("agent_chat", chat_id.clone()));
                }
                let binding =
                    AccountMainAgentBindingRepo::get_main_binding(&*self.db, &input.binding_id)
                        .await?
                        .filter(|binding| {
                            binding.account_id == input.actor_user_id
                                && binding.identity_id == identity.id
                        })
                        .ok_or_else(|| {
                            ServiceError::invalid_operation(
                                "frozen Main Agent binding history is unavailable",
                            )
                        })?;
                validate_frozen_binding_state(
                    &binding.state,
                    binding.version,
                    input.binding_version,
                )?;
                if binding.tool_policy_revision != input.policy_revision {
                    return Err(ServiceError::invalid_operation(
                        "frozen Main Agent policy revision changed after admission",
                    ));
                }
                if crate::agent_turn_admission::policy_digest(&binding.autonomy_policy_json)?
                    != input.policy_digest
                {
                    return Err(ServiceError::invalid_operation(
                        "frozen Main Agent policy changed after admission",
                    ));
                }
                None
            }
            "project" => {
                if chat.account_id.is_some() {
                    return Err(ServiceError::not_found("agent_chat", chat_id.clone()));
                }
                let project_id = chat.project_id.clone().ok_or_else(|| {
                    ServiceError::invalid_operation("Project Agent Chat has no Project scope")
                })?;
                ProjectMemberRepo::get_member(&*self.db, &project_id, &input.actor_user_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project", project_id.clone()))?;
                let binding =
                    ProjectAgentBindingRepo::get_project_binding(&*self.db, &input.binding_id)
                        .await?
                        .filter(|binding| {
                            binding.project_id == project_id
                                && binding.identity_id.as_deref() == Some(identity.id.as_str())
                        })
                        .ok_or_else(|| {
                            ServiceError::invalid_operation(
                                "frozen Project Agent binding history is unavailable",
                            )
                        })?;
                validate_frozen_binding_state(
                    &binding.state,
                    binding.version,
                    input.binding_version,
                )?;
                let policy_row = self
                    .db
                    .frozen_project_agent_binding_policy(&input.binding_id, &project_id)
                    .await?;
                let policy_revision: String = policy_row.policy_revision()?;
                let stored_policy_digest: String = policy_row.policy_digest()?;
                if policy_revision != input.policy_revision
                    || stored_policy_digest != input.policy_digest
                {
                    return Err(ServiceError::invalid_operation(
                        "frozen Project Agent policy changed after admission",
                    ));
                }
                if crate::agent_turn_admission::policy_digest(&binding.permission_ceiling_json)?
                    != input.permission_policy_digest
                {
                    return Err(ServiceError::invalid_operation(
                        "frozen Project Agent permission policy changed after admission",
                    ));
                }
                Some(project_id)
            }
            _ => {
                return Err(ServiceError::invalid_operation(
                    "Agent Chat has an unsupported canonical kind",
                ));
            }
        };
        if crate::agent_turn_admission::policy_digest(&profile.tool_policy_json)?
            != input.tool_policy_digest
        {
            return Err(ServiceError::invalid_operation(
                "frozen Agent Chat Profile policy changed after admission",
            ));
        }

        // Both chat kinds carry their own workspace, so the frozen session
        // must resolve the same access the turn will assert. Building `Deny`
        // here and a workspace access at turn time is a binding mismatch,
        // which the runtime correctly refuses.
        let workspace_access = match (project_id.as_deref(), chat.account_id.as_deref()) {
            (Some(project_id), _) if self.project_agent_workspace(project_id).await.is_some() => {
                WorkspaceAccess::ProjectVerify
            }
            (None, Some(account_id)) if self.main_agent_workspace(account_id).await.is_some() => {
                WorkspaceAccess::AccountScratch
            }
            _ => WorkspaceAccess::Deny,
        };
        let canonical = CanonicalScope {
            scope_type: CanonicalScopeType::AgentChat,
            scope_id: chat_id.clone(),
            workspace_access,
        };
        canonical
            .validate()
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        self.persist_authorized_session(
            identity,
            profile,
            canonical,
            project_id,
            None,
            None,
            scope_authority_json(&input.actor_user_id, &input.scope),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_authorized_session(
        &self,
        identity: Agent,
        profile: AgentProfile,
        canonical: CanonicalScope,
        project_id: Option<String>,
        task_id: Option<String>,
        task_role: Option<String>,
        authority_json: Value,
    ) -> Result<AgentSession> {
        let now = now_rfc3339();
        let scope_workspace_path = match (canonical.workspace_access, project_id.as_deref()) {
            (WorkspaceAccess::ProjectVerify, Some(project_id)) => self
                .project_agent_workspace(project_id)
                .await
                .map(|path| path.to_string_lossy().into_owned()),
            // The scratch directory belongs to the account that owns the
            // identity, so it is resolved from ownership rather than from the
            // opaque scope id.
            (WorkspaceAccess::AccountScratch, _) => match identity.owner_id.as_deref() {
                Some(account_id) => self
                    .main_agent_workspace(account_id)
                    .await
                    .map(|path| path.to_string_lossy().into_owned()),
                None => None,
            },
            _ => None,
        };
        let scope = AgentContextScopeRepo::create_context_scope(
            &*self.db,
            CreateAgentContextScope {
                id: new_uuid_v4(),
                identity_id: identity.id.clone(),
                scope_type: canonical_scope_name(canonical.scope_type).to_owned(),
                scope_id: canonical.scope_id.clone(),
                project_id,
                task_id,
                task_role,
                workspace_access: workspace_access_name(canonical.workspace_access).to_owned(),
                // A `project_verify` or `account_scratch` scope owns its
                // directory directly; every other scope leaves this NULL and
                // the Task `workspace` table stays the authority.
                workspace_path: match canonical.workspace_access {
                    WorkspaceAccess::ProjectVerify | WorkspaceAccess::AccountScratch => {
                        scope_workspace_path.clone()
                    }
                    _ => None,
                },
                authority_json: authority_json.to_string(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await?;
        if let Some(active) =
            AgentSessionRepo::get_active_agent_session(&*self.db, &identity.id, &scope.id).await?
        {
            return self
                .reuse_or_rotate_active_session(active, &profile, &canonical)
                .await;
        }
        // A native topic retains its runtime identity across server
        // suspension. Only a scope with no active session may resume one: the
        // active-scope unique index admits a single live row, and restarts
        // before topic working sets left older suspended rows behind.
        if profile.backend_kind == "native" && canonical.scope_type == CanonicalScopeType::AgentChat
        {
            if let Some(dormant) = self
                .dormant_native_chat_session(&identity.id, &scope.id, &profile.id)
                .await?
            {
                let resumed = self.resume_dormant_session(&dormant, &now).await?;
                return self
                    .reuse_or_rotate_active_session(resumed, &profile, &canonical)
                    .await;
            }
        }
        let capabilities = capabilities_for_profile(&profile, &canonical);
        let (status, connection_status) = self.initial_session_status(&profile).await?;
        let created = AgentSessionRepo::create_agent_session(
            &*self.db,
            CreateAgentSession {
                id: new_uuid_v4(),
                identity_id: identity.id.clone(),
                profile_id: profile.id.clone(),
                context_scope_id: scope.id.clone(),
                backend_kind: profile.backend_kind.clone(),
                runtime_session_id: (profile.backend_kind == "native").then(new_uuid_v4),
                status,
                capabilities_json: serde_json::to_string(&capabilities)
                    .unwrap_or_else(|_| "{}".to_owned()),
                connection_status,
                predecessor_session_id: None,
                last_activity_at: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await;
        match created {
            Ok(session) => Ok(session),
            Err(error) => {
                let winner = self
                    .activation_winner(error.into(), &identity.id, &scope.id)
                    .await?;
                self.reuse_or_rotate_active_session(winner, &profile, &canonical)
                    .await
            }
        }
    }

    /// Returns the scope's active session when it still matches the Profile
    /// and its derived capabilities, and rotates it otherwise.
    async fn reuse_or_rotate_active_session(
        &self,
        active: AgentSession,
        profile: &AgentProfile,
        canonical: &CanonicalScope,
    ) -> Result<AgentSession> {
        let capabilities = capabilities_for_profile(profile, canonical);
        if !self.native_session_retired(&active).await?
            && active.profile_id == profile.id
            && serde_json::from_str::<BackendCapabilities>(&active.capabilities_json).ok()
                == Some(capabilities)
        {
            return Ok(active);
        }
        // Capability policy is derived runtime state, not immutable
        // Profile data. Rotate when a server upgrade changes it so an
        // existing Task runtime cannot keep a now-disabled LCM session.
        self.rotate_authorized_session(&active, profile).await
    }

    /// The newest suspended native session of this scope and Profile that was
    /// never replaced, unless a pending topic rotation owns it as its
    /// successor.
    async fn dormant_native_chat_session(
        &self,
        identity_id: &str,
        context_scope_id: &str,
        profile_id: &str,
    ) -> Result<Option<AgentSession>> {
        let dormant: Option<String> = self
            .db
            .dormant_native_chat_session_id(identity_id, context_scope_id, profile_id)
            .await?;
        let Some(id) = dormant else {
            return Ok(None);
        };
        let pending_successor: bool = self.db.native_chat_session_pending_successor(&id).await?;
        if pending_successor {
            return Ok(None);
        }
        AgentSessionRepo::get_agent_session(&*self.db, &id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_session", &id))
            .map(Some)
    }

    /// Activates a suspended session read earlier. A concurrent call for the
    /// same scope may activate a session first; this call then returns that
    /// session instead of its own stale write's error.
    pub(super) async fn resume_dormant_session(
        &self,
        dormant: &AgentSession,
        now: &str,
    ) -> Result<AgentSession> {
        if self.native_session_retired(dormant).await? {
            return self.rotate_retired_session(dormant).await;
        }
        let resumed = AgentSessionRepo::update_agent_session(
            &*self.db,
            UpdateAgentSession {
                id: dormant.id.clone(),
                expected_version: dormant.version,
                runtime_session_id: None,
                status: Some("ready".to_owned()),
                connection_status: None,
                last_activity_at: None,
                updated_at: now.to_owned(),
            },
        )
        .await;
        match resumed {
            Ok(session) => Ok(session),
            Err(db::DbError::SessionRetired) => self.rotate_retired_session(dormant).await,
            Err(error) => {
                self.activation_winner(
                    error.into(),
                    &dormant.identity_id,
                    &dormant.context_scope_id,
                )
                .await
            }
        }
    }

    /// Resolves a lost activation race. The active-scope unique index and the
    /// session version column let one caller activate a session for a scope;
    /// the loser adopts that session. Any other error, or a conflict with no
    /// active session to adopt, is returned unchanged.
    pub(super) async fn activation_winner(
        &self,
        error: ServiceError,
        identity_id: &str,
        context_scope_id: &str,
    ) -> Result<AgentSession> {
        let lost_race = match &error {
            ServiceError::Db(db::DbError::VersionConflict) => true,
            ServiceError::Db(db::DbError::Sqlx(sqlx::Error::Database(database))) => {
                database.is_unique_violation()
            }
            _ => false,
        };
        if !lost_race {
            return Err(error);
        }
        match AgentSessionRepo::get_active_agent_session(&*self.db, identity_id, context_scope_id)
            .await?
        {
            Some(winner) => Ok(winner),
            None => Err(error),
        }
    }

    pub async fn rotate_session(
        &self,
        actor_user_id: &str,
        session_id: &str,
        expected_version: i64,
    ) -> Result<AgentSession> {
        let previous = AgentSessionRepo::get_agent_session(&*self.db, session_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_session", session_id.to_owned()))?;
        self.require_owned_identity(&previous.identity_id, actor_user_id)
            .await?;
        if previous.version != expected_version {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }
        let profile = AgentProfileRepo::get_profile(&*self.db, &previous.profile_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_profile", previous.profile_id.clone()))?;
        self.rotate_session_for_profile(actor_user_id, &previous, &profile)
            .await
    }

    pub async fn set_session_status(
        &self,
        actor_user_id: &str,
        session_id: &str,
        expected_version: i64,
        status: &str,
    ) -> Result<AgentSession> {
        if !matches!(status, "ready" | "suspended" | "cancelled") {
            return Err(ServiceError::invalid_operation(
                "session status must be ready, suspended, or cancelled",
            ));
        }
        let session = AgentSessionRepo::get_agent_session(&*self.db, session_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_session", session_id.to_owned()))?;
        self.require_owned_identity(&session.identity_id, actor_user_id)
            .await?;
        AgentSessionRepo::update_agent_session(
            &*self.db,
            UpdateAgentSession {
                id: session.id,
                expected_version,
                runtime_session_id: None,
                status: Some(status.to_owned()),
                connection_status: None,
                last_activity_at: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    pub async fn cancel_session_turn(&self, actor_user_id: &str, session_id: &str) -> Result<()> {
        let session = AgentSessionRepo::get_agent_session(&*self.db, session_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_session", session_id.to_owned()))?;
        self.require_owned_identity(&session.identity_id, actor_user_id)
            .await?;
        if session.backend_kind != "native" {
            return Err(ServiceError::invalid_operation(
                "selected backend does not support native cancellation",
            ));
        }
        let runtime_session_id = session.runtime_session_id.ok_or_else(|| {
            ServiceError::invalid_operation("session has no native runtime identifier")
        })?;
        self.native_backend
            .cancel(&runtime_session_id)
            .await
            .map_err(redacted_host_error)
    }

    pub async fn steer_session_turn(
        &self,
        actor_user_id: &str,
        session_id: &str,
        content: String,
    ) -> Result<()> {
        if content.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "steer content cannot be empty",
            ));
        }
        let session = AgentSessionRepo::get_agent_session(&*self.db, session_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_session", session_id.to_owned()))?;
        self.require_owned_identity(&session.identity_id, actor_user_id)
            .await?;
        if session.backend_kind != "native" {
            return Err(ServiceError::invalid_operation(
                "selected backend does not support steering",
            ));
        }
        let runtime_session_id = session.runtime_session_id.ok_or_else(|| {
            ServiceError::invalid_operation("session has no native runtime identifier")
        })?;
        self.native_backend
            .steer(&runtime_session_id, content)
            .await
            .map_err(redacted_host_error)
    }

    pub async fn list_sessions(
        &self,
        actor_user_id: &str,
        identity_id: &str,
    ) -> Result<Vec<AgentSession>> {
        self.require_owned_identity(identity_id, actor_user_id)
            .await?;
        AgentSessionRepo::list_agent_sessions(&*self.db, identity_id)
            .await
            .map_err(Into::into)
    }

    pub async fn effective_permissions(
        &self,
        actor_user_id: &str,
        identity_id: &str,
        scope_request: &RequestedCanonicalScope,
    ) -> Result<EffectivePermissions> {
        let identity = self
            .require_owned_identity(identity_id, actor_user_id)
            .await?;
        let profile = AgentProfileRepo::get_profile(&*self.db, &identity.profile_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_profile", identity.profile_id.clone()))?;
        let canonical = self
            .authorize_scope(actor_user_id, &identity, scope_request)
            .await?;
        let allowed = if canonical.scope_type == CanonicalScopeType::Task {
            // A preview for an identity, not a session: Task authority itself
            // also needs the running execution, which no preview has.
            let account_json: String = self
                .db
                .embedded_identity_account_permission_ceiling(identity_id)
                .await?;
            intersect_non_empty_layers(&[
                permission_set(&account_json),
                permission_set(&profile.tool_policy_json),
                scope_permission_set(&canonical, false, false),
            ])
        } else {
            let authority = self
                .db
                .resolve_effective_authority(
                    identity_id,
                    Some(&profile.id),
                    canonical_scope_name(canonical.scope_type),
                    &canonical.scope_id,
                    match canonical.workspace_access {
                        WorkspaceAccess::AccountScratch => "account_scratch",
                        WorkspaceAccess::ProjectVerify => "project_verify",
                        _ => "deny",
                    },
                )
                .await?;
            if authority.active {
                authority.ceiling
            } else {
                BTreeSet::new()
            }
        };
        let known = known_permissions();
        let denied = known.difference(&allowed).cloned().collect();
        let requires_approval = allowed
            .iter()
            .filter(|permission| permission.starts_with("propose_") || *permission == "task_write")
            .cloned()
            .collect();
        Ok(EffectivePermissions {
            allowed,
            denied,
            requires_approval,
        })
    }

    async fn rotate_session_for_profile(
        &self,
        actor_user_id: &str,
        previous: &AgentSession,
        profile: &AgentProfile,
    ) -> Result<AgentSession> {
        self.require_owned_identity(&previous.identity_id, actor_user_id)
            .await?;
        self.rotate_authorized_session(previous, profile).await
    }

    pub(super) async fn rotate_authorized_session(
        &self,
        previous: &AgentSession,
        profile: &AgentProfile,
    ) -> Result<AgentSession> {
        if profile.identity_id != previous.identity_id {
            return Err(ServiceError::invalid_operation(
                "replacement profile belongs to another identity",
            ));
        }
        let now = now_rfc3339();
        let replacement_id = new_uuid_v4();
        let (status, connection_status) = self.initial_session_status(profile).await?;
        let scope = AgentContextScopeRepo::get_context_scope(&*self.db, &previous.context_scope_id)
            .await?
            .ok_or_else(|| {
                ServiceError::not_found("agent_context_scope", previous.context_scope_id.clone())
            })?;
        let canonical = CanonicalScope {
            scope_type: parse_canonical_scope_type(&scope.scope_type)?,
            scope_id: scope.scope_id,
            workspace_access: parse_workspace_access(&scope.workspace_access)?,
        };
        let capabilities = capabilities_for_profile(profile, &canonical);
        AgentSessionRepo::rotate_agent_session(
            &*self.db,
            RotateAgentSession {
                previous_session_id: previous.id.clone(),
                expected_version: previous.version,
                replacement: CreateAgentSession {
                    id: replacement_id,
                    identity_id: previous.identity_id.clone(),
                    profile_id: profile.id.clone(),
                    context_scope_id: previous.context_scope_id.clone(),
                    backend_kind: profile.backend_kind.clone(),
                    runtime_session_id: (profile.backend_kind == "native").then(new_uuid_v4),
                    status,
                    capabilities_json: serde_json::to_string(&capabilities)
                        .unwrap_or_else(|_| "{}".to_owned()),
                    connection_status,
                    predecessor_session_id: Some(previous.id.clone()),
                    last_activity_at: None,
                    created_at: now.clone(),
                    updated_at: now,
                },
            },
        )
        .await
        .map_err(Into::into)
    }

    async fn initial_session_status(&self, profile: &AgentProfile) -> Result<(String, String)> {
        if profile.backend_kind != "native" {
            return Ok(("ready".to_owned(), "unknown".to_owned()));
        }
        let connection_status =
            AgentConnectionHealthRepo::get_connection_health(&*self.db, &profile.id)
                .await?
                .map(|health| health.status)
                .unwrap_or_else(|| "unknown".to_owned());
        let status = if connection_status == "healthy" {
            "ready"
        } else {
            "degraded"
        };
        Ok((status.to_owned(), connection_status))
    }
}
