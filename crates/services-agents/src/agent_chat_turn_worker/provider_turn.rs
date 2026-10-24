//! Agent Chat context construction and native provider turn execution.

use super::*;

impl FederatedAgentChatTurnRunner {
    pub(super) async fn load_turn_context(
        &self,
        job: &AgentChatTurnJob,
    ) -> Result<LoadedAgentChatContext> {
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
        let frozen_authority = self
            .load_frozen_authority(&chat, job, &agent, &profile)
            .await
            .map_err(|error| match error {
                ServiceError::InvalidOperation { .. } | ServiceError::Conflict(_) => {
                    ServiceError::TurnFailure {
                        failure: TurnFailure::Authority,
                        error: Box::new(error),
                    }
                }
                other => other,
            })?;

        // The Project operating skill is admitted before session creation or
        // any model call.  A Project Chat is never allowed to fall back to a
        // profile-only prompt, an old handoff overlay, or a legacy Project
        // binding when its Charter pointers cannot be proven current.
        let (mut operating_instruction, state_card, operating_context_sources) = match chat
            .kind
            .as_str()
        {
            "account_main" => {
                if chat.project_id.is_some() {
                    return Err(ServiceError::invalid_operation(
                        "Main Agent Chat unexpectedly has a Project scope",
                    ));
                }
                let main_ceiling: String = self
                    .db
                    .main_turn_account_permission_ceiling(&agent.id)
                    .await?;
                let main_ceiling = readable_permission_ceiling(&main_ceiling);
                // A complete job chooses its Main branch from the persisted
                // skill revision, not from a lifecycle read taken after the
                // job was admitted.  Historical Genesis instructions are
                // immutable; constrain the lookup to rows that existed by
                // admission so a later Genesis cannot replace the context.
                let frozen_skill_key = frozen_authority
                    .as_ref()
                    .map(|snapshot| snapshot.operating_skill_key.as_str());
                let frozen_baseline = frozen_skill_key == Some(MAIN_BASELINE_OPERATING_SKILL_KEY);
                let frozen_genesis = frozen_skill_key == Some(MAIN_OPERATING_SKILL_KEY);
                if frozen_authority.is_some() && !frozen_baseline && !frozen_genesis {
                    return Err(ServiceError::invalid_operation(
                        "Agent Chat frozen Main skill branch is unsupported",
                    ));
                }
                let instruction_row = if frozen_baseline {
                    None
                } else if frozen_genesis {
                    self.db
                        .main_turn_genesis_instruction_at_admission(&job.chat_id, &job.created_at)
                        .await?
                } else {
                    self.db
                        .main_turn_active_genesis_instruction(&job.chat_id)
                        .await?
                };
                if let Some(instruction_row) = instruction_row {
                    let instruction_id: String = instruction_row.instruction_id()?;
                    let instruction_revision: i64 = instruction_row.revision()?;
                    let instruction_body: String = instruction_row.body()?;
                    let genesis_id: String = instruction_row.genesis_id()?;
                    let genesis_version: i64 = instruction_row.genesis_version()?;
                    let genesis_lifecycle: String = instruction_row.genesis_lifecycle()?;
                    let main_skill_row = if let Some(frozen) = frozen_authority.as_ref() {
                        self.db
                            .main_turn_frozen_operating_skill(&frozen.operating_skill_revision_id)
                            .await?
                    } else {
                        self.db
                            .main_turn_active_operating_skill(MAIN_OPERATING_SKILL_KEY)
                            .await?
                    }
                    .ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Main Agent Genesis has no active server operating-skill revision",
                        )
                    })?;
                    let main_skill_revision: String =
                        main_skill_row.current_revision_id_required()?;
                    let main_skill_key: String = main_skill_row.skill_key()?;
                    let main_skill_revision_number: i64 = main_skill_row.revision()?;
                    let main_skill_schema_version: String = main_skill_row.schema_version()?;
                    let main_skill_render_version: String = main_skill_row.render_version()?;
                    let main_skill_canonical_body: String = main_skill_row.canonical_body()?;
                    let main_skill_policy_json: String = main_skill_row.policy_json()?;
                    let main_skill_policy_digest: String = main_skill_row.policy_digest()?;
                    let main_skill_content_digest: String = main_skill_row.content_digest()?;
                    let historical_skill_valid = frozen_authority.as_ref().is_some_and(|frozen| {
                        main_skill_revision == frozen.operating_skill_revision_id
                            && main_skill_key == frozen.operating_skill_key
                            && main_skill_revision_number >= 1
                            && !main_skill_schema_version.trim().is_empty()
                            && !main_skill_render_version.trim().is_empty()
                            && !main_skill_canonical_body.trim().is_empty()
                            && !main_skill_policy_json.trim().is_empty()
                            && !main_skill_policy_digest.trim().is_empty()
                            && main_skill_content_digest == frozen.operating_skill_content_digest
                    });
                    let current_skill_valid = frozen_authority.is_none()
                        && main_skill_key == MAIN_OPERATING_SKILL_KEY
                        && main_skill_revision_number >= 1
                        && main_skill_revision
                            == format!("{MAIN_OPERATING_SKILL_KEY}@{main_skill_revision_number}")
                        && main_skill_schema_version == MAIN_OPERATING_SKILL_SCHEMA_VERSION
                        && main_skill_render_version == MAIN_OPERATING_SKILL_RENDER_VERSION
                        && main_skill_canonical_body == canonical_main_operating_skill_body()
                        && main_skill_policy_json == MAIN_OPERATING_SKILL_POLICY_JSON
                        && main_skill_policy_digest == MAIN_OPERATING_SKILL_POLICY_DIGEST
                        && main_skill_content_digest == MAIN_OPERATING_SKILL_CONTENT_DIGEST;
                    if !historical_skill_valid && !current_skill_valid {
                        return Err(ServiceError::invalid_operation(
                            "Main Agent Genesis operating skill does not match this server \
                             build's canonical contract; restart the Forge server so pending \
                             migrations reseed the operating skill",
                        ));
                    }
                    let charter_id: Option<String> = instruction_row.charter_id_optional()?;
                    let charter_revision_id: Option<String> =
                        instruction_row.charter_revision_id_optional()?;
                    let charter_version: Option<i64> =
                        instruction_row.charter_version_optional()?;
                    let charter_content_digest: Option<String> =
                        instruction_row.charter_content_digest_optional()?;
                    let charter_render_digest: Option<String> =
                        instruction_row.charter_render_digest_optional()?;
                    let mut references = vec![
                        OperatingContextReference::included(
                            &genesis_id,
                            "main_genesis",
                            format!("v{genesis_version}:{genesis_lifecycle}"),
                            fingerprint_id(&format!(
                                "genesis:{genesis_id}:v{genesis_version}:lifecycle:{genesis_lifecycle}"
                            )),
                            "active_product_genesis",
                        ),
                        OperatingContextReference::included(
                            &profile.id,
                            "main_profile",
                            format!("v{}", profile.version),
                            fingerprint_id(&format!(
                                "profile:{}:v{}:policy:{}",
                                profile.id, profile.version, profile.tool_policy_json
                            )),
                            "authenticated_main_profile",
                        ),
                        OperatingContextReference::included(
                            &instruction_id,
                            "main_genesis_instruction",
                            instruction_revision.to_string(),
                            fingerprint_id(&instruction_body),
                            "active_genesis_instruction",
                        ),
                    ];
                    let account_id = chat.account_id.as_deref().ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Main Agent Chat has no authenticated account for portfolio context",
                        )
                    })?;
                    if agent.owner_id.as_deref() != Some(account_id) {
                        return Err(ServiceError::invalid_operation(
                            "Main Agent identity is not owned by the Chat account",
                        ));
                    }
                    references.extend(self.load_main_portfolio_context(account_id).await?);
                    if let (Some(charter_id), Some(charter_revision_id)) =
                        (charter_id.as_deref(), charter_revision_id.as_deref())
                    {
                        if let (Some(content_digest), Some(_)) = (
                            charter_content_digest.as_deref(),
                            charter_render_digest.as_deref(),
                        ) {
                            references.push(OperatingContextReference::included(
                                charter_id,
                                "main_charter",
                                charter_revision_id,
                                content_digest,
                                "active_genesis_charter",
                            ));
                        }
                    }
                    let instruction = replace_rendered_skill_body(
                        render_main_operating_skill(&MainOperatingSkillContext {
                            profile_text: profile.prompt_template.clone().unwrap_or_default(),
                            ..MainOperatingSkillContext::default()
                        })
                        .expect("active Genesis protocol"),
                        Some(&main_skill_canonical_body),
                        canonical_main_operating_skill_body(),
                    )?;
                    let scalars = format!(
                        "- Permission ceiling: {main_ceiling}\n- Genesis lifecycle: {genesis_lifecycle}\n- Active Product Genesis session ID: {genesis_id}\n- Genesis version: {genesis_version}\n- Current Charter revision: {}\n- Current Charter version: {}\n",
                        charter_revision_id.as_deref().unwrap_or("(none recorded)"),
                        charter_version
                            .map(|v| v.to_string())
                            .as_deref()
                            .unwrap_or("(none recorded)"),
                    );
                    let portfolio = references
                        .iter()
                        .filter(|reference| reference.source_type == "main_portfolio_projection")
                        .map(portfolio_state_display)
                        .take(8)
                        .collect::<Vec<_>>();
                    let state_card =
                        genesis_state_card_from_snapshot(&instruction_body, &scalars, &portfolio);
                    (
                        Some(instruction),
                        state_card,
                        main_operating_context_sources(
                            frozen_authority
                                .as_ref()
                                .map(|snapshot| snapshot.operating_skill_key.as_str())
                                .unwrap_or(MAIN_OPERATING_SKILL_KEY),
                            &main_skill_revision,
                            &main_skill_content_digest,
                            "server_owned_main_genesis_operating_skill",
                            "main_genesis_context",
                            &references,
                        )?,
                    )
                } else {
                    let active_genesis_id = if frozen_authority.is_some() {
                        None
                    } else {
                        self.db
                            .main_turn_active_genesis_id(&job.chat_id)
                            .await?
                            .flatten()
                    };
                    if active_genesis_id.is_some() {
                        return Err(ServiceError::invalid_operation(
                            "Active Product Genesis has no immutable Main instruction revision",
                        ));
                    }
                    // Outside Genesis the server-owned account baseline is the
                    // operating instruction, so a Main turn never reaches a
                    // model backend without knowing it is Forge's Main Agent.
                    let account_id = chat.account_id.as_deref().ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Main Agent Chat has no authenticated account for baseline context",
                        )
                    })?;
                    if agent.owner_id.as_deref() != Some(account_id) {
                        return Err(ServiceError::invalid_operation(
                            "Main Agent identity is not owned by the Chat account",
                        ));
                    }
                    let mut references = vec![OperatingContextReference::included(
                        &profile.id,
                        "main_profile",
                        format!("v{}", profile.version),
                        fingerprint_id(&format!(
                            "profile:{}:v{}:policy:{}",
                            profile.id, profile.version, profile.tool_policy_json
                        )),
                        "authenticated_main_profile",
                    )];
                    references.extend(self.load_main_portfolio_context(account_id).await?);
                    let context = MainBaselineSkillContext {
                        portfolio_references: references
                            .iter()
                            .filter(|reference| {
                                reference.source_type == "main_portfolio_projection"
                            })
                            .map(portfolio_state_display)
                            .collect(),
                        permission_ceiling: main_ceiling.clone(),
                        profile_text: profile.prompt_template.clone().unwrap_or_default(),
                    };
                    let instruction = replace_rendered_skill_body(
                        render_main_baseline_operating_skill(&context),
                        frozen_authority
                            .as_ref()
                            .map(|snapshot| snapshot.operating_skill_body.as_str()),
                        canonical_main_baseline_operating_skill_body(),
                    )?;
                    let (skill_key, skill_revision, skill_content_digest) = frozen_authority
                        .as_ref()
                        .map(|snapshot| {
                            (
                                snapshot.operating_skill_key.as_str(),
                                snapshot.operating_skill_revision_id.as_str(),
                                snapshot.operating_skill_content_digest.as_str(),
                            )
                        })
                        .unwrap_or((
                            MAIN_BASELINE_OPERATING_SKILL_KEY,
                            MAIN_BASELINE_OPERATING_SKILL_REVISION,
                            MAIN_BASELINE_OPERATING_SKILL_CONTENT_DIGEST,
                        ));
                    (
                        Some(instruction),
                        render_main_baseline_state_card(&context),
                        main_operating_context_sources(
                            skill_key,
                            skill_revision,
                            skill_content_digest,
                            "server_owned_main_baseline_operating_skill",
                            "main_baseline_context",
                            &references,
                        )?,
                    )
                }
            }
            "project" => {
                let project_id = chat.project_id.as_deref().ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Project Agent Chat has no authenticated Project scope",
                    )
                })?;
                let snapshot = self
                    .load_project_operating_skill(
                        project_id,
                        &agent,
                        &profile,
                        &chat.id,
                        frozen_authority.as_ref(),
                    )
                    .await?;
                (
                    Some(snapshot.instruction),
                    snapshot.state_card,
                    snapshot.context_sources,
                )
            }
            _ => {
                return Err(ServiceError::invalid_operation(
                    "Agent Chat has an unsupported canonical kind",
                ));
            }
        };
        if chat.kind == "project" {
            if let Some(protocol) = operating_instruction.as_mut() {
                append_project_turn_protocol(protocol);
            }
        }
        append_delivery_followup_retry_instruction(
            job.error_code.as_deref(),
            delivery_followup_postcondition(&input)?
                .as_ref()
                .map(|postcondition| postcondition.required_event_type.as_str()),
            &mut operating_instruction,
        )?;
        // D21/F18: a new Main turn's episodic context is bounded to the
        // current topic's messages, not the whole prior chat history. A
        // topic-less chat (every Project Chat today, and any Main Chat
        // before its first topic exists) has no floor, so this is a no-op
        // for them.
        let topic_floor =
            db::AgentChatTopicRepo::get_current_agent_chat_topic(&*self.db, &job.chat_id)
                .await?
                .map(|topic| topic.starting_message_sequence)
                .unwrap_or(0);
        let history = AgentChatMessageRepo::list_agent_chat_messages(
            &*self.db,
            AgentChatMessageListQuery {
                chat_id: job.chat_id.clone(),
                before_sequence: Some(input.sequence),
                page: PageRequest {
                    cursor: None,
                    limit: MAX_HISTORY,
                    include_total: false,
                    sort_by: db::SortBy::CreatedAt,
                    sort_order: db::SortOrder::Asc,
                },
            },
        )
        .await?
        .items
        .into_iter()
        .filter(|message| {
            message.sequence < input.sequence
                && message.sequence >= topic_floor
                && message.status == AgentChatMessageStatus::Complete
        })
        .collect();
        Ok(LoadedAgentChatContext {
            agent,
            profile,
            input,
            history,
            operating_instruction,
            state_card,
            operating_context_sources,
            frozen_authority,
        })
    }

    /// Load only the account owner's bounded portfolio projection for Main
    /// context. Project rows are represented by stable identifiers, current
    /// versions, and safe lifecycle pointers; names and other mutable detail
    /// remain behind the typed `portfolio.read` projection. No Project Chat,
    /// Task, repository, memory, or credential data enters Main context.
    async fn load_main_portfolio_context(
        &self,
        account_id: &str,
    ) -> Result<Vec<OperatingContextReference>> {
        let rows = self.db.main_turn_portfolio_projects(account_id).await?;
        rows.into_iter()
            .map(|row| {
                let project_id: String = row.id()?;
                let version: i64 = row.version()?;
                let updated_at: String = row.updated_at()?;
                let paused = row.paused_at()?.is_some();
                let charter_status: String = row.charter_status()?;
                let charter_revision_id: Option<String> = row.current_charter_revision_id()?;
                let digest = canonical_context_digest(&serde_json::json!({
                    "project_id": &project_id,
                    "version": version,
                    "updated_at": &updated_at,
                    "paused": paused,
                    "charter_status": &charter_status,
                    "current_charter_revision_id": &charter_revision_id,
                }))?;
                Ok(OperatingContextReference::included(
                    project_id,
                    "main_portfolio_projection",
                    format!("v{version}:{updated_at}"),
                    digest,
                    "bounded_account_portfolio_projection",
                ))
            })
            .collect()
    }

    /// Load and authenticate the complete Project operating-skill boundary.
    ///
    /// The worker intentionally does not trust the chat row, handoff prose, or
    /// Profile as an authority source.  The active binding, Project pointers,
    /// approved Charter revision, approval receipt, and selected server skill
    /// are joined and compared here before a session is admitted to a model
    /// backend.  A missing or stale pointer is an error rather than a degraded
    /// prompt: a Project Agent must not make a mutating proposal against an
    /// unproven Project state.
    async fn load_project_operating_skill(
        &self,
        project_id: &str,
        agent: &Agent,
        profile: &AgentProfile,
        project_chat_id: &str,
        frozen: Option<&FrozenTurnAuthority>,
    ) -> Result<ProjectOperatingSkillSnapshot> {
        // Authenticate the Project Agent binding before retrieving any
        // Project row.  The chat's project_id is a routing hint, not an ACL
        // grant, so cross-scope rows must never become a lookup oracle.
        let binding = if let Some(frozen) = frozen {
            ProjectAgentBindingRepo::get_project_binding(&*self.db, &frozen.binding_id)
                .await?
                .filter(|binding| binding.project_id == project_id)
                .ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Project Agent turn has no historical admitted Project binding",
                    )
                })?
        } else {
            ProjectAgentBindingRepo::get_active_project_binding(&*self.db, project_id)
                .await?
                .filter(|binding| {
                    // Legacy rows have no frozen binding.  Keep the old
                    // conservative path, which requires the active binding
                    // to still name the admitted identity.
                    binding.state == "active"
                        && binding.identity_id.as_deref() == Some(agent.id.as_str())
                })
                .ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Project Agent turn has no authenticated active Project binding",
                    )
                })?
        };
        let project = ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        // A ceiling of `{}` is textually present but grants nothing, which
        // would hand the model a turn with no tools bound — the agent then
        // narrates work it cannot perform instead of failing. Reject it here.
        if ceiling_grants_nothing(&binding.permission_ceiling_json) {
            return Err(ServiceError::invalid_operation(
                "Project Agent binding has no permission ceiling",
            ));
        }

        // These columns were added by V076 and are intentionally read as a
        // narrow row here until the orchestration repositories expose their
        // full typed binding record.  Reading the exact row also ensures a
        // stale Rust model cannot accidentally turn a missing column into a
        // permissive default.
        let binding_row = self
            .db
            .project_turn_binding_authority(&binding.id, project_id)
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "Project Agent binding disappeared while admitting the turn",
                )
            })?;
        let binding_state: String = binding_row.state()?;
        let binding_version: i64 = binding_row.version()?;
        let binding_project_id: String = binding_row.project_id()?;
        let binding_identity_id: Option<String> = binding_row.identity_id()?;
        let binding_skill_revision_id: Option<String> =
            binding_row.operating_skill_revision_id()?;
        let binding_policy_revision: String = binding_row.policy_revision()?;
        let binding_policy_digest: String = binding_row.policy_digest()?;
        let binding_charter_id: Option<String> = binding_row.charter_id_optional()?;
        let binding_charter_revision_id: Option<String> =
            binding_row.charter_revision_id_optional()?;
        let binding_charter_setup_required: i64 = binding_row.charter_setup_required()?;
        let binding_admission_receipt_id: Option<String> = binding_row.admission_receipt_id()?;
        let binding_charter_approval_id: Option<String> = binding_row.charter_approval_id()?;
        if let Some(frozen) = frozen {
            let historical_binding_version_matches = binding_version == frozen.binding_version
                || (binding_state == "replaced" && binding_version == frozen.binding_version + 1);
            if !historical_binding_version_matches
                || !binding_skill_revision_id
                    .as_deref()
                    .is_some_and(|revision| {
                        revision.starts_with(&format!("{PROJECT_OPERATING_SKILL_KEY}@"))
                    })
                || binding_policy_revision != frozen.policy_revision
                || binding_policy_digest != frozen.policy_digest
            {
                return Err(ServiceError::invalid_operation(
                    "Project Agent historical binding no longer matches turn admission",
                ));
            }
        }

        // Existing Projects intentionally remain usable while they are being
        // adopted into the Charter model.  Their Project Agent may only read
        // the bounded legacy state and propose the adoption Charter; this
        // branch never claims Charter/handoff authority and never enables
        // repository-capable work or release operations.
        if project.charter_status == "legacy_unverified" && project.charter_setup_required {
            if binding_state != "active"
                || binding_project_id != project_id
                || binding_identity_id.as_deref() != Some(agent.id.as_str())
                || binding_charter_setup_required != 1
            {
                return Err(ServiceError::invalid_operation(
                    "Legacy Project Agent setup binding is not authenticated",
                ));
            }
            let skill_row = self
                .db
                .project_turn_setup_operating_skill(PROJECT_OPERATING_SKILL_KEY)
                .await?
                .ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Project setup cannot load the server-owned operating skill",
                    )
                })?;
            let setup_skill_revision_id: String = skill_row.id()?;
            let setup_skill_key: String = skill_row.skill_key()?;
            let setup_skill_revision: i64 = skill_row.revision()?;
            let setup_skill_schema_version: String = skill_row.schema_version()?;
            let setup_skill_render_version: String = skill_row.render_version()?;
            let setup_skill_canonical_body: String = skill_row.canonical_body()?;
            let setup_skill_policy_json: String = skill_row.policy_json()?;
            let setup_skill_policy_digest: String = skill_row.policy_digest()?;
            let setup_skill_content_digest: String = skill_row.content_digest()?;
            let setup_skill_current_revision: Option<String> =
                skill_row.current_revision_id_optional()?;
            if setup_skill_key != PROJECT_OPERATING_SKILL_KEY
                || setup_skill_revision < 1
                || setup_skill_schema_version != PROJECT_OPERATING_SKILL_SCHEMA_VERSION
                || setup_skill_render_version != PROJECT_OPERATING_SKILL_RENDER_VERSION
                || setup_skill_canonical_body != canonical_project_operating_skill_body()
                || setup_skill_policy_json != PROJECT_OPERATING_SKILL_POLICY_JSON
                || setup_skill_policy_digest != PROJECT_OPERATING_SKILL_POLICY_DIGEST
                || setup_skill_content_digest != PROJECT_OPERATING_SKILL_CONTENT_DIGEST
                || setup_skill_current_revision.as_deref() != Some(setup_skill_revision_id.as_str())
                || setup_skill_revision_id
                    != format!("{PROJECT_OPERATING_SKILL_KEY}@{setup_skill_revision}")
            {
                return Err(ServiceError::invalid_operation(
                    "Project setup operating skill is not the selected server contract",
                ));
            }
            let project_identity_digest = canonical_context_digest(&serde_json::json!({
                "id": &project.id,
                "name": &project.name,
                "paused": project.paused_at.is_some(),
                "charter_status": &project.charter_status,
                "charter_setup_required": project.charter_setup_required,
                "version": project.version,
                "updated_at": &project.updated_at,
            }))?;
            let context_references = vec![
                OperatingContextReference::included(
                    project_id,
                    "project_identity",
                    format!("v{}", project.version),
                    project_identity_digest,
                    "authenticated_project_identity",
                ),
                OperatingContextReference::included(
                    project_id,
                    "project_authority",
                    "legacy_unverified",
                    canonical_context_digest(&serde_json::json!({
                        "authority": "legacy_unverified",
                        "project_id": project_id,
                    }))?,
                    "legacy_setup_restriction",
                ),
                OperatingContextReference::included(
                    project_id,
                    "project_setup",
                    "charter_adoption_required",
                    canonical_context_digest(&serde_json::json!({
                        "setup": "charter_adoption_required",
                        "project_id": project_id,
                    }))?,
                    "legacy_setup_restriction",
                ),
            ];
            let context = ProjectOperatingSkillContext {
                project_id: project_id.to_owned(),
                project_version: Some(project.version),
                charter_status: project.charter_status.clone(),
                binding_id: binding.id.clone(),
                permission_ceiling: readable_permission_ceiling(PROJECT_SETUP_PERMISSION_CEILING),
                policy_revision: None,
                charter_id: None,
                charter_revision: None,
                approval_receipt_id: None,
                project_mode: api_types::ProjectMode::Compact,
                effective_state: EffectiveProjectStateContext::default(),
                profile_text: profile.prompt_template.clone().unwrap_or_default(),
            };
            let instruction = replace_rendered_skill_body(
                render_project_operating_skill(&context),
                frozen.map(|snapshot| snapshot.operating_skill_body.as_str()),
                canonical_project_operating_skill_body(),
            )?;
            let (setup_skill_revision_id, setup_skill_content_digest) = frozen
                .map(|snapshot| {
                    (
                        snapshot.operating_skill_revision_id.as_str(),
                        snapshot.operating_skill_content_digest.as_str(),
                    )
                })
                .unwrap_or((&setup_skill_revision_id, &setup_skill_content_digest));
            let context_sources = project_operating_context_sources(
                setup_skill_revision_id,
                setup_skill_content_digest,
                &context_references,
            )?;
            return Ok(ProjectOperatingSkillSnapshot {
                instruction,
                state_card: render_project_state_card(&context),
                context_sources,
            });
        }

        if project.charter_status != "charter_backed"
            || project.charter_setup_required
            || project.current_charter_id.is_none()
            || project.current_charter_revision_id.is_none()
        {
            return Err(ServiceError::invalid_operation(
                "Project Agent turn is blocked until the Project has an approved Charter",
            ));
        }
        let binding_is_executable = if frozen.is_some() {
            matches!(binding_state.as_str(), "active" | "replaced")
        } else {
            binding_state == "active"
        };
        if !binding_is_executable
            || binding_project_id != project_id
            || binding_identity_id.as_deref() != Some(agent.id.as_str())
            || binding_charter_setup_required != 0
            || binding_policy_revision.trim().is_empty()
            || binding_policy_digest.trim().is_empty()
            || binding_admission_receipt_id.is_none()
            || binding_charter_approval_id.is_none()
        {
            return Err(ServiceError::invalid_operation(
                "Project Agent binding is not an active Charter-backed authority",
            ));
        }
        let binding_skill_revision_id = binding_skill_revision_id.ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Agent binding has no server-owned operating-skill revision",
            )
        })?;
        let binding_charter_id = binding_charter_id.ok_or_else(|| {
            ServiceError::invalid_operation("Project Agent binding has no Charter pointer")
        })?;
        let binding_charter_revision_id = binding_charter_revision_id.ok_or_else(|| {
            ServiceError::invalid_operation("Project Agent binding has no Charter revision pointer")
        })?;

        let project_charter_id = project.current_charter_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation("Project has no current Charter pointer")
        })?;
        let project_charter_revision_id = project
            .current_charter_revision_id
            .as_deref()
            .ok_or_else(|| {
                ServiceError::invalid_operation("Project has no current Charter revision pointer")
            })?;
        if binding_charter_id != project_charter_id
            || binding_charter_revision_id != project_charter_revision_id
        {
            return Err(ServiceError::invalid_operation(
                "Project Agent binding Charter pointers do not match the Project",
            ));
        }

        let charter_row = self
            .db
            .project_turn_charter_approval(
                project_id,
                project_charter_id,
                project_charter_revision_id,
                binding_charter_approval_id.as_deref(),
            )
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "Project Agent turn has no current approved Charter approval receipt",
                )
            })?;
        let charter_id: String = charter_row.charter_id_required()?;
        let charter_account_id: String = charter_row.charter_account_id()?;
        let charter_revision_id: String = charter_row.charter_revision_id_required()?;
        let current_approved_revision_id: Option<String> =
            charter_row.current_approved_revision_id()?;
        let charter_revision_lifecycle: String = charter_row.charter_revision_lifecycle()?;
        let charter_version: i64 = charter_row.charter_version_required()?;
        let expected_charter_version: i64 = charter_row.expected_charter_version()?;
        let approval_lifecycle: String = charter_row.approval_lifecycle()?;
        let approval_id: String = charter_row.approval_id()?;
        let approval_content_digest: String = charter_row.approval_content_digest()?;
        let approval_render_digest: String = charter_row.approval_render_digest()?;
        let charter_content_digest: String = charter_row.charter_content_digest_required()?;
        let charter_render_digest: String = charter_row.charter_render_digest_required()?;
        let consumed_project_id: Option<String> = charter_row.consumed_project_id()?;
        let selected_skill_revision_id: Option<String> =
            charter_row.selected_operating_skill_revision_id()?;
        let approval_event_id: Option<String> = charter_row.approval_event_id()?;
        let approval_principal_kind: String = charter_row.approving_principal_type()?;
        let approval_principal_id: String = charter_row.approving_principal_id()?;
        let approval_authorization_basis: String = charter_row.approval_authorization_basis()?;
        let approval_authorization_action: String = charter_row.approval_authorization_action()?;
        let approval_authorization_event_id: String =
            charter_row.approval_authorization_event_id()?;
        let approval_authorization_occurred_at: String =
            charter_row.approval_authorization_occurred_at()?;
        let approval_created_at: String = charter_row.approval_created_at()?;
        let expected_approval_event_id = approval_event_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Agent Charter approval has no immutable authorization event",
            )
        })?;
        let expected_skill_revision_id =
            selected_skill_revision_id.as_deref().ok_or_else(|| {
                ServiceError::invalid_operation(
                    "Project Agent Charter approval has no selected operating skill revision",
                )
            })?;
        if charter_id != project_charter_id
            || charter_revision_id != project_charter_revision_id
            || current_approved_revision_id.as_deref() != Some(project_charter_revision_id)
            || charter_revision_lifecycle != "approved"
            || approval_lifecycle != "consumed"
            || project.current_charter_version != charter_version
            || expected_charter_version + 1 != project.current_charter_version
            || approval_content_digest != charter_content_digest
            || approval_render_digest != charter_render_digest
            || charter_content_digest.trim().is_empty()
            || charter_render_digest.trim().is_empty()
            // The approval receipt immutably records the skill revision that
            // was current at approval time. The skill body is server-owned —
            // the user approved the agent and policy, not the instruction
            // text — so a later server-owned revision of the SAME skill key
            // is admissible; the next block still requires the binding to
            // carry the exact current server contract.
            || !expected_skill_revision_id
                .starts_with(&format!("{PROJECT_OPERATING_SKILL_KEY}@"))
            || consumed_project_id.as_deref() != Some(project_id)
        {
            return Err(ServiceError::invalid_operation(
                "Project Agent Charter, approval, binding, or selected policy is stale or mismatched",
            ));
        }

        let skill_row = self
            .db
            .project_turn_selected_operating_skill(
                frozen
                    .map(|snapshot| snapshot.operating_skill_revision_id.as_str())
                    .unwrap_or(&binding_skill_revision_id),
            )
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "Project Agent operating-skill revision is not present",
                )
            })?;
        let skill_id: String = skill_row.id()?;
        let skill_key: String = skill_row.skill_key()?;
        let skill_revision: i64 = skill_row.revision()?;
        let skill_schema_version: String = skill_row.schema_version()?;
        let skill_render_version: String = skill_row.render_version()?;
        let skill_canonical_body: String = skill_row.canonical_body()?;
        let skill_policy_json: String = skill_row.policy_json()?;
        let skill_policy_digest: String = skill_row.policy_digest()?;
        let skill_content_digest: String = skill_row.content_digest()?;
        let historical_skill_valid = frozen.is_some_and(|snapshot| {
            skill_id == snapshot.operating_skill_revision_id
                && skill_key == snapshot.operating_skill_key
                && skill_revision >= 1
                && !skill_schema_version.trim().is_empty()
                && !skill_render_version.trim().is_empty()
                && !skill_canonical_body.trim().is_empty()
                && !skill_policy_json.trim().is_empty()
                && !skill_policy_digest.trim().is_empty()
                && skill_content_digest == snapshot.operating_skill_content_digest
        });
        let current_skill_valid = frozen.is_none()
            && skill_id == binding_skill_revision_id
            && skill_key == PROJECT_OPERATING_SKILL_KEY
            && skill_revision >= 1
            && skill_schema_version == PROJECT_OPERATING_SKILL_SCHEMA_VERSION
            && skill_render_version == PROJECT_OPERATING_SKILL_RENDER_VERSION
            && skill_canonical_body == canonical_project_operating_skill_body()
            && skill_policy_json == PROJECT_OPERATING_SKILL_POLICY_JSON
            && skill_policy_digest == PROJECT_OPERATING_SKILL_POLICY_DIGEST
            && skill_content_digest == PROJECT_OPERATING_SKILL_CONTENT_DIGEST
            && binding_skill_revision_id
                == format!("{PROJECT_OPERATING_SKILL_KEY}@{skill_revision}")
            && skill_row.current_revision_id_optional()?.as_deref()
                == Some(binding_skill_revision_id.as_str());
        if !historical_skill_valid && !current_skill_valid {
            return Err(ServiceError::invalid_operation(
                "Project Agent operating-skill revision is not the selected server contract",
            ));
        }

        let project_owner_id = project.owner_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation(
                "Charter-backed Project has no owner for Main-to-Project authentication",
            )
        })?;
        if charter_account_id != project_owner_id {
            return Err(ServiceError::invalid_operation(
                "Project Charter account does not match the Project owner",
            ));
        }
        if agent.owner_id.as_deref() != Some(project_owner_id) {
            return Err(ServiceError::invalid_operation(
                "Project Agent identity is not owned by the Project account",
            ));
        }
        if approval_principal_kind != "user" || approval_principal_id != project_owner_id {
            return Err(ServiceError::invalid_operation(
                "Project Charter approval is not an authenticated user authorization",
            ));
        }
        let approval_event = self
            .db
            .project_turn_approval_event(&approval_id, expected_approval_event_id, project_owner_id)
            .await?;
        let approval_event = approval_event.ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Charter approval event is not an exact user authorization",
            )
        })?;
        if approval_event.authorization_basis()? != approval_authorization_basis
            || approval_event.action()? != approval_authorization_action
            || approval_event.explicit_event()? != approval_authorization_event_id
            || approval_event.occurred_at()? != approval_authorization_occurred_at
        {
            return Err(ServiceError::invalid_operation(
                "Project Charter approval event is not an exact user authorization",
            ));
        }
        let admission_receipt_id = binding_admission_receipt_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation(
                "Project Agent binding has no immutable Project admission receipt",
            )
        })?;
        let has_stable_admission_receipt = !admission_receipt_id.trim().is_empty();
        let (handoff_id, handoff_payload_hash, admission_source_kind) =
            if has_stable_admission_receipt {
                let receipt = self
                    .db
                    .project_turn_admission_receipt(admission_receipt_id, project_id)
                    .await?
                    .ok_or_else(|| {
                        ServiceError::invalid_operation(
                            "Project Agent admission receipt is missing or cross-Project",
                        )
                    })?;
                let receipt_project_id: String = receipt.project_id()?;
                let receipt_source_kind: String = receipt.source_kind()?;
                let receipt_handoff_id: Option<String> = receipt.handoff_id()?;
                let receipt_initial_approval_id: String = receipt.initial_charter_approval_id()?;
                let receipt_initial_charter_id: String = receipt.initial_charter_id()?;
                let receipt_initial_revision_id: String = receipt.initial_charter_revision_id()?;
                let receipt_payload_digest: String = receipt.payload_digest()?;
                let receipt_schema: String = receipt.validation_schema_version()?;
                if receipt_project_id != project_id
                    || receipt_initial_charter_id != charter_id
                    || receipt_payload_digest.trim().is_empty()
                    || receipt_schema != "forge.project-admission/v1"
                {
                    return Err(ServiceError::invalid_operation(
                        "Project Agent admission receipt is stale or malformed",
                    ));
                }

                match receipt_source_kind.as_str() {
                    "genesis_handoff" => {
                        let receipt_handoff_id =
                            receipt_handoff_id.as_deref().ok_or_else(|| {
                                ServiceError::invalid_operation(
                                    "Genesis Project admission receipt has no handoff",
                                )
                            })?;
                        let handoff = self
                            .db
                            .project_turn_admission_handoff(receipt_handoff_id)
                            .await?
                            .ok_or_else(|| {
                                ServiceError::invalid_operation(
                                    "Genesis Project admission handoff is missing",
                                )
                            })?;
                        let handoff_content: String = handoff.content()?;
                        let handoff_source_revisions: String = handoff.source_revisions_json()?;
                        let handoff_status: String = handoff.status()?;
                        let handoff_target_chat_id: String = handoff.target_chat_id()?;
                        let handoff_target_project_id: Option<String> =
                            handoff.target_project_id()?;
                        let packet = parse_project_handoff_packet(&handoff_source_revisions)?;
                        let packet_value: Value = serde_json::from_str(&handoff_source_revisions)
                            .map_err(|_| {
                            ServiceError::invalid_operation(
                                "Genesis Project admission handoff packet is invalid",
                            )
                        })?;
                        let authorization = serde_json::to_value(&packet.request.authorization)
                            .map_err(|_| {
                                ServiceError::invalid_operation(
                                    "Genesis Project admission authorization is invalid",
                                )
                            })?;
                        let recomputed_digest = project_handoff_request_fingerprint(
                            &packet_value,
                            &packet.request.source_revisions_json,
                            &authorization,
                        )
                        .map_err(ServiceError::invalid_operation)?;
                        if handoff_status != "delivered"
                            || handoff_target_chat_id != project_chat_id
                            || handoff_target_project_id.as_deref() != Some(project_id)
                            || packet.schema_version != PROJECT_HANDOFF_SCHEMA_VERSION
                            || packet.handoff_id != receipt_handoff_id
                            || packet.project.id != project_id
                            || packet.target.chat_id != project_chat_id
                            || packet.approval_id != receipt_initial_approval_id
                            || packet.approval.id != receipt_initial_approval_id
                            || packet.charter.id != receipt_initial_charter_id
                            || packet.charter.revision_id != receipt_initial_revision_id
                            || packet.request.source_revisions_digest != receipt_payload_digest
                            || recomputed_digest != receipt_payload_digest
                        {
                            return Err(ServiceError::invalid_operation(
                                "Genesis Project admission receipt does not match its immutable handoff",
                            ));
                        }
                        let receipt_handoff_id = receipt_handoff_id.to_owned();
                        let historical_payload_hash = hash_parts(
                            b"forge-project-handoff-payload-v1\0",
                            [
                                &receipt_handoff_id,
                                &handoff_content,
                                &handoff_source_revisions,
                            ],
                        );
                        (
                            Some(receipt_handoff_id),
                            historical_payload_hash,
                            receipt_source_kind,
                        )
                    }
                    "charter_adoption" => {
                        if receipt_handoff_id.is_some() {
                            return Err(ServiceError::invalid_operation(
                                "Charter-adoption admission receipt unexpectedly names a handoff",
                            ));
                        }
                        let initial_content_digest: Option<String> = self
                            .db
                            .project_turn_adoption_approval_digest(
                                &receipt_initial_approval_id,
                                &receipt_initial_charter_id,
                                &receipt_initial_revision_id,
                                project_id,
                            )
                            .await?;
                        if initial_content_digest.as_deref()
                            != Some(receipt_payload_digest.as_str())
                        {
                            return Err(ServiceError::invalid_operation(
                                "Charter-adoption admission receipt does not match its consumed approval",
                            ));
                        }
                        (None, receipt_payload_digest, receipt_source_kind)
                    }
                    _ => {
                        return Err(ServiceError::invalid_operation(
                            "Project Agent admission receipt has an unsupported source kind",
                        ));
                    }
                }
            } else {
                return Err(ServiceError::invalid_operation(
                    "Project Agent binding has no immutable Project admission receipt",
                ));
            };

        let project_mode = match charter_row.project_mode()?.as_str() {
            "compact" => api_types::ProjectMode::Compact,
            "standard" => api_types::ProjectMode::Standard,
            _ => {
                return Err(ServiceError::invalid_operation(
                    "Project Charter has an unsupported Project mode",
                ));
            }
        };

        let project_identity_digest = canonical_context_digest(&serde_json::json!({
            "id": &project.id,
            "name": &project.name,
            "paused": project.paused_at.is_some(),
            "charter_status": &project.charter_status,
            "charter_setup_required": project.charter_setup_required,
            "version": project.version,
            "updated_at": &project.updated_at,
            "charter_id": &charter_id,
            "charter_revision_id": &charter_revision_id,
            "charter_content_digest": &charter_content_digest,
            "charter_render_digest": &charter_render_digest,
        }))?;
        let approval_receipt_digest = canonical_context_digest(&serde_json::json!({
            "id": &approval_id,
            "event_id": expected_approval_event_id,
            "principal_type": &approval_principal_kind,
            "principal_id": &approval_principal_id,
            "authorization_basis": &approval_authorization_basis,
            "authorization_action": &approval_authorization_action,
            "authorization_occurred_at": &approval_authorization_occurred_at,
            "created_at": &approval_created_at,
            "charter_id": &charter_id,
            "revision_id": &charter_revision_id,
            "content_digest": &charter_content_digest,
            "render_digest": &charter_render_digest,
        }))?;

        let mut context_references = vec![
            OperatingContextReference::included(
                project_id,
                "project_identity",
                format!("v{}", project.version),
                project_identity_digest,
                "authenticated_project_identity",
            ),
            OperatingContextReference::included(
                &binding.id,
                "project_agent_binding",
                binding_policy_revision.clone(),
                &binding_policy_digest,
                "authenticated_project_binding_policy",
            ),
            OperatingContextReference::included(
                &skill_id,
                "server_operating_skill",
                skill_id.clone(),
                &skill_content_digest,
                "canonical_project_operating_skill",
            ),
            OperatingContextReference::included(
                &charter_id,
                "project_charter",
                charter_revision_id.clone(),
                &charter_content_digest,
                "current_approved_project_charter",
            ),
            OperatingContextReference::included(
                &approval_id,
                "charter_approval_receipt",
                expected_approval_event_id.to_owned(),
                approval_receipt_digest,
                "consumed_project_charter_approval",
            ),
        ];
        if let Some(handoff_id) = handoff_id.as_deref() {
            context_references.push(OperatingContextReference::included(
                handoff_id,
                "main_to_project_handoff_receipt",
                "forge.project-charter-handoff/v1",
                &handoff_payload_hash,
                "opaque_handoff_provenance_only",
            ));
        } else if admission_source_kind == "charter_adoption" {
            context_references.push(OperatingContextReference::included(
                admission_receipt_id,
                "project_adoption_bootstrap",
                "forge.project-admission/v1",
                &handoff_payload_hash,
                "immutable_project_admission_provenance",
            ));
        } else {
            context_references.push(OperatingContextReference::included(
                &approval_id,
                "charter_amendment_bootstrap",
                charter_revision_id.clone(),
                &handoff_payload_hash,
                "consumed_user_charter_amendment_approval",
            ));
        }

        let projection = load_effective_project_state(&self.db, project_id, Some(32)).await?;
        if projection
            .governing_charter
            .as_ref()
            .map(|charter| format!("{}@{}", charter.id, charter.revision_id))
            .as_deref()
            != Some(&format!("{charter_id}@{charter_revision_id}"))
        {
            return Err(ServiceError::invalid_operation(
                "Project effective-state projection does not match the authenticated Charter",
            ));
        }
        let effective_state = effective_state_context(&projection)?;

        for document in &projection.approved_documents {
            context_references.push(OperatingContextReference::included(
                &document.id,
                "project_document",
                &document.revision_id,
                &document.content_digest,
                "current_approved_document",
            ));
        }

        for decision in &projection.active_decisions {
            let digest = canonical_context_digest(decision)?;
            context_references.push(OperatingContextReference::included(
                &decision.id,
                "project_decision",
                &decision.created_at,
                digest,
                "active_effective_decision",
            ));
        }
        for decision in &projection.invalidated_decisions {
            let digest = canonical_context_digest(decision)?;
            context_references.push(OperatingContextReference::omitted(
                &decision.id,
                "project_decision",
                &decision.created_at,
                digest,
                "invalidated_decision",
            ));
        }

        for reconciliation in &projection.reconciliation_required {
            context_references.push(OperatingContextReference::omitted(
                &reconciliation.record_id,
                "project_reconciliation",
                &reconciliation.record_revision,
                &reconciliation.record_digest,
                "reconciliation_required",
            ));
        }
        for conflict in &projection.canonical_conflicts {
            context_references.push(OperatingContextReference::omitted(
                &conflict.id,
                "canonical_project_conflict",
                &conflict.created_at,
                &conflict.governing_record_digest,
                "canonical_conflict_blocks_affected_work",
            ));
        }

        let task_summary_digest = canonical_context_digest(&projection.task_summary)?;
        context_references.push(OperatingContextReference::included(
            project_id,
            "project_task_projection",
            &projection.source_event_watermark,
            task_summary_digest,
            "latest_server_accepted_task_versions",
        ));

        let validation_summary_digest = canonical_context_digest(&projection.validation_summary)?;
        context_references.push(OperatingContextReference::included(
            project_id,
            "project_validation_projection",
            &projection.source_event_watermark,
            validation_summary_digest,
            "latest_validation_summary",
        ));

        // Coordination state is a bounded reconciliation projection.  Only
        // server-derived ids, lifecycle/version metadata, and evidence counts
        // enter the Project prompt; inbox bodies and commitment descriptions
        // remain behind explicit scoped read tools and cannot inject prompt
        // instructions.
        for commitment in &projection.commitments {
            let digest = canonical_context_digest(commitment)?;
            context_references.push(OperatingContextReference::included(
                &commitment.id,
                "project_commitment",
                format!("v{}", commitment.version),
                digest,
                "project_commitment_reconciliation",
            ));
        }
        for item in &projection.inbox {
            let digest = canonical_context_digest(item)?;
            context_references.push(OperatingContextReference::included(
                &item.id,
                "project_inbox_reconciliation",
                format!("v{}", item.version),
                digest,
                "project_inbox_reconciliation",
            ));
        }

        for milestone in &projection.active_milestones {
            context_references.push(
                match (
                    milestone.definition_revision_id.as_deref(),
                    milestone.definition_digest.as_deref(),
                ) {
                    (Some(definition_revision), Some(definition_digest))
                        if !definition_revision.trim().is_empty()
                            && !definition_digest.trim().is_empty() =>
                    {
                        OperatingContextReference::included(
                            &milestone.id,
                            "project_milestone_definition",
                            definition_revision,
                            definition_digest,
                            "current_project_milestone_definition",
                        )
                    }
                    _ => OperatingContextReference::omitted(
                        &milestone.id,
                        "project_milestone_definition",
                        "unresolved",
                        canonical_context_digest(milestone)?,
                        "missing_milestone_definition",
                    ),
                },
            );
        }

        if let Some(readiness) = projection.readiness.latest.as_ref() {
            context_references.push(OperatingContextReference::included(
                &readiness.id,
                "project_readiness_snapshot",
                &readiness.id,
                &readiness.readiness_digest,
                "latest_readiness_snapshot",
            ));
        } else {
            context_references.push(OperatingContextReference::omitted(
                project_id,
                "project_readiness_snapshot",
                &projection.source_event_watermark,
                canonical_context_digest(&serde_json::json!({
                    "project_id": project_id,
                    "source_event_watermark": &projection.source_event_watermark,
                    "readiness": Value::Null,
                }))?,
                "no_readiness_snapshot",
            ));
        }

        for release in &projection.releases {
            let release_revision = release.release_revision.to_string();
            context_references.push(OperatingContextReference::included(
                &release.id,
                "immutable_project_release",
                &release_revision,
                &release.snapshot_digest,
                "immutable_release_history",
            ));
        }

        let unreleased = &projection.unreleased_changes;
        for (source_type, ids, reason) in [
            (
                "unreleased_document_change",
                &unreleased.document_ids,
                "current_document_draft_differs_from_approved",
            ),
            (
                "unreleased_decision_candidate",
                &unreleased.decision_candidate_ids,
                "decision_candidate_not_effective",
            ),
            (
                "unreleased_active_milestone",
                &unreleased.active_milestone_ids,
                "active_milestone_has_no_release",
            ),
            (
                "unreleased_reconciliation",
                &unreleased.reconciliation_ids,
                "reconciliation_record_remains_required",
            ),
        ] {
            for id in ids {
                let digest = canonical_context_digest(&serde_json::json!({
                    "source_type": source_type,
                    "source_id": id,
                    "source_event_watermark": &projection.source_event_watermark,
                }))?;
                context_references.push(OperatingContextReference::omitted(
                    id,
                    source_type,
                    &projection.source_event_watermark,
                    digest,
                    reason,
                ));
            }
        }

        // The state card and manifest use the same canonical projection.  Raw
        // handoff packet JSON, Main chat IDs, and Main transcript bodies never
        // enter these references.

        let context = ProjectOperatingSkillContext {
            project_id: project_id.to_owned(),
            project_version: Some(projection.project.version),
            charter_status: projection.project.charter_status.clone(),
            binding_id: binding.id,
            permission_ceiling: readable_permission_ceiling(&binding.permission_ceiling_json),
            policy_revision: Some(binding_policy_revision.clone()),
            charter_id: Some(charter_id),
            charter_revision: Some(charter_revision_id),
            approval_receipt_id: Some(approval_id),
            project_mode,
            effective_state,
            profile_text: profile.prompt_template.clone().unwrap_or_default(),
        };
        let instruction = replace_rendered_skill_body(
            render_project_operating_skill(&context),
            frozen.map(|snapshot| snapshot.operating_skill_body.as_str()),
            canonical_project_operating_skill_body(),
        )?;
        let context_sources = project_operating_context_sources(
            &skill_id,
            &skill_content_digest,
            &context_references,
        )?;
        Ok(ProjectOperatingSkillSnapshot {
            instruction,
            state_card: render_project_state_card(&context),
            context_sources,
        })
    }

    pub(super) async fn run_typed_turn(
        &self,
        job: &AgentChatTurnJob,
        cancellation: CancellationToken,
    ) -> std::result::Result<CompletedAgentChatTurn, TurnRunError> {
        let turn = self.load_turn(job).await?;
        match turn.profile.backend_kind.as_str() {
            "native" => self.run_native(job, turn, cancellation).await,
            "cli" => self.run_cli(job, turn, cancellation).await,
            _ => Err(TurnRunError {
                failure: TurnFailure::Configuration,
                error: ServiceError::invalid_operation(
                    "selected Agent Chat backend is unsupported",
                ),
            }),
        }
    }

    async fn run_native(
        &self,
        job: &AgentChatTurnJob,
        turn: LoadedAgentChatTurn,
        cancellation: CancellationToken,
    ) -> std::result::Result<CompletedAgentChatTurn, TurnRunError> {
        let LoadedAgentChatTurn {
            agent,
            profile,
            session,
            input,
            history,
            operating_instruction,
            state_card,
            operating_context_sources,
        } = turn;
        let owner_user_id = agent.owner_id.as_deref().ok_or_else(|| TurnRunError {
            failure: TurnFailure::Authority,
            error: ServiceError::invalid_operation("Agent identity has no owner"),
        })?;
        let credential_ref = profile
            .credential_ref
            .as_deref()
            .ok_or_else(|| TurnRunError::configuration("Agent profile has no credential"))?;
        let config: NativeProfileConfig = serde_json::from_str(&profile.config_json)
            .map_err(|_| TurnRunError::configuration("Agent profile config is invalid"))?;
        let runtime_session_id = session
            .runtime_session_id
            .clone()
            .ok_or_else(|| TurnRunError::configuration("Agent session has no runtime id"))?;
        let provider = profile
            .provider
            .clone()
            .ok_or_else(|| TurnRunError::configuration("Agent profile has no provider"))?;
        let model = profile
            .model
            .clone()
            .ok_or_else(|| TurnRunError::configuration("Agent profile has no model"))?;
        let provider_account_id =
            CredentialHandleRepo::get_credential_handle(&*self.db, credential_ref)
                .await?
                .as_ref()
                .and_then(crate::embedded_agent_service::entry_provider_account_id);
        // Both Agents run their turn inside their own workspace. The Project
        // Agent gets `forge/` for what it writes and `checkout/` for the
        // software it exercises; the Main Agent gets an account scratch
        // directory that holds no repository at all.
        let chat = AgentChatRepo::get_agent_chat(&*self.db, &job.chat_id).await?;
        let (turn_workspace, turn_workspace_access) =
            match chat.as_ref().and_then(|chat| chat.project_id.clone()) {
                Some(project_id) => (
                    self.embedded_agents
                        .project_agent_workspace(&project_id)
                        .await,
                    WorkspaceAccess::ProjectVerify,
                ),
                None => match chat.as_ref().and_then(|chat| chat.account_id.clone()) {
                    Some(account_id) => (
                        self.embedded_agents.main_agent_workspace(&account_id).await,
                        WorkspaceAccess::AccountScratch,
                    ),
                    None => (None, WorkspaceAccess::Deny),
                },
            };
        // A Project Agent's commands run in that Project's checkout, so the
        // Project's own settings layer over the configured baseline; the Main
        // Agent's scratch belongs to the account and takes the baseline.
        let command_allowlist = self
            .embedded_agents
            .effective_command_allowlist(chat.as_ref().and_then(|chat| chat.project_id.as_deref()))
            .await;
        let environment =
            if let Some(project_id) = chat.as_ref().and_then(|chat| chat.project_id.as_deref()) {
                match db::ProjectRepo::get_by_id(&*self.db, project_id).await? {
                    Some(project) => crate::lifecycle::project_env(&project.settings),
                    None => Default::default(),
                }
            } else {
                Default::default()
            };
        let turn_log = self.turn_log_sink(job).await;
        let started = std::time::Instant::now();
        let mut native_request = AgentTurnRequest {
            forge_session_id: session.id.clone(),
            runtime_session_id,
            scope: CanonicalScope {
                scope_type: CanonicalScopeType::AgentChat,
                scope_id: job.chat_id.clone(),
                workspace_access: turn_workspace
                    .as_ref()
                    .map_or(WorkspaceAccess::Deny, |_| turn_workspace_access),
            },
            workspace_path: turn_workspace
                .as_ref()
                .map(|path| path.to_string_lossy().into_owned()),
            provider: {
                let (context_tokens, max_input_tokens, max_output_tokens) =
                    crate::embedded_agent_service::effective_native_limits(
                        &provider,
                        config.context_tokens,
                        config.max_input_tokens,
                        config.max_output_tokens,
                    );
                NativeProviderConfig {
                    provider,
                    base_url: config.base_url,
                    model: model.clone(),
                    reasoning_effort: profile.reasoning_effort.clone(),
                    credential_handle_id: credential_ref.to_owned(),
                    owner_user_id: owner_user_id.to_owned(),
                    provider_account_id,
                    context_tokens,
                    max_input_tokens,
                    max_output_tokens,
                }
            },
            system_prompt: compose_system_prompt(
                profile.prompt_template.as_deref(),
                operating_instruction.as_deref(),
            ),
            history: runtime_history(&history),
            input: input.content,
            server_state_card: Some(state_card),
            command_allowlist: Some(command_allowlist),
            environment,
            cancellation,
        };
        let output = match self
            .embedded_agents
            .run_native_turn(&mut native_request, turn_log)
            .await
        {
            Ok(output) => output,
            Err(error) => {
                let failure = error.turn_failure();
                if matches!(
                    &error,
                    forge_agent_host::AgentHostError::RuntimeWithUsage {
                        provider_auth_rejected: true,
                        ..
                    }
                ) {
                    if let Err(health_error) =
                        crate::provider_health::record_chat_auth_rejection(&self.db, credential_ref)
                            .await
                    {
                        tracing::warn!(turn_job_id = %job.id, error = %health_error, "provider auth health could not be recorded");
                    }
                }
                let message = error.to_string();
                if let forge_agent_host::AgentHostError::RuntimeWithUsage {
                    usage_reports, ..
                } = &error
                {
                    let mapped = usage_reports
                        .iter()
                        .enumerate()
                        .map(|(sequence, report)| {
                            crate::chat_usage::usage_report_from_host(
                                report,
                                "chat",
                                job.invocation_count.saturating_sub(1),
                                u32::try_from(sequence).map_err(|_| {
                                    ServiceError::invalid_operation(
                                        "usage report sequence overflows",
                                    )
                                })?,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    self.remember_usage(&job.id, mapped).await;
                }
                return Err(TurnRunError {
                    failure,
                    error: ServiceError::invalid_operation(format!(
                        "native Agent Chat turn failed: {message}"
                    )),
                });
            }
        };
        let session = if native_request.forge_session_id == session.id {
            session
        } else {
            self.db
                .get_agent_session(&native_request.forge_session_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::not_found("agent_session", &native_request.forge_session_id)
                })?
        };
        let mut usage_reports = output
            .usage_reports
            .iter()
            .enumerate()
            .map(|(sequence, report)| {
                crate::chat_usage::usage_report_from_host(
                    report,
                    "chat",
                    job.invocation_count.checked_sub(1).ok_or_else(|| {
                        ServiceError::invalid_operation("chat attempt count is invalid")
                    })?,
                    u32::try_from(sequence).map_err(|_| {
                        ServiceError::invalid_operation("usage report sequence overflows")
                    })?,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        if usage_reports.is_empty() {
            let attempt_ordinal = job
                .invocation_count
                .checked_sub(1)
                .ok_or_else(|| ServiceError::invalid_operation("chat attempt count is invalid"))?;
            let counters = executors::UsageCounters {
                input_tokens: Some(output.input_tokens),
                output_tokens: Some(output.output_tokens),
                cache_read_tokens: Some(output.cache_read_tokens),
                cache_write_tokens: Some(output.cache_write_tokens),
            };
            usage_reports.push(executors::UsageReport {
                report_id: format!("{}:native", job.id),
                request_id: None,
                report_sequence: 0,
                candidate_key: Some("chat".to_owned()),
                attempt_ordinal: u32::try_from(attempt_ordinal).map_err(|_| {
                    ServiceError::invalid_operation("chat attempt ordinal overflows")
                })?,
                provider_id: profile.provider.clone(),
                model_id: Some(model.clone()),
                counters,
                telemetry_state: match output.telemetry_state {
                    forge_agent_host::AgentTurnTelemetryState::Metered => {
                        executors::UsageTelemetryState::Metered
                    }
                    forge_agent_host::AgentTurnTelemetryState::Unmetered => {
                        executors::UsageTelemetryState::Unmetered
                    }
                },
                context_tokens: None,
                selected_tier: None,
                reported_cost_usd: None,
                outcome: None,
                partial: false,
            });
        }
        self.remember_usage(&job.id, usage_reports).await;
        let content = output.text.trim().to_owned();
        guard_agent_chat_content(&content)?;
        let context_manifest_id = if let Some(manifest) = output.context_manifest.as_ref() {
            Some(
                self.persist_runtime_context_manifest(
                    job,
                    &agent,
                    &profile,
                    &session,
                    Some(&model),
                    manifest,
                    &operating_context_sources,
                )
                .await?,
            )
        } else if !operating_context_sources.is_empty() {
            Some(
                self.persist_server_context_manifest(
                    job,
                    &agent,
                    &profile,
                    &session,
                    Some(&model),
                    &operating_context_sources,
                )
                .await?,
            )
        } else {
            None
        };
        Ok(CompletedAgentChatTurn {
            identity_id: agent.id,
            profile_id: profile.id,
            session_id: session.id,
            model: Some(model),
            content,
            // Typed ledger settlement is the accounting authority. Keep the
            // legacy field empty on all newly admitted turns; public
            // projections read the immutable ledger events instead.
            token_usage_json: None,
            duration_ms: checked_duration_ms(started.elapsed())?,
            context_manifest_id,
            pending_interaction_id: output.pending_interaction_id,
        })
    }
}
