//! Owner, repository, chat, and Project binding reconciliation for Solo bootstrap.

use super::*;

impl SoloBootstrapService {
    pub(super) async fn ensure_owner(
        &self,
        request: &SoloBootstrapRequest,
        existing_metadata: Option<&SoloProjectMetadata>,
    ) -> Result<String> {
        let key = owner_setting_key(&request.repository_id);
        if let Some(value) = SystemSettingRepo::get_setting(&*self.db, &key).await? {
            let binding = serde_json::from_str::<SoloOwnerBinding>(&value)
                .map_err(|error| conflict(format!("Solo owner setting is malformed: {error}")))?;
            verify_owner_binding(&binding, request)?;
            let user = UserRepo::get_user_by_id(&*self.db, &binding.owner_id)
                .await?
                .ok_or_else(|| conflict("Solo owner setting points to a missing user"))?;
            if user.id != binding.owner_id {
                return Err(conflict("Solo owner setting resolved to the wrong user"));
            }
            verify_internal_owner(&user, &request.repository_id)?;
            return Ok(binding.owner_id);
        }

        // A response can be lost after Project creation but before writing the
        // setting.  Project metadata is authoritative for recovering that
        // owner; creating a second principal would break scope identity.
        let owner_id = if let Some(metadata) = existing_metadata {
            let user = UserRepo::get_user_by_id(&*self.db, &metadata.owner_id)
                .await?
                .ok_or_else(|| conflict("Solo Project owner is missing from the user table"))?;
            if user.id != metadata.owner_id {
                return Err(conflict("Solo Project owner metadata is invalid"));
            }
            verify_internal_owner(&user, &request.repository_id)?;
            metadata.owner_id.clone()
        } else {
            let email = owner_email(&request.repository_id);
            if let Some(user) = UserRepo::get_user_by_email(&*self.db, &email).await? {
                user.id
            } else {
                let id = db::new_uuid_v4();
                let now = db::now_rfc3339();
                // This is deliberately not a password hash.  The owner is an
                // internal, non-login principal; the random value is retained
                // only to satisfy the legacy User schema and is never returned
                // or logged.
                UserRepo::create_user(
                    &*self.db,
                    &User {
                        id: id.clone(),
                        email,
                        password_hash: format!("forge-solo-unrecoverable:{}", db::new_uuid_v4()),
                        display_name: Some("Forge Solo owner".to_owned()),
                        is_admin: false,
                        created_at: now.clone(),
                        updated_at: now,
                    },
                )
                .await?;
                id
            }
        };

        let owner = UserRepo::get_user_by_id(&*self.db, &owner_id)
            .await?
            .ok_or_else(|| conflict("Solo owner principal disappeared during bootstrap"))?;
        verify_internal_owner(&owner, &request.repository_id)?;

        let binding = SoloOwnerBinding {
            contract_revision: SOLO_BOOTSTRAP_CONTRACT_REVISION.to_owned(),
            repository_id: request.repository_id.clone(),
            canonical_repository: request.canonical_repository.clone(),
            data_root: request.data_root.clone(),
            owner_id: owner_id.clone(),
        };
        let value = serde_json::to_string(&binding).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "Solo owner setting is not serializable: {error}"
            ))
        })?;
        SystemSettingRepo::set_setting(&*self.db, &key, &value, &db::now_rfc3339()).await?;
        Ok(owner_id)
    }

    pub(super) async fn persist_selection_metadata(
        &self,
        mut project: Project,
        metadata: &SoloProjectMetadata,
    ) -> Result<Project> {
        let current_metadata = parse_project_metadata(&project)?;
        if current_metadata == *metadata {
            return Ok(project);
        }
        if !same_immutable_metadata(&current_metadata, metadata) {
            return Err(conflict(
                "Solo Project selection metadata conflicts with the durable setup",
            ));
        }
        let settings = update_project_settings(
            &project.settings,
            metadata,
            metadata.selected_worker_agent_id.as_deref(),
        )?;
        let expected_version = project.version;
        project = ProjectRepo::update_at_version(
            &*self.db,
            UpdateProject {
                id: project.id.clone(),
                name: None,
                settings: Some(settings),
                primary_repo_id: None,
                paused_at: None,
                updated_at: db::now_rfc3339(),
            },
            expected_version,
            None,
        )
        .await
        .map_err(|error| match error {
            db::DbError::VersionConflict => ServiceError::Conflict(
                "Solo Project changed while confirming Agent selection".to_owned(),
            ),
            other => ServiceError::Db(other),
        })?;
        Ok(project)
    }

    pub(super) async fn ensure_owner_membership(
        &self,
        project: &Project,
        owner_id: &str,
    ) -> Result<ProjectMember> {
        let members = ProjectMemberRepo::list_members(&*self.db, &project.id).await?;
        if members.len() > 1 {
            return Err(conflict(
                "Solo Project contains more than its one internal owner membership",
            ));
        }
        if let Some(member) = members.into_iter().next() {
            if member.user_id != owner_id || member.role != "owner" {
                return Err(conflict(
                    "Solo Project owner membership does not match the internal owner",
                ));
            }
            return Ok(member);
        }
        let now = db::now_rfc3339();
        match ProjectMemberRepo::add_member(
            &*self.db,
            CreateProjectMember {
                id: db::new_uuid_v4(),
                project_id: project.id.clone(),
                user_id: owner_id.to_owned(),
                role: "owner".to_owned(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        {
            Ok(member) => Ok(member),
            Err(error) => {
                // A concurrent/replayed insert is reconciled by the typed
                // read.  Other errors are not hidden as partial success.
                if let Some(member) =
                    ProjectMemberRepo::get_member(&*self.db, &project.id, owner_id).await?
                {
                    if member.role == "owner" {
                        return Ok(member);
                    }
                }
                Err(error.into())
            }
        }
    }

    pub(super) async fn ensure_local_repo(
        &self,
        project: &mut Project,
        request: &SoloBootstrapRequest,
    ) -> Result<Repo> {
        let repos = list_project_repos(&self.db, &project.id).await?;
        let primary = match project.primary_repo_id.as_deref() {
            Some(primary_id) => Some(
                repos
                    .iter()
                    .find(|repo| repo.id == primary_id)
                    .cloned()
                    .ok_or_else(|| conflict("Solo Project primary Repo is missing"))?,
            ),
            None => None,
        };
        if let Some(primary) = primary.as_ref() {
            verify_local_repo_identity(primary, request)?;
            if repos
                .iter()
                .any(|repo| repo.id != primary.id && is_same_source_path(repo, request))
            {
                return Err(conflict(
                    "Solo Project has more than one Repo for the local source checkout",
                ));
            }
            return self.reconcile_repo_source_path(primary, request).await;
        }

        let matching = repos
            .iter()
            .filter(|repo| is_same_repo_identity(repo, request))
            .cloned()
            .collect::<Vec<_>>();
        if matching.len() > 1 {
            return Err(conflict(
                "Solo Project has ambiguous Repo bindings for this repository identity",
            ));
        }
        let repo = if let Some(repo) = matching.into_iter().next() {
            if repos
                .iter()
                .any(|other| other.id != repo.id && is_same_source_path(other, request))
            {
                return Err(conflict(
                    "Solo Project has more than one Repo for the local source checkout",
                ));
            }
            self.reconcile_repo_source_path(&repo, request).await?
        } else {
            if !repos.is_empty() {
                return Err(conflict(
                    "Solo Project contains a Repo that does not match this repository identity",
                ));
            }
            let now = db::now_rfc3339();
            RepoRepo::create(
                &*self.db,
                CreateRepo {
                    id: db::new_uuid_v4(),
                    project_id: project.id.clone(),
                    name: repository_name(request),
                    // The canonical source identity is stable across checkout
                    // moves and avoids a fabricated network origin.
                    remote_url: Some(request.canonical_repository.clone()),
                    local_path: Some(request.source_path().to_owned()),
                    default_branch: request.default_branch.clone(),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await?
        };
        if project.primary_repo_id.is_none() {
            let expected_version = project.version;
            match ProjectRepo::update_at_version(
                &*self.db,
                UpdateProject {
                    id: project.id.clone(),
                    name: None,
                    settings: None,
                    primary_repo_id: Some(Some(repo.id.clone())),
                    paused_at: None,
                    updated_at: db::now_rfc3339(),
                },
                expected_version,
                None,
            )
            .await
            {
                Ok(updated) => *project = updated,
                Err(db::DbError::VersionConflict) => {
                    let current = ProjectRepo::get_by_id(&*self.db, &project.id)
                        .await?
                        .ok_or_else(|| ServiceError::not_found("project", project.id.clone()))?;
                    if current.primary_repo_id.as_deref() != Some(repo.id.as_str()) {
                        return Err(conflict(
                            "Solo Project primary Repo changed during bootstrap",
                        ));
                    }
                    *project = current;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(repo)
    }

    async fn reconcile_repo_source_path(
        &self,
        repo: &Repo,
        request: &SoloBootstrapRequest,
    ) -> Result<Repo> {
        if repo.local_path.as_deref() == Some(request.source_path()) {
            return Ok(repo.clone());
        }
        RepoRepo::update(
            &*self.db,
            UpdateRepo {
                id: repo.id.clone(),
                name: None,
                local_path: Some(Some(request.source_path().to_owned())),
                remote_url: None,
                default_branch: None,
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .map_err(Into::into)
    }

    pub(super) async fn ensure_chat(&self, project: &Project) -> Result<AgentChat> {
        let chat = AgentChatRepo::get_project_chat(&*self.db, &project.id)
            .await?
            .ok_or_else(|| conflict("Solo Project is missing its canonical Project Chat"))?;
        if chat.kind != "project" || chat.project_id.as_deref() != Some(project.id.as_str()) {
            return Err(conflict("Solo Project Chat is not bound to the Project"));
        }
        Ok(chat)
    }

    pub(super) async fn ensure_binding(
        &self,
        project: &Project,
        chat: &AgentChat,
        metadata: &SoloProjectMetadata,
    ) -> Result<ProjectAgentBinding> {
        let binding = ProjectAgentBindingRepo::get_active_project_binding(&*self.db, &project.id)
            .await?
            .ok_or_else(|| conflict("Solo Project is missing its canonical Agent binding"))?;
        match project.charter_status.as_str() {
            "legacy_unverified" if project.charter_setup_required => match binding.state.as_str() {
                PROJECT_AGENT_BINDING_SETUP_STATE => {
                    if binding.identity_id.is_some()
                        || binding.profile_id.is_some()
                        || !binding.charter_setup_required
                        || chat.status != PROJECT_CHAT_SETUP_STATUS
                    {
                        return Err(conflict(
                            "Solo Project setup binding/chat is not in the legacy adoption state",
                        ));
                    }
                }
                "active" => {
                    let Some(selected_id) = metadata.selected_project_agent_id.as_deref() else {
                        return Err(conflict(
                            "active legacy Project Agent binding has no confirmed Solo selection",
                        ));
                    };
                    let Some(identity_id) = binding.identity_id.as_deref() else {
                        return Err(conflict(
                            "active legacy Project Agent binding has no identity",
                        ));
                    };
                    let Some(profile_id) = binding.profile_id.as_deref() else {
                        return Err(conflict(
                            "active legacy Project Agent binding has no profile",
                        ));
                    };
                    let agent = AgentRepo::get_by_id(&*self.db, identity_id)
                        .await?
                        .ok_or_else(|| {
                            conflict("active legacy Project Agent binding identity is missing")
                        })?;
                    if identity_id != selected_id
                        || agent.profile_id != profile_id
                        || !binding.charter_setup_required
                        || binding.admission_receipt_id.is_some()
                        || binding.charter_approval_id.is_some()
                        || binding.charter_id.is_some()
                        || binding.charter_revision_id.is_some()
                        || chat.status != PROJECT_CHAT_READY_STATUS
                        || binding.permission_ceiling_json != SOLO_PROJECT_PERMISSION_CEILING_JSON
                    {
                        return Err(conflict(
                                "active legacy Project Agent binding/chat is not in the adoption-only state",
                            ));
                    }
                    let current_skill =
                        ProjectAdmissionReceiptRepo::get_current_project_operating_skill_revision(
                            &*self.db,
                        )
                        .await?;
                    if binding.operating_skill_revision_id.as_deref() != current_skill.as_deref() {
                        return Err(conflict(
                                "active legacy Project Agent binding does not use the current Project operating skill",
                            ));
                    }
                }
                _ => {
                    return Err(conflict(
                        "Solo Project setup binding/chat is not in the legacy adoption state",
                    ));
                }
            },
            "charter_backed" if !project.charter_setup_required => {
                if binding.state != "active"
                    || binding.identity_id.is_none()
                    || binding.profile_id.is_none()
                    || binding.charter_setup_required
                    || chat.status != PROJECT_CHAT_READY_STATUS
                {
                    return Err(conflict(
                        "Solo Project Charter-backed binding/chat is inconsistent",
                    ));
                }
                if ProjectAdmissionReceiptRepo::resolve_current_project_binding_authority(
                    &*self.db,
                    &project.id,
                )
                .await?
                .is_none()
                {
                    return Err(conflict(
                        "Solo Project Charter-backed binding has no admission authority",
                    ));
                }
            }
            _ => {
                return Err(conflict(
                    "Solo Project Charter state is not a supported adoption or ready state",
                ));
            }
        }
        Ok(binding)
    }

    pub(super) async fn activate_project_agent_for_adoption(
        &self,
        project: &Project,
        metadata: &SoloProjectMetadata,
        binding: &ProjectAgentBinding,
    ) -> Result<ProjectAgentBinding> {
        let identity_id = metadata
            .selected_project_agent_id
            .as_deref()
            .ok_or_else(|| conflict("Project Agent adoption requires a confirmed identity"))?;
        if binding.state != PROJECT_AGENT_BINDING_SETUP_STATE {
            return Ok(binding.clone());
        }
        let agent_chat_service = services_agents::AgentChatService::new(Arc::clone(&self.db));
        agent_chat_service
            .set_project_binding(services_agents::SetProjectAgentBindingInput {
                actor_user_id: metadata.owner_id.clone(),
                project_id: project.id.clone(),
                identity_id: Some(identity_id.to_owned()),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: SOLO_PROJECT_PERMISSION_CEILING_JSON.to_owned(),
                subscriptions_json: "[]".to_owned(),
                // This is the existing typed binding field; a one-turn
                // adoption budget is sufficient and does not become Task
                // execution authority.
                wake_budget: 1,
                expected_version: Some(binding.version),
                replacement_reason: Some(
                    "Forge Solo confirmed Project Agent; Charter adoption pending".to_owned(),
                ),
            })
            .await
    }
}

fn same_immutable_metadata(left: &SoloProjectMetadata, right: &SoloProjectMetadata) -> bool {
    left.contract_revision == right.contract_revision
        && left.repository_id == right.repository_id
        && left.canonical_repository == right.canonical_repository
        && left.data_root == right.data_root
        && left.default_branch == right.default_branch
        && left.idempotency_key == right.idempotency_key
        && left.input_digest == right.input_digest
        && left.owner_id == right.owner_id
        && left.workflow_template_name == right.workflow_template_name
}

/// Merge a newly confirmed setup selection into the existing Project settings.
/// The settings object may contain unrelated user-controlled keys, so only
/// the Solo metadata, workflow marker, and worker assignment are reconciled.
/// A conflicting worker assignment is never silently replaced.
fn update_project_settings(
    settings_json: &str,
    metadata: &SoloProjectMetadata,
    worker_id: Option<&str>,
) -> Result<String> {
    let mut settings = serde_json::from_str::<Value>(settings_json)
        .map_err(|error| conflict(format!("Solo Project settings are malformed: {error}")))?;
    let object = settings
        .as_object_mut()
        .ok_or_else(|| conflict("Solo Project settings must be a JSON object"))?;

    let current_metadata = object
        .get(SOLO_PROJECT_SETTINGS_KEY)
        .ok_or_else(|| conflict("Solo Project is missing its Solo binding metadata".to_owned()))?;
    let current_metadata = serde_json::from_value::<SoloProjectMetadata>(current_metadata.clone())
        .map_err(|error| {
            conflict(format!(
                "Solo Project binding metadata is malformed: {error}"
            ))
        })?;
    if !same_immutable_metadata(&current_metadata, metadata) {
        return Err(conflict(
            "Solo Project selection update conflicts with immutable metadata",
        ));
    }
    object.insert(
        SOLO_PROJECT_SETTINGS_KEY.to_owned(),
        serde_json::to_value(metadata).map_err(|error| {
            ServiceError::invalid_operation(format!(
                "Solo Project metadata is not serializable: {error}"
            ))
        })?,
    );

    if let Some(existing_template) = object
        .get("workflow_template_name")
        .and_then(Value::as_str)
        .filter(|name| *name != SOLO_WORKFLOW_TEMPLATE_NAME)
    {
        return Err(conflict(format!(
            "Solo Project workflow template is not autonomous_v1: {existing_template}"
        )));
    }
    object.insert(
        "workflow_template_name".to_owned(),
        Value::String(SOLO_WORKFLOW_TEMPLATE_NAME.to_owned()),
    );

    let assignments = object
        .entry("default_role_assignments".to_owned())
        .or_insert_with(|| Value::Array(Vec::new()));
    let assignments = assignments
        .as_array_mut()
        .ok_or_else(|| conflict("default_role_assignments must be an array"))?;
    let worker_positions = assignments
        .iter()
        .enumerate()
        .filter(|(_, assignment)| {
            assignment.get("role_name").and_then(Value::as_str) == Some(default_roles::WORKER)
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if worker_positions.len() > 1 {
        return Err(conflict(
            "Solo Project has more than one default Task Worker assignment",
        ));
    }

    match (worker_id, worker_positions.first().copied()) {
        (Some(worker_id), Some(index)) => {
            let assignment = assignments
                .get(index)
                .and_then(Value::as_object)
                .ok_or_else(|| conflict("default Task Worker assignment must be an object"))?;
            let existing_type = assignment
                .get("assignee_type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let existing_id = assignment
                .get("assignee_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if existing_type != "agent" || existing_id != worker_id {
                return Err(conflict(
                    "default Task Worker assignment conflicts with Solo selection",
                ));
            }
        }
        (Some(worker_id), None) => assignments.push(json!({
            "role_name": default_roles::WORKER,
            "assignee_type": "agent",
            "assignee_id": worker_id,
        })),
        (None, Some(_)) => {
            return Err(conflict(
                "default Task Worker assignment exists without a Solo Worker selection",
            ));
        }
        (None, None) => {}
    }

    serde_json::to_string(&settings).map_err(|error| {
        ServiceError::invalid_operation(format!(
            "Solo Project settings are not serializable: {error}"
        ))
    })
}

fn owner_setting_key(repository_id: &str) -> String {
    format!("{SOLO_OWNER_SETTING_PREFIX}{repository_id}")
}

fn owner_email(repository_id: &str) -> String {
    format!("forge-solo-owner+{repository_id}@invalid.local")
}

fn verify_owner_binding(binding: &SoloOwnerBinding, request: &SoloBootstrapRequest) -> Result<()> {
    if binding.contract_revision != SOLO_BOOTSTRAP_CONTRACT_REVISION
        || binding.repository_id != request.repository_id
        || binding.canonical_repository != request.canonical_repository
        || binding.data_root != request.data_root
        || binding.owner_id.trim().is_empty()
    {
        return Err(conflict(
            "Solo owner setting is bound to a different repository or data root",
        ));
    }
    Ok(())
}

fn verify_internal_owner(user: &User, repository_id: &str) -> Result<()> {
    if user.email != owner_email(repository_id) || user.is_admin {
        return Err(conflict(
            "Solo owner principal does not match the internal repository-scoped owner",
        ));
    }
    Ok(())
}

fn repository_name(request: &SoloBootstrapRequest) -> String {
    request
        .repository_name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .map(str::trim)
        .map(str::to_owned)
        .or_else(|| {
            Path::new(request.source_path())
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| !name.trim().is_empty())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "local".to_owned())
}

fn is_same_repo_identity(repo: &Repo, request: &SoloBootstrapRequest) -> bool {
    repo.remote_url.as_deref() == Some(request.canonical_repository.as_str())
        && repo.default_branch == request.default_branch
}

fn is_same_source_path(repo: &Repo, request: &SoloBootstrapRequest) -> bool {
    repo.local_path.as_deref() == Some(request.source_path())
}

fn verify_local_repo_identity(repo: &Repo, request: &SoloBootstrapRequest) -> Result<()> {
    if !is_same_repo_identity(repo, request) {
        return Err(conflict(
            "Solo Project primary Repo does not match the selected local repository",
        ));
    }
    Ok(())
}
