//! Persisted runtime and CLI chat scope authorization.

use super::*;

impl SqliteProtectedRuntimeStore {
    /// Loads the server-issued identity/scope binding for one runtime session.
    ///
    /// The optional Task workspace is authorized by the exact host-supplied path;
    /// a path that is not the current persisted workspace is therefore
    /// rejected before RuntimeBuilder receives a filesystem-capable tool.
    pub(crate) async fn runtime_scope_binding(
        &self,
        forge_session_id: &str,
        runtime_session_id: &str,
        workspace_path: Option<&str>,
    ) -> Result<crate::RuntimeScopeBinding, crate::AgentHostError> {
        self.session_scope_binding(forge_session_id, Some(runtime_session_id), workspace_path)
            .await
    }

    /// Resolve a CLI Chat through its persisted identity, profile and scope,
    /// using the same permission intersection as native sessions.
    pub async fn cli_chat_scope_binding(
        &self,
        forge_session_id: &str,
    ) -> Result<crate::RuntimeScopeBinding, crate::AgentHostError> {
        let mut binding = self
            .session_scope_binding(forge_session_id, None, None)
            .await?;
        if binding.scope.scope_type != crate::CanonicalScopeType::AgentChat {
            return Err(crate::AgentHostError::Authority(
                "CLI Chat tools require a persisted Agent Chat scope".to_owned(),
            ));
        }
        // Native Agent Chat sessions may be provisioned with a Project
        // verification checkout or Main account scratch directory. A CLI
        // transport must never inherit either filesystem boundary: validate
        // the full persisted binding above, then narrow the transport view to
        // the same canonical Agent Chat with no workspace before composing
        // tools. Keep the authenticated identity, owning Project, setup state,
        // and persisted permission ceiling intact while applying the denied
        // scope ceiling once more at this boundary.
        let project_agent_chat = binding.agent_chat_project_id.is_some();
        if binding.scope.workspace_access != crate::WorkspaceAccess::Deny {
            binding.scope.workspace_access = crate::WorkspaceAccess::Deny;
            binding.workspace_path = None;
        }
        intersect_permissions(
            &mut binding.allowed_permissions,
            &scope_permission_set(
                crate::CanonicalScopeType::AgentChat,
                crate::WorkspaceAccess::Deny,
                project_agent_chat,
                binding.project_charter_setup_required,
            ),
        );
        binding.scope.validate()?;
        Ok(binding)
    }

    async fn session_scope_binding(
        &self,
        forge_session_id: &str,
        runtime_session_id: Option<&str>,
        workspace_path: Option<&str>,
    ) -> Result<crate::RuntimeScopeBinding, crate::AgentHostError> {
        let row = { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0); sqlx::query(
            "SELECT session.identity_id, session.profile_id,
                    identity.account_permission_ceiling,
                    identity.paused,
                    identity.archived_at,
                    profile.tool_policy_json,
                    scope.scope_type,
                    scope.scope_id,
                    scope.project_id,
                    scope.task_role,
                    scope.workspace_access,
                    chat.kind AS agent_chat_kind,
                    chat.project_id AS agent_chat_project_id,
                    binding.permission_ceiling_json AS binding_permission_ceiling,
                    bound_project.charter_setup_required AS project_charter_setup_required,
                    scope.workspace_path
             FROM agent_session AS session
             JOIN agent_identity AS identity
               ON identity.id = session.identity_id
             JOIN agent_profile AS profile
               ON profile.id = session.profile_id
             JOIN agent_context_scope AS scope
               ON scope.id = session.context_scope_id
             LEFT JOIN agent_chat AS chat
               ON scope.scope_type = 'agent_chat'
              AND chat.id = scope.scope_id
             LEFT JOIN project_agent_binding AS binding
               ON binding.project_id = CASE
                    WHEN scope.scope_type = 'agent_chat' THEN chat.project_id
                    ELSE scope.project_id
                  END
             AND binding.identity_id = session.identity_id
             AND binding.state = 'active'
             LEFT JOIN project AS bound_project
               ON bound_project.id = CASE
                    WHEN scope.scope_type = 'agent_chat' THEN chat.project_id
                    ELSE scope.project_id
                  END
             WHERE session.id = ?
               AND session.runtime_session_id IS ?
               AND (? IS NOT NULL OR (session.backend_kind = 'cli' AND profile.backend_kind = 'cli'))
             LIMIT 1",
        )
        .bind(forge_session_id)
        .bind(runtime_session_id)
        .bind(runtime_session_id)
        .fetch_optional(self.db.pool())
        .await }
        .map_err(|_| crate::AgentHostError::ProtectedPersistence)?
        .ok_or(crate::AgentHostError::SessionNotFound)?;

        let identity_id: String = row
            .try_get("identity_id")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let identity_paused: i64 = row
            .try_get("paused")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let identity_archived_at: Option<String> = row
            .try_get("archived_at")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let scope_type: String = row
            .try_get("scope_type")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let scope_id: String = row
            .try_get("scope_id")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let project_id: Option<String> = row
            .try_get("project_id")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let agent_chat_kind: Option<String> = row
            .try_get("agent_chat_kind")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let agent_chat_project_id: Option<String> = row
            .try_get("agent_chat_project_id")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let task_role: Option<String> = row
            .try_get("task_role")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let workspace_access: String = row
            .try_get("workspace_access")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let mut persisted_workspace_path: Option<String> = row
            .try_get("workspace_path")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let binding_permission_ceiling: Option<String> = row
            .try_get("binding_permission_ceiling")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
        let project_charter_setup_required: bool = row
            .try_get::<Option<i64>, _>("project_charter_setup_required")
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?
            .is_some_and(|value| value != 0);
        let scope_type = match scope_type.as_str() {
            "account" => crate::CanonicalScopeType::Account,
            "project" => crate::CanonicalScopeType::Project,
            "agent_chat" => crate::CanonicalScopeType::AgentChat,
            "task" => crate::CanonicalScopeType::Task,
            _ => {
                return Err(crate::AgentHostError::Authority(
                    "persisted canonical scope type is invalid".to_owned(),
                ));
            }
        };
        let workspace_access = match workspace_access.as_str() {
            "deny" => crate::WorkspaceAccess::Deny,
            "task_read" => crate::WorkspaceAccess::TaskRead,
            "task_write" => crate::WorkspaceAccess::TaskWrite,
            "project_verify" => crate::WorkspaceAccess::ProjectVerify,
            "account_scratch" => crate::WorkspaceAccess::AccountScratch,
            _ => {
                return Err(crate::AgentHostError::Authority(
                    "persisted workspace access is invalid".to_owned(),
                ));
            }
        };
        let scope = crate::CanonicalScope {
            scope_type,
            scope_id,
            workspace_access,
        };
        scope.validate()?;
        let agent_chat_project_id =
            if matches!(scope.scope_type, crate::CanonicalScopeType::AgentChat) {
                match agent_chat_kind.as_deref() {
                    Some("account_main") => {
                        if agent_chat_project_id.is_some() || project_id.is_some() {
                            return Err(crate::AgentHostError::Authority(
                                "Main Agent Chat has an invalid Project binding".to_owned(),
                            ));
                        }
                        None
                    }
                    Some("project") => {
                        let Some(chat_project_id) = agent_chat_project_id else {
                            return Err(crate::AgentHostError::Authority(
                                "Project Agent Chat has no owning Project".to_owned(),
                            ));
                        };
                        if project_id.as_deref() != Some(chat_project_id.as_str()) {
                            return Err(crate::AgentHostError::Authority(
                                "Project Agent Chat scope does not match its owning Project"
                                    .to_owned(),
                            ));
                        }
                        Some(chat_project_id)
                    }
                    _ => {
                        return Err(crate::AgentHostError::Authority(
                            "persisted Agent Chat kind is not admitted".to_owned(),
                        ));
                    }
                }
            } else {
                None
            };
        if matches!(scope.scope_type, crate::CanonicalScopeType::Task) {
            let mut authorized_path = None;
            if let Some(path) = workspace_path {
                if db::WorkspaceRepo::task_owns_embedded_path(&*self.db, &scope.scope_id, path)
                    .await
                    .map_err(|_| crate::AgentHostError::ProtectedPersistence)?
                {
                    authorized_path = Some(path.to_owned());
                }
            }
            persisted_workspace_path = Some(authorized_path.ok_or_else(|| {
                crate::AgentHostError::Authority(
                    "Task session has no active persisted workspace".to_owned(),
                )
            })?);
        }
        // A Project Agent Chat owns its verification checkout and a Main Agent
        // Chat (or an inquiry sub-agent under the account scope) owns its
        // scratch directory; every other non-Task scope remains
        // filesystem-denied.
        if !matches!(scope.scope_type, crate::CanonicalScopeType::Task)
            && !matches!(
                scope.workspace_access,
                crate::WorkspaceAccess::ProjectVerify | crate::WorkspaceAccess::AccountScratch
            )
            && persisted_workspace_path.is_some()
        {
            return Err(crate::AgentHostError::Authority(
                "non-Task session is bound to a workspace".to_owned(),
            ));
        }
        if scope.workspace_access == crate::WorkspaceAccess::ProjectVerify
            && persisted_workspace_path.is_none()
        {
            return Err(crate::AgentHostError::Authority(
                "Project verification session has no persisted workspace".to_owned(),
            ));
        }
        // A scratch scope that advertises a directory it does not have would
        // compose file tools over nothing; fail closed the same way.
        if scope.workspace_access == crate::WorkspaceAccess::AccountScratch
            && persisted_workspace_path.is_none()
        {
            return Err(crate::AgentHostError::Authority(
                "account scratch session has no persisted workspace".to_owned(),
            ));
        }
        if identity_paused != 0 || identity_archived_at.is_some() {
            return Err(crate::AgentHostError::Authority(
                "native session identity is no longer active".to_owned(),
            ));
        }
        if project_id.is_some()
            && !matches!(scope.scope_type, crate::CanonicalScopeType::Task)
            && binding_permission_ceiling.is_none()
        {
            return Err(crate::AgentHostError::Authority(
                "native session Project authority is no longer active".to_owned(),
            ));
        }
        let authority = {
            let profile_id: String = row
                .try_get("profile_id")
                .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
            let authority = if scope.scope_type == crate::CanonicalScopeType::Task {
                if let Some(runtime) = runtime_session_id {
                    self.db
                        .resolve_task_effective_authority(
                            &identity_id,
                            Some(&profile_id),
                            &scope.scope_id,
                            workspace_access_name(scope.workspace_access),
                            runtime,
                        )
                        .await
                } else {
                    self.db
                        .resolve_effective_authority(
                            &identity_id,
                            Some(&profile_id),
                            "task",
                            &scope.scope_id,
                            workspace_access_name(scope.workspace_access),
                        )
                        .await
                }
            } else {
                self.db
                    .resolve_effective_authority(
                        &identity_id,
                        Some(&profile_id),
                        scope_type_name(scope.scope_type),
                        &scope.scope_id,
                        workspace_access_name(scope.workspace_access),
                    )
                    .await
            }
            .map_err(|_| crate::AgentHostError::ProtectedPersistence)?;
            if !authority.active {
                return Err(crate::AgentHostError::Authority(
                    "session binding is no longer active".to_owned(),
                ));
            }
            Some(authority)
        };
        let allowed_permissions = authority
            .as_ref()
            .expect("resolved session authority")
            .ceiling
            .clone();
        Ok(crate::RuntimeScopeBinding {
            identity_id,
            scope,
            task_role,
            workspace_path: persisted_workspace_path,
            agent_chat_project_id,
            project_charter_setup_required,
            allowed_permissions,
            authority,
        })
    }
}

fn intersect_permissions(target: &mut BTreeSet<String>, layer: &BTreeSet<String>) {
    *target = target.intersection(layer).cloned().collect();
}

fn scope_permission_set(
    scope_type: crate::CanonicalScopeType,
    workspace_access: crate::WorkspaceAccess,
    project_agent_chat: bool,
    project_charter_setup_required: bool,
) -> BTreeSet<String> {
    operation_registry::authority::scope_permissions(
        scope_type_name(scope_type),
        workspace_access_name(workspace_access),
        project_agent_chat,
        project_charter_setup_required,
    )
}
fn scope_type_name(scope: crate::CanonicalScopeType) -> &'static str {
    match scope {
        crate::CanonicalScopeType::Account => "account",
        crate::CanonicalScopeType::Project => "project",
        crate::CanonicalScopeType::AgentChat => "agent_chat",
        crate::CanonicalScopeType::Task => "task",
    }
}
fn workspace_access_name(access: crate::WorkspaceAccess) -> &'static str {
    match access {
        crate::WorkspaceAccess::Deny => "deny",
        crate::WorkspaceAccess::TaskRead => "task_read",
        crate::WorkspaceAccess::TaskWrite => "task_write",
        crate::WorkspaceAccess::ProjectVerify => "project_verify",
        crate::WorkspaceAccess::AccountScratch => "account_scratch",
    }
}

#[cfg(test)]
mod tests;
