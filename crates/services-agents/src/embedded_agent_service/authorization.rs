//! Embedded identity ownership and canonical scope authorization.

use super::*;

impl EmbeddedAgentService {
    pub(super) async fn require_owned_identity(
        &self,
        identity_id: &str,
        user_id: &str,
    ) -> Result<Agent> {
        AgentRepo::get_by_id(&*self.db, identity_id)
            .await?
            .filter(|agent| agent.owner_id.as_deref() == Some(user_id))
            .ok_or_else(|| ServiceError::not_found("agent", identity_id.to_owned()))
    }

    pub(super) async fn authorize_scope(
        &self,
        actor_user_id: &str,
        identity: &Agent,
        requested: &RequestedCanonicalScope,
    ) -> Result<CanonicalScope> {
        match requested {
            RequestedCanonicalScope::Account => Ok(CanonicalScope {
                scope_type: CanonicalScopeType::Account,
                scope_id: actor_user_id.to_owned(),
                workspace_access: WorkspaceAccess::Deny,
            }),
            RequestedCanonicalScope::Project { project_id } => {
                require_project_authority(&self.db, actor_user_id, &identity.id, project_id)
                    .await?;
                Ok(CanonicalScope {
                    scope_type: CanonicalScopeType::Project,
                    scope_id: project_id.clone(),
                    workspace_access: WorkspaceAccess::Deny,
                })
            }
            RequestedCanonicalScope::AgentChat { chat_id } => {
                let chat = AgentChatRepo::get_agent_chat(&*self.db, chat_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("agent_chat", chat_id.clone()))?;
                match chat.kind.as_str() {
                    "account_main" => {
                        if chat.account_id.as_deref() != Some(actor_user_id) {
                            return Err(ServiceError::not_found("agent_chat", chat_id.clone()));
                        }
                        AccountMainAgentBindingRepo::get_active_main_binding(
                            &*self.db,
                            actor_user_id,
                        )
                        .await?
                        .filter(|binding| {
                            binding.state == "active" && binding.identity_id == identity.id
                        })
                        .ok_or_else(|| {
                            ServiceError::not_found("main_agent_binding", identity.id.clone())
                        })?;
                    }
                    "project" => {
                        let project_id = chat.project_id.as_deref().ok_or_else(|| {
                            ServiceError::not_found("agent_chat", chat_id.clone())
                        })?;
                        ProjectMemberRepo::get_member(&*self.db, project_id, actor_user_id)
                            .await?
                            .ok_or_else(|| {
                                ServiceError::not_found("project", project_id.to_owned())
                            })?;
                        ProjectAgentBindingRepo::get_active_project_binding(&*self.db, project_id)
                            .await?
                            .filter(|binding| {
                                binding.state == "active"
                                    && binding.identity_id.as_deref() == Some(identity.id.as_str())
                            })
                            .ok_or_else(|| {
                                ServiceError::not_found(
                                    "project_agent_binding",
                                    identity.id.clone(),
                                )
                            })?;
                    }
                    _ => {
                        return Err(ServiceError::not_found("agent_chat", chat_id.clone()));
                    }
                }
                // A Project Agent Chat holds a disposable verification
                // checkout when the Project has a repository, so the Agent can
                // exercise the delivered software rather than reason about it.
                // There is no write tool in that composition and the checkout
                // is never a delivery worktree.
                let workspace_access =
                    match (chat.project_id.as_deref(), chat.account_id.as_deref()) {
                        (Some(project_id), _)
                            if self.project_agent_workspace(project_id).await.is_some() =>
                        {
                            WorkspaceAccess::ProjectVerify
                        }
                        // A Main Chat holds an account scratch directory: no
                        // repository, nothing to push, just somewhere to keep
                        // findings instead of carrying them in context.
                        (None, Some(account_id))
                            if self.main_agent_workspace(account_id).await.is_some() =>
                        {
                            WorkspaceAccess::AccountScratch
                        }
                        _ => WorkspaceAccess::Deny,
                    };
                Ok(CanonicalScope {
                    scope_type: CanonicalScopeType::AgentChat,
                    scope_id: chat_id.clone(),
                    workspace_access,
                })
            }
            RequestedCanonicalScope::Task { task_id, role } => {
                if !matches!(role.as_str(), "worker" | "reviewer" | "planner") {
                    return Err(ServiceError::invalid_operation(
                        "embedded Task sessions support only worker, reviewer, or planner roles",
                    ));
                }
                let task = TaskRepo::get_by_id(&*self.db, task_id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?;
                ProjectMemberRepo::get_member(&*self.db, &task.project_id, actor_user_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
                let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
                let workflow = WorkflowEngine::resolve_workflow_for_task(
                    &task,
                    &project.workflow_definition,
                    &api_types::Actor::system(api_types::SystemComponent::Executor),
                );
                let active_role = workflow
                    .states
                    .iter()
                    .find(|state| state.name == task.status)
                    .and_then(effective_role);
                if !task_role_admitted_by_workflow(active_role, role) {
                    return Err(ServiceError::invalid_operation(
                        "embedded Task scope is not admitted by the current workflow state",
                    ));
                }
                // The default Forge workflow calls its implementation role
                // `coder`, while the embedded authority contract calls the
                // same write-capable scope a `worker`. Resolve the durable
                // assignment here without granting anything to an unassigned
                // identity; reviewers remain an exact role match.
                let assignment = TaskRoleAssignmentRepo::list_by_task(&*self.db, task_id)
                    .await?
                    .into_iter()
                    .find(|assignment| {
                        assignment.assignee_type == Some(AssigneeKind::Agent)
                            && assignment.assignee_id.as_deref() == Some(identity.id.as_str())
                            && (assignment.role_name == role.as_str()
                                || (role == "worker"
                                    && matches!(assignment.role_name.as_str(), "worker" | "coder")))
                    })
                    .ok_or_else(|| {
                        ServiceError::not_found("task_role_assignment", task_id.clone())
                    })?;
                // Reviewer and planner sessions never receive worktree write
                // authority; planners author plans through Task metadata and
                // native tools only. Planning/discovery Tasks are read-only
                // for every role — the execution snapshot marks their
                // worktree read-only, so the session binding must match or a
                // coder-role dispatch on a planning record binds TaskWrite
                // while the turn requests TaskRead and authority validation
                // rejects the turn.
                let access = if matches!(role.as_str(), "reviewer" | "planner")
                    || matches!(task.task_type.as_str(), "planning_task" | "discovery")
                {
                    WorkspaceAccess::TaskRead
                } else {
                    WorkspaceAccess::TaskWrite
                };
                // A role-table preassignment is not itself a workspace grant.
                // The workflow must have admitted the task and a live
                // execution must exist for this identity.  This prevents a
                // caller from creating a durable write-capable Task session
                // before the normal claim/dispatch transaction runs.
                let active_execution = ExecutionRepo::list_running_by_task(&*self.db, task_id)
                    .await?
                    .into_iter()
                    .any(|execution| {
                        execution.agent_id.as_deref() == Some(identity.id.as_str())
                            && execution_role_serves_task_role(&execution.role, role.as_str())
                    });
                // The durable claim marker is either the Task-level assignee
                // (interactive claim path) or the role-table assignment the
                // claim/dispatch transaction wrote (dispatcher-initiated
                // initial role dispatch never sets the Task assignee). Both
                // are server-written records; requiring the Task assignee
                // alone made every dispatcher-initiated embedded worker
                // dispatch fail authorization.
                let claim_admitted = role != "worker"
                    || (task.assignee_type.as_deref() == Some("agent")
                        && task.assignee_id.as_deref() == Some(identity.id.as_str()))
                    || assignment.assignee_id.as_deref() == Some(identity.id.as_str());
                if !claim_admitted || !active_execution {
                    // The Running execution row is re-queried here instead of
                    // being threaded from the caller, so a stop/terminalize
                    // racing this authorization makes the row disappear
                    // between the runner's admission check and this lookup.
                    // Name the race so the failure is diagnosable; recovery
                    // stays with the dispatcher's normal retry pass.
                    return Err(ServiceError::invalid_operation(format!(
                        "no Running {role} execution for this identity on task {task_id}; \
                         the execution likely stopped or was superseded between dispatch \
                         admission and session authorization"
                    )));
                }
                Ok(CanonicalScope {
                    scope_type: CanonicalScopeType::Task,
                    scope_id: task_id.clone(),
                    workspace_access: access,
                })
            }
        }
    }

    pub(super) async fn scope_project_id(
        &self,
        requested: &RequestedCanonicalScope,
    ) -> Result<Option<String>> {
        match requested {
            RequestedCanonicalScope::Account => Ok(None),
            RequestedCanonicalScope::Project { project_id } => Ok(Some(project_id.clone())),
            RequestedCanonicalScope::AgentChat { chat_id } => {
                Ok(AgentChatRepo::get_agent_chat(&*self.db, chat_id)
                    .await?
                    .and_then(|chat| chat.project_id))
            }
            RequestedCanonicalScope::Task { task_id, .. } => Ok(Some(
                TaskRepo::get_by_id(&*self.db, task_id, false)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("task", task_id.clone()))?
                    .project_id,
            )),
        }
    }
}

async fn require_project_authority(
    db: &SqliteDb,
    user_id: &str,
    identity_id: &str,
    project_id: &str,
) -> Result<()> {
    ProjectMemberRepo::get_member(db, project_id, user_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
    ProjectAgentBindingRepo::get_active_project_binding(db, project_id)
        .await?
        .filter(|binding| {
            binding.state == "active" && binding.identity_id.as_deref() == Some(identity_id)
        })
        .ok_or_else(|| ServiceError::not_found("project_agent_binding", identity_id.to_owned()))?;
    Ok(())
}
