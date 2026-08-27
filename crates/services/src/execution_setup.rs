//! Shared Project execution-setup and Task capability admission helpers.
//!
//! This module is intentionally independent of the durable setup projection.
//! It is the policy seam used while that projection is materialized: workflow
//! roles and the active baseline release policy are interpreted here, and the
//! same Task-kind/capability classification is used by proposal admission and
//! WorkspaceLease issuance.

use std::collections::HashMap;

use api_types::{RetryAction, SetupRequirement, WorkflowDefinition};
use db::{Agent, Project, ProjectRepo, SqliteDb};
use serde_json::Value;
use sqlx::Row;

use crate::{
    agent_service::{compute_effective_status, EffectiveStatus},
    workflow::{default_roles, effective_role},
    Result, ServiceError,
};

/// Capability profiles that do not grant repository mutation.
pub const READ_ONLY_CAPABILITY_PROFILES: &[&str] = &[
    "repository_read",
    "read_only",
    "discovery_read",
    "planning_read",
];

/// The closed set of capability profiles understood by the scheduler.
pub const SUPPORTED_CAPABILITY_PROFILES: &[&str] = &[
    "repository_read",
    "repository_write",
    "read_only",
    "discovery_read",
    "planning_read",
];

/// Canonical intent derived from Task kind and the server-owned capability.
/// Repository presence is deliberately not an input: it is a resource binding
/// checked after intent admission, never a substitute for intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskExecutionClass {
    Implementation,
    ReadOnlyPlanning,
}

impl TaskExecutionClass {
    #[must_use]
    pub const fn requires_baseline(self) -> bool {
        matches!(self, Self::Implementation)
    }

    #[must_use]
    pub const fn is_read_only(self) -> bool {
        matches!(self, Self::ReadOnlyPlanning)
    }
}

/// Classify a Task from its canonical kind and capability profile.
///
/// An omitted capability is normalized by [`canonical_task_capability`]
/// before this function is called. Explicit read-only capability always wins
/// over a repository binding, while an implementation kind with no explicit
/// capability remains implementation intent even when its repository is not
/// configured yet.
pub fn classify_task_execution(
    task_type: &str,
    capability_class: Option<&str>,
) -> Result<TaskExecutionClass> {
    let task_type = task_type.trim();
    if !matches!(
        task_type,
        "task" | "planning_task" | "sub_task" | "discovery"
    ) {
        return Err(ServiceError::invalid_operation(
            "task_type must be task, planning_task, sub_task, or discovery",
        ));
    }
    let capability = capability_class
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(capability) = capability {
        if !SUPPORTED_CAPABILITY_PROFILES.contains(&capability) {
            return Err(ServiceError::invalid_operation(format!(
                "Task capability_class '{}' is not server-approved; allowed values: {}",
                capability,
                SUPPORTED_CAPABILITY_PROFILES.join(", ")
            )));
        }
    }

    if matches!(task_type, "planning_task" | "discovery")
        || capability.is_some_and(is_read_only_capability)
    {
        return Ok(TaskExecutionClass::ReadOnlyPlanning);
    }
    Ok(TaskExecutionClass::Implementation)
}

/// Return the server-owned default capability for a canonical Task kind.
/// This is also used when no Project repository exists, so implementation
/// intent cannot silently degrade into an ungoverned planning Task.
pub fn canonical_task_capability(task_type: &str, requested: Option<&str>) -> Result<String> {
    let capability = requested
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    classify_task_execution(task_type, capability.as_deref())?;
    Ok(capability.unwrap_or_else(|| {
        if matches!(task_type.trim(), "planning_task" | "discovery") {
            "repository_read".to_owned()
        } else {
            "repository_write".to_owned()
        }
    }))
}

#[must_use]
pub fn is_read_only_capability(capability_class: &str) -> bool {
    READ_ONLY_CAPABILITY_PROFILES.contains(&capability_class.trim())
}

/// Required Project-level execution roles derived from the current workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequiredExecutionRoles {
    pub worker_role: Option<String>,
    pub reviewer_role: Option<String>,
    pub independent_reviewer_required: bool,
}

/// Result of resolving eligible default role identities. Missing roles are
/// represented as typed setup requirements instead of silently reusing an
/// active coordinator or self-reviewing Worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionRoleResolution {
    pub worker_role: Option<String>,
    pub worker_identity_id: Option<String>,
    pub reviewer_role: Option<String>,
    pub reviewer_identity_id: Option<String>,
    pub requirements: Vec<SetupRequirement>,
}

impl ExecutionRoleResolution {
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.requirements.is_empty()
    }

    #[must_use]
    pub fn default_role_assignments(&self) -> Vec<Value> {
        let mut assignments = Vec::new();
        if let (Some(role), Some(identity_id)) = (
            self.worker_role.as_deref(),
            self.worker_identity_id.as_deref(),
        ) {
            assignments.push(serde_json::json!({
                "role_name": role,
                "assignee_type": "agent",
                "assignee_id": identity_id,
            }));
        }
        if let (Some(role), Some(identity_id)) = (
            self.reviewer_role.as_deref(),
            self.reviewer_identity_id.as_deref(),
        ) {
            assignments.push(serde_json::json!({
                "role_name": role,
                "assignee_type": "agent",
                "assignee_id": identity_id,
            }));
        }
        assignments
    }
}

/// Resolve required Worker/reviewer roles from a workflow and, when present,
/// the active baseline's reviewer-independence policy.
pub fn required_execution_roles(
    workflow: &WorkflowDefinition,
    active_baseline_release_policy: Option<&Value>,
) -> RequiredExecutionRoles {
    let mut worker_role = None;
    let mut reviewer_role = None;
    for state in &workflow.states {
        let Some(role) = effective_role(state) else {
            continue;
        };
        match workflow.canonical_phase_for_state(&state.name) {
            api_types::CanonicalPhase::Working
                if role != default_roles::PLANNER && role != default_roles::REVIEWER =>
            {
                worker_role.get_or_insert_with(|| role.to_owned());
            }
            api_types::CanonicalPhase::Review => {
                reviewer_role.get_or_insert_with(|| role.to_owned());
            }
            _ => {}
        }
    }

    // A custom workflow may represent review as a gate without naming its
    // role. An active baseline policy still makes the independent reviewer a
    // required execution role, using the canonical reviewer role name.
    let policy_requires_reviewer = active_baseline_release_policy
        .and_then(|policy| policy.get("reviewer_independence_rules"))
        .and_then(Value::as_array)
        .is_some_and(|rules| !rules.is_empty());
    let independent_reviewer_required = reviewer_role.is_some() || policy_requires_reviewer;
    if independent_reviewer_required {
        reviewer_role.get_or_insert_with(|| default_roles::REVIEWER.to_owned());
    }

    RequiredExecutionRoles {
        worker_role,
        reviewer_role,
        independent_reviewer_required,
    }
}

/// Resolve eligible identities for a Project's default Worker/reviewer roles.
/// The active Main and Project Agent identities are excluded at the query
/// boundary, not merely at the final assignment call.
pub async fn resolve_project_execution_roles(
    db: &SqliteDb,
    project: &Project,
) -> Result<ExecutionRoleResolution> {
    resolve_project_execution_roles_with_mode(db, project, false).await
}

/// Resolve role identities during durable provisioning. This is the one
/// pre-assignment path allowed to choose deterministic candidates; the
/// canonical read projection above remains configured-only so it never turns
/// an uncommitted suggestion into an apparently ready setup.
pub async fn resolve_project_execution_roles_for_provisioning(
    db: &SqliteDb,
    project: &Project,
) -> Result<ExecutionRoleResolution> {
    resolve_project_execution_roles_with_mode(db, project, true).await
}

async fn resolve_project_execution_roles_with_mode(
    db: &SqliteDb,
    project: &Project,
    allow_preflight_fallback: bool,
) -> Result<ExecutionRoleResolution> {
    let workflow =
        crate::workflow::engine::WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let release_policy = active_baseline_release_policy(db, project).await?;
    let required = required_execution_roles(&workflow, release_policy.as_ref());
    let candidates = eligible_project_execution_agents(db, project).await?;
    let settings_assignments = project_default_assignments(project)?;

    let worker_identity_id = resolve_role_identity(
        required.worker_role.as_deref(),
        settings_assignments.get(required.worker_role.as_deref().unwrap_or_default()),
        &candidates,
        allow_preflight_fallback,
    );
    let mut requirements = Vec::new();
    if required.worker_role.is_some() && worker_identity_id.is_none() {
        requirements.push(role_setup_requirement(
            "worker",
            "repository_write",
            RetryAction::SelectWorker,
        ));
    }

    let reviewer_identity_id = required.reviewer_role.as_deref().and_then(|role| {
        let configured = settings_assignments.get(role);
        let candidate = resolve_role_identity(
            Some(role),
            configured,
            &candidates,
            allow_preflight_fallback,
        );
        candidate
            .filter(|identity_id| Some(identity_id.as_str()) != worker_identity_id.as_deref())
            .or_else(|| {
                allow_preflight_fallback
                    .then(|| {
                        candidates
                            .iter()
                            .filter(|agent| auto_selectable_execution_agent(agent))
                            .find(|agent| Some(agent.id.as_str()) != worker_identity_id.as_deref())
                            .map(|agent| agent.id.clone())
                    })
                    .flatten()
            })
    });
    if required.independent_reviewer_required && reviewer_identity_id.is_none() {
        requirements.push(role_setup_requirement(
            "independent_reviewer",
            "repository_read",
            RetryAction::SelectIndependentReviewer,
        ));
    }

    Ok(ExecutionRoleResolution {
        worker_role: required.worker_role,
        worker_identity_id,
        reviewer_role: required.reviewer_role,
        reviewer_identity_id,
        requirements,
    })
}

/// Return whether an identity can receive a scheduler-issued WorkspaceLease
/// for this Project right now. This is deliberately re-evaluated at lease
/// admission so a stale default assignment cannot bypass a binding/status
/// change.
pub async fn is_eligible_execution_identity(
    db: &SqliteDb,
    project_id: &str,
    identity_id: &str,
) -> Result<bool> {
    let Some(project) = ProjectRepo::get_by_id(db, project_id).await? else {
        return Ok(false);
    };
    Ok(eligible_project_execution_agents(db, &project)
        .await?
        .iter()
        .any(|agent| agent.id == identity_id))
}

/// Verify that a repository execution subject is currently eligible for the
/// workflow role that is about to run.
///
/// The Task's explicit role assignment is authoritative. Project role
/// selections are defaults used by provisioning and Task creation, not an
/// execution-time identity allowlist. Eligibility is still re-evaluated here
/// so disabled, paused, unavailable, cross-account, or coordinator identities
/// cannot receive a Workspace lease through a stale Task assignment.
pub async fn ensure_execution_role_principal(
    db: &SqliteDb,
    project_id: &str,
    role: &str,
    identity_id: &str,
) -> Result<()> {
    let role = role.trim();
    if role.is_empty() || identity_id.trim().is_empty() {
        return Err(ServiceError::invalid_operation(
            "repository execution requires a non-empty workflow role and identity",
        ));
    }

    if !is_eligible_execution_identity(db, project_id, identity_id).await? {
        let project = ProjectRepo::get_by_id(db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        let workflow =
            crate::workflow::engine::WorkflowEngine::resolve_workflow(&project.workflow_definition);
        let release_policy = active_baseline_release_policy(db, &project).await?;
        let required = required_execution_roles(&workflow, release_policy.as_ref());
        let (requirement_role, capability, action) =
            if required.reviewer_role.as_deref() == Some(role) {
                (
                    "reviewer",
                    "repository_read",
                    RetryAction::SelectIndependentReviewer,
                )
            } else {
                ("worker", "repository_write", RetryAction::SelectWorker)
            };
        let mut requirement = role_setup_requirement(requirement_role, capability, action);
        requirement.role = Some(role.to_owned());
        return Err(ServiceError::execution_setup_required(
            format!(
                "workflow role '{}' is assigned to an agent that is not enabled and available for Project Task execution",
                role
            ),
            vec![requirement],
        ));
    }
    Ok(())
}

async fn active_baseline_release_policy(db: &SqliteDb, project: &Project) -> Result<Option<Value>> {
    let Some(row) = sqlx::query(
        "SELECT r.release_policy_json
         FROM project_execution_baseline b
         JOIN project_execution_baseline_revision r
           ON r.id = b.current_revision_id AND r.baseline_id = b.id
         WHERE b.project_id = ? AND b.lifecycle = 'active'
           AND r.lifecycle = 'approved'
         ORDER BY b.updated_at DESC, b.id DESC
         LIMIT 1",
    )
    .bind(&project.id)
    .fetch_optional(db.pool())
    .await?
    else {
        return Ok(None);
    };
    let raw: String = row.try_get("release_policy_json")?;
    Ok(serde_json::from_str(&raw).ok())
}

/// Return active, Project-eligible execution identities. Coordinator
/// bindings are excluded at the query boundary so setup actions and the
/// scheduler share the same candidate set.
pub async fn eligible_project_execution_agents(
    db: &SqliteDb,
    project: &Project,
) -> Result<Vec<Agent>> {
    let owner_id = project.owner_id.clone().unwrap_or_default();
    let mut candidates = db
        .list_agents_usable_in_project(&project.id, &owner_id)
        .await?;
    let mut eligible = Vec::new();
    for agent in candidates.drain(..) {
        // `Busy` means healthy but currently at its concurrency limit. That is
        // a scheduling fact, not an identity fact: capacity is enforced when a
        // Task is claimed. Excluding it here made an agent ineligible for the
        // very execution it had already started, because that execution is the
        // one consuming its capacity — so a `max_concurrent_tasks = 1` Worker
        // could never dispatch. Role eligibility asks whether this identity may
        // hold the role at all, so only unhealthy or unavailable states
        // disqualify it.
        if !matches!(
            compute_effective_status(db, &agent).await?,
            EffectiveStatus::Active | EffectiveStatus::Busy
        ) {
            continue;
        }
        let bound: i64 = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM account_main_agent_binding
                 WHERE identity_id = ? AND state = 'active'
             ) OR EXISTS (
                 SELECT 1 FROM project_agent_binding
                 WHERE identity_id = ? AND state = 'active'
             )",
        )
        .bind(&agent.id)
        .bind(&agent.id)
        .fetch_one(db.pool())
        .await?;
        if bound == 0 {
            eligible.push(agent);
        }
    }
    eligible.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(eligible)
}

fn project_default_assignments(project: &Project) -> Result<HashMap<String, String>> {
    let settings = serde_json::from_str::<Value>(&project.settings).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid Project settings: {error}"))
    })?;
    let mut assignments = HashMap::new();
    let Some(values) = settings
        .get("default_role_assignments")
        .and_then(Value::as_array)
    else {
        return Ok(assignments);
    };
    for value in values {
        if value.get("assignee_type").and_then(Value::as_str) != Some("agent") {
            continue;
        }
        let (Some(role), Some(identity_id)) = (
            value.get("role_name").and_then(Value::as_str),
            value.get("assignee_id").and_then(Value::as_str),
        ) else {
            continue;
        };
        if !role.trim().is_empty() && !identity_id.trim().is_empty() {
            assignments.insert(role.to_owned(), identity_id.to_owned());
        }
    }
    Ok(assignments)
}

fn resolve_role_identity(
    role: Option<&str>,
    configured: Option<&String>,
    candidates: &[Agent],
    allow_preflight_fallback: bool,
) -> Option<String> {
    let _role = role?;
    if let Some(configured) = configured {
        if candidates.iter().any(|agent| agent.id == *configured) {
            return Some(configured.clone());
        }
    }
    allow_preflight_fallback.then(|| {
        candidates
            .iter()
            .find(|agent| auto_selectable_execution_agent(agent))
            .map(|agent| agent.id.clone())
    })?
}

/// Bootstrap defaults are discovery conveniences, not proof that a Task can
/// authenticate. They remain available for an explicit user assignment (the
/// daemon may own local CLI auth), but provisioning never silently chooses a
/// credential-less default for Worker or reviewer authority.
fn auto_selectable_execution_agent(agent: &Agent) -> bool {
    !agent.is_default || agent.credential_ref.is_some()
}

fn role_setup_requirement(role: &str, capability: &str, action: RetryAction) -> SetupRequirement {
    let mut requirement = SetupRequirement::new("role_assignment");
    requirement.role = Some(role.to_owned());
    requirement.capability = Some(capability.to_owned());
    requirement.action = Some(action);
    requirement
}

#[cfg(test)]
mod tests {
    use super::*;
    use api_types::{CanonicalPhase, StateDefinition, StateHooks, StateKind, WorkflowDefinition};
    use serde_json::json;

    fn state(
        name: &str,
        kind: StateKind,
        phase: CanonicalPhase,
        role: Option<&str>,
    ) -> StateDefinition {
        StateDefinition {
            name: name.to_owned(),
            kind,
            column: name.to_owned(),
            display_name: name.to_owned(),
            role: role.map(str::to_owned),
            hooks: StateHooks::default(),
            cleanup: None,
            canonical_phase: Some(phase),
            gate_config: None,
            dispatch: None,
            triggers: Default::default(),
            config: json!({}),
        }
    }

    #[test]
    fn task_kind_not_repository_presence_controls_implementation_class() {
        assert_eq!(
            classify_task_execution("task", None).expect("implementation default"),
            TaskExecutionClass::Implementation
        );
        assert_eq!(
            classify_task_execution("planning_task", Some("repository_read"))
                .expect("planning read"),
            TaskExecutionClass::ReadOnlyPlanning
        );
        assert_eq!(
            classify_task_execution("task", Some("repository_read")).expect("explicit read"),
            TaskExecutionClass::ReadOnlyPlanning
        );
    }

    #[test]
    fn required_roles_follow_workflow_and_baseline_independence() {
        let workflow = WorkflowDefinition {
            roles: Vec::new(),
            states: vec![
                state(
                    "working",
                    StateKind::Active,
                    CanonicalPhase::Working,
                    Some("worker"),
                ),
                state("review", StateKind::Gate, CanonicalPhase::Review, None),
            ],
            ..WorkflowDefinition::default()
        };
        let roles = required_execution_roles(
            &workflow,
            Some(&json!({"reviewer_independence_rules":["always"]})),
        );
        assert_eq!(roles.worker_role.as_deref(), Some("worker"));
        assert_eq!(roles.reviewer_role.as_deref(), Some("reviewer"));
        assert!(roles.independent_reviewer_required);
    }

    #[test]
    fn reviewer_requirement_is_distinct_from_worker_requirement() {
        let mut requirement = role_setup_requirement(
            "independent_reviewer",
            "repository_read",
            RetryAction::SelectIndependentReviewer,
        );
        requirement.resource_type = Some("agent_identity".to_owned());
        assert_eq!(requirement.role.as_deref(), Some("independent_reviewer"));
        assert_eq!(requirement.capability.as_deref(), Some("repository_read"));
    }

    #[test]
    fn provisioning_does_not_auto_pick_a_credentialless_default() {
        let agent = Agent {
            id: "default-cli".to_owned(),
            name: "CLI Default".to_owned(),
            description: None,
            profile_id: "profile-1".to_owned(),
            backend_kind: "cli".to_owned(),
            executor_type: "claude_code".to_owned(),
            provider: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: true,
            paused: false,
            owner_id: Some("user-1".to_owned()),
            visibility: "account".to_owned(),
            version: 1,
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
        };
        assert!(!auto_selectable_execution_agent(&agent));

        let explicitly_configured = resolve_role_identity(
            Some("worker"),
            Some(&agent.id),
            std::slice::from_ref(&agent),
            true,
        );
        assert_eq!(explicitly_configured.as_deref(), Some("default-cli"));
        assert_eq!(
            resolve_role_identity(Some("worker"), None, &[agent], true),
            None
        );
    }
}
