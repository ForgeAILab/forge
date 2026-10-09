//! Shared permission normalization and transport-neutral authority evaluation.
use serde_json::Value;
use std::collections::BTreeSet;

pub type PermissionSet = BTreeSet<String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionDocumentError {
    Invalid,
    Conflicting,
}
impl PermissionDocumentError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Invalid => "invalid_permission_document",
            Self::Conflicting => "conflicting_permission_document",
        }
    }
}
impl std::fmt::Display for PermissionDocumentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for PermissionDocumentError {}

/// Stored arrays, either object key, both agreeing keys, and the empty
/// default object or null. Compare normalized sets, so ordering and duplicates do
/// not turn equivalent documents into conflicts. Never select precedence.
pub fn parse_permissions(document: &str) -> Result<PermissionSet, PermissionDocumentError> {
    fn entries(value: &Value) -> Result<PermissionSet, PermissionDocumentError> {
        value
            .as_array()
            .ok_or(PermissionDocumentError::Invalid)?
            .iter()
            .map(|v| {
                v.as_str()
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .ok_or(PermissionDocumentError::Invalid)
            })
            .collect()
    }
    let value: Value =
        serde_json::from_str(document).map_err(|_| PermissionDocumentError::Invalid)?;
    match &value {
        Value::Null => Ok(PermissionSet::new()),
        Value::Array(_) => entries(&value),
        Value::Object(map) => match (map.get("permissions"), map.get("allowed")) {
            (Some(a), Some(b)) => {
                let a = entries(a)?;
                let b = entries(b)?;
                if a == b {
                    Ok(a)
                } else {
                    Err(PermissionDocumentError::Conflicting)
                }
            }
            (Some(a), None) | (None, Some(a)) => entries(a),
            (None, None) => Ok(PermissionSet::new()),
        },
        _ => Err(PermissionDocumentError::Invalid),
    }
}
/// Read-time failure is an empty ceiling, including conflicting stored data.
pub fn permission_set(document: &str) -> PermissionSet {
    parse_permissions(document).unwrap_or_default()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Principal {
    InteractiveUser {
        user_id: String,
    },
    DelegatedUser {
        user_id: String,
    },
    MainAgent {
        identity_id: String,
    },
    ProjectAgent {
        identity_id: String,
        project_id: String,
    },
    TaskAgent {
        identity_id: String,
        execution_id: String,
    },
    SystemComponent {
        name: String,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrincipalRule {
    BoundIdentity,
    MainOrInquiry,
    MainChat,
    ProjectAgent,
    DelegatedUser,
    DelegatedAccountUser,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveAuthority {
    pub principal: Principal,
    pub scope_type: String,
    pub scope_id: String,
    pub admitted_profile_id: String,
    pub ceiling: PermissionSet,
    pub binding_id: Option<String>,
    pub setup_required: bool,
    pub active: bool,
}
/// All facts are server-loaded. Empty or malformed layers never disappear.
pub struct AuthorityFacts {
    pub principal: Principal,
    pub scope_type: String,
    pub scope_id: String,
    pub profile_id: String,
    pub layers: Vec<PermissionSet>,
    pub binding_id: Option<String>,
    pub setup_required: bool,
    pub active: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityDenial {
    Revoked,
    PermissionMissing(String),
    PrincipalMismatch,
    StateChanged,
}
impl EffectiveAuthority {
    /// The only resolver. Call at admission, then intersect fresh facts with
    /// this ceiling before an effect. Changing profiles cannot widen a turn.
    pub fn resolve(facts: AuthorityFacts) -> Self {
        let ceiling = facts
            .layers
            .first()
            .map_or_else(PermissionSet::new, |first| {
                facts
                    .layers
                    .iter()
                    .skip(1)
                    .fold(first.clone(), |set, layer| {
                        set.intersection(layer).cloned().collect()
                    })
            });
        Self {
            principal: facts.principal,
            scope_type: facts.scope_type,
            scope_id: facts.scope_id,
            admitted_profile_id: facts.profile_id,
            ceiling,
            binding_id: facts.binding_id,
            setup_required: facts.setup_required,
            active: facts.active,
        }
    }
    pub fn narrowed(&self, current: &Self) -> Result<Self, AuthorityDenial> {
        if !current.active
            || self.principal != current.principal
            || self.scope_type != current.scope_type
            || self.scope_id != current.scope_id
            || self.binding_id != current.binding_id
        {
            return Err(AuthorityDenial::Revoked);
        }
        let mut authority = self.clone();
        authority.ceiling = self
            .ceiling
            .intersection(&current.ceiling)
            .cloned()
            .collect();
        authority.setup_required = current.setup_required;
        authority.active = current.active;
        Ok(authority)
    }
    /// Advertisement and effect enforcement share this evaluator. Domain
    /// invariants and exact-object CAS remain in the command transactions.
    pub fn evaluate(&self, requirement: impl AuthorityRequirement) -> Result<(), AuthorityDenial> {
        if !self.active {
            return Err(AuthorityDenial::Revoked);
        }
        let principal_ok = match requirement.principal_rule() {
            PrincipalRule::BoundIdentity => match &self.principal {
                Principal::MainAgent { .. } => {
                    matches!(self.scope_type.as_str(), "account" | "agent_chat")
                }
                Principal::ProjectAgent { .. } => {
                    matches!(self.scope_type.as_str(), "project" | "agent_chat")
                }
                Principal::TaskAgent { .. } => self.scope_type == "task",
                _ => false,
            },
            PrincipalRule::MainOrInquiry => {
                self.binding_id.is_some()
                    && matches!(self.principal, Principal::MainAgent { .. })
                    && matches!(self.scope_type.as_str(), "account" | "agent_chat")
            }
            PrincipalRule::MainChat => {
                matches!(self.principal, Principal::MainAgent { .. })
                    && self.scope_type == "agent_chat"
            }
            PrincipalRule::ProjectAgent => {
                matches!(self.principal, Principal::ProjectAgent { .. })
                    && matches!(self.scope_type.as_str(), "project" | "agent_chat")
            }
            PrincipalRule::DelegatedUser => {
                matches!(&self.principal, Principal::DelegatedUser {user_id} if !user_id.is_empty())
            }
            PrincipalRule::DelegatedAccountUser => {
                self.scope_type == "account"
                    && matches!(&self.principal, Principal::DelegatedUser {user_id} if !user_id.is_empty())
            }
        };
        if !principal_ok {
            return Err(AuthorityDenial::PrincipalMismatch);
        }
        match requirement.permission(&self.scope_type) {
            RequiredPermission::Unavailable => return Err(AuthorityDenial::PrincipalMismatch),
            RequiredPermission::Named(permission) if !self.ceiling.contains(permission) => {
                return Err(AuthorityDenial::PermissionMissing(permission.to_owned()));
            }
            _ => {}
        }
        let available = match requirement.availability() {
            crate::AvailabilityRule::Always => true,
            crate::AvailabilityRule::ReadyOnly => !self.setup_required,
            crate::AvailabilityRule::SetupOnly => self.setup_required,
            crate::AvailabilityRule::MainChatOnly => {
                self.scope_type == "agent_chat"
                    && matches!(self.principal, Principal::MainAgent { .. })
            }
        };
        if available {
            Ok(())
        } else {
            Err(AuthorityDenial::StateChanged)
        }
    }
}

/// Canonical scope ceiling, shared by protected admission and UI/CLI
/// projection. Task callers keep their execution authorization path in F.
pub fn scope_permissions(
    scope: &str,
    workspace: &str,
    project_chat: bool,
    setup: bool,
) -> PermissionSet {
    let mut names = match scope {
        "account" if workspace == "account_scratch" => vec!["read_account", "propose_discovery"],
        "account" => vec![
            "read_account",
            "propose_discovery",
            "propose_project",
            "propose_handoff",
        ],
        "project" => vec![
            "read_project",
            "read_memory",
            "propose_project",
            "propose_message",
        ],
        "agent_chat" => vec![
            "read_agent_chat",
            "read_memory",
            "propose_message",
            "propose_project",
        ],
        "task" if workspace == "task_read" => vec!["read_task", "read_memory", "task_read"],
        "task" if workspace == "task_write" => {
            vec!["read_task", "read_memory", "task_read", "task_write"]
        }
        _ => vec![],
    };
    if scope == "agent_chat" && !project_chat {
        names.extend(["propose_discovery", "propose_handoff"]);
    }
    // A Project Agent Chat is narrower than the Project scope: it never
    // carried review or decision proposals, and unifying the two tables must
    // not add them.
    if scope == "project" && !setup {
        names.extend([
            "propose_task",
            "propose_commitment",
            "propose_memory",
            "propose_review",
            "propose_decision",
            "propose_session",
        ]);
    }
    if scope == "agent_chat" && project_chat && !setup {
        names.extend([
            "propose_task",
            "propose_commitment",
            "propose_memory",
            "propose_session",
        ]);
    }
    names.into_iter().map(str::to_owned).collect()
}

/// MCP credentials are delegated users, even when constrained to a Project.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpScopeRule {
    AccountInspection,
    BoundProject,
}
pub const MCP_OPERATIONS: &[(&str, McpScopeRule)] = &[
    ("forge_create_task", McpScopeRule::BoundProject),
    ("forge_create_sub_tasks", McpScopeRule::BoundProject),
    ("forge_add_task_dependency", McpScopeRule::BoundProject),
    ("forge_remove_task_dependency", McpScopeRule::BoundProject),
    ("forge_list_task_dependencies", McpScopeRule::BoundProject),
    ("forge_list_task_dependents", McpScopeRule::BoundProject),
    ("forge_list_sub_tasks", McpScopeRule::BoundProject),
    ("forge_reorder_sub_tasks", McpScopeRule::BoundProject),
    ("forge_list_tasks", McpScopeRule::BoundProject),
    ("forge_get_task", McpScopeRule::BoundProject),
    ("forge_preview_prompt", McpScopeRule::BoundProject),
    ("forge_memory_search", McpScopeRule::BoundProject),
    ("forge_memory_get", McpScopeRule::BoundProject),
    ("forge_assign_agent", McpScopeRule::BoundProject),
    ("forge_task_action", McpScopeRule::BoundProject),
    ("forge_get_task_diff", McpScopeRule::BoundProject),
    ("forge_list_executions", McpScopeRule::BoundProject),
    ("forge_update_task", McpScopeRule::BoundProject),
    ("forge_transition_task", McpScopeRule::BoundProject),
    ("forge_register_agent", McpScopeRule::AccountInspection),
    ("forge_list_agents", McpScopeRule::AccountInspection),
    ("forge_list_projects", McpScopeRule::BoundProject),
    ("forge_get_project", McpScopeRule::BoundProject),
    ("forge_create_project", McpScopeRule::AccountInspection),
    ("forge_update_project", McpScopeRule::BoundProject),
    (
        "forge_update_project_lifecycle_hooks",
        McpScopeRule::BoundProject,
    ),
    ("forge_follow_up_execution", McpScopeRule::BoundProject),
    ("forge_list_agent_profiles", McpScopeRule::AccountInspection),
    ("forge_list_agent_sessions", McpScopeRule::AccountInspection),
    ("forge_get_agent_session", McpScopeRule::AccountInspection),
    ("forge_get_main_agent", McpScopeRule::AccountInspection),
    ("forge_set_main_agent", McpScopeRule::AccountInspection),
    ("forge_project_escalate", McpScopeRule::BoundProject),
    ("forge_get_project_agent", McpScopeRule::BoundProject),
    ("forge_set_project_agent", McpScopeRule::BoundProject),
    ("forge_list_agent_chats", McpScopeRule::BoundProject),
    ("forge_get_agent_chat", McpScopeRule::BoundProject),
    ("forge_list_agent_chat_messages", McpScopeRule::BoundProject),
    ("forge_send_agent_chat_message", McpScopeRule::BoundProject),
    ("forge_list_agent_handoffs", McpScopeRule::BoundProject),
    ("forge_get_agent_handoff", McpScopeRule::BoundProject),
    ("forge_create_agent_handoff", McpScopeRule::BoundProject),
];
pub fn mcp_scope_rule(name: &str) -> Option<McpScopeRule> {
    MCP_OPERATIONS
        .iter()
        .find(|(id, _)| *id == name)
        .map(|(_, rule)| *rule)
}
pub fn mcp_authority(user_id: &str, constrained_project: Option<&str>) -> EffectiveAuthority {
    EffectiveAuthority::resolve(AuthorityFacts {
        principal: Principal::DelegatedUser {
            user_id: user_id.to_owned(),
        },
        scope_type: if constrained_project.is_some() {
            "project"
        } else {
            "account"
        }
        .into(),
        scope_id: constrained_project.unwrap_or(user_id).to_owned(),
        profile_id: String::new(),
        layers: vec![],
        binding_id: None,
        setup_required: false,
        active: true,
    })
}
/// Native registry specs and delegated MCP scope classifications are inputs
/// to one evaluator, rather than independent permission predicates.
pub enum RequiredPermission {
    Named(&'static str),
    Unavailable,
    None,
}
pub trait AuthorityRequirement {
    fn principal_rule(&self) -> PrincipalRule;
    fn permission(&self, scope: &str) -> RequiredPermission;
    fn availability(&self) -> crate::AvailabilityRule;
}
impl<E> AuthorityRequirement for &crate::OperationSpec<E> {
    fn principal_rule(&self) -> PrincipalRule {
        self.authority.principal
    }
    fn permission(&self, scope: &str) -> RequiredPermission {
        self.authority
            .permission(scope)
            .map_or(RequiredPermission::Unavailable, RequiredPermission::Named)
    }
    fn availability(&self) -> crate::AvailabilityRule {
        self.availability
    }
}
impl AuthorityRequirement for McpScopeRule {
    fn principal_rule(&self) -> PrincipalRule {
        match self {
            Self::AccountInspection => PrincipalRule::DelegatedAccountUser,
            Self::BoundProject => PrincipalRule::DelegatedUser,
        }
    }
    fn permission(&self, _: &str) -> RequiredPermission {
        RequiredPermission::None
    }
    fn availability(&self) -> crate::AvailabilityRule {
        crate::AvailabilityRule::Always
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn authority(permissions: &[&str]) -> EffectiveAuthority {
        EffectiveAuthority::resolve(AuthorityFacts {
            principal: Principal::MainAgent {
                identity_id: "main".into(),
            },
            scope_type: "agent_chat".into(),
            scope_id: "chat".into(),
            profile_id: "admitted".into(),
            layers: vec![permissions.iter().map(|p| (*p).to_owned()).collect()],
            binding_id: Some("binding".into()),
            setup_required: false,
            active: true,
        })
    }
    #[test]
    fn profile_widening_cannot_widen_an_admitted_turn_and_revocation_narrows() {
        let admitted = authority(&["read_agent_chat"]);
        let current = authority(&["read_agent_chat", "propose_project"]);
        let bounded = admitted.narrowed(&current).unwrap();
        assert_eq!(bounded.ceiling, admitted.ceiling);
        let create = crate::main_proposals::CATALOG
            .lookup("project.create")
            .unwrap();
        assert!(bounded.evaluate(create).is_err());
        assert!(
            current.evaluate(create).is_ok(),
            "next admission sees changed authority"
        );
        let narrowed = admitted.narrowed(&authority(&[])).unwrap();
        assert_eq!(
            narrowed.evaluate(crate::READ_CATALOG.lookup("agent_chat.summary").unwrap()),
            Err(AuthorityDenial::PermissionMissing("read_agent_chat".into()))
        );
        let mut revoked = current.clone();
        revoked.binding_id = Some("replacement".into());
        assert_eq!(admitted.narrowed(&revoked), Err(AuthorityDenial::Revoked));
        revoked.binding_id = current.binding_id;
        revoked.active = false;
        assert_eq!(admitted.narrowed(&revoked), Err(AuthorityDenial::Revoked));
    }
    #[test]
    fn reviewers_never_receive_workflow_authority() {
        assert!(!scope_permissions("task", "task_read", false, false).contains("propose_review"));
        assert!(!scope_permissions("task", "task_write", false, false).contains("propose_review"));
        let mut reviewer = authority(&["propose_review", "propose_project", "propose_discovery"]);
        reviewer.principal = Principal::TaskAgent {
            identity_id: "reviewer".into(),
            execution_id: "review-exec".into(),
        };
        reviewer.scope_type = "task".into();
        for spec in crate::main_proposals::CATALOG.iter() {
            assert!(reviewer.evaluate(spec).is_err());
        }
    }
    /// The scope ceiling is pinned literally, row by row. Only the reviewer
    /// row (`task_read` without `propose_review`) differs from the tables
    /// this function replaced; any other change is a widening or a loss.
    #[test]
    fn scope_ceilings_match_the_replaced_tables() {
        let set = |scope, workspace, project_chat, setup| {
            scope_permissions(scope, workspace, project_chat, setup)
                .into_iter()
                .collect::<Vec<_>>()
                .join(",")
        };
        for (scope, workspace, project_chat, setup, expected) in [
            ("account", "deny", false, false, "propose_discovery,propose_handoff,propose_project,read_account"),
            ("account", "account_scratch", false, false, "propose_discovery,read_account"),
            ("project", "deny", false, true, "propose_message,propose_project,read_memory,read_project"),
            ("project", "project_verify", false, false, "propose_commitment,propose_decision,propose_memory,propose_message,propose_project,propose_review,propose_session,propose_task,read_memory,read_project"),
            ("agent_chat", "deny", false, false, "propose_discovery,propose_handoff,propose_message,propose_project,read_agent_chat,read_memory"),
            ("agent_chat", "deny", false, true, "propose_discovery,propose_handoff,propose_message,propose_project,read_agent_chat,read_memory"),
            ("agent_chat", "project_verify", true, true, "propose_message,propose_project,read_agent_chat,read_memory"),
            ("agent_chat", "project_verify", true, false, "propose_commitment,propose_memory,propose_message,propose_project,propose_session,propose_task,read_agent_chat,read_memory"),
            ("task", "task_read", false, false, "read_memory,read_task,task_read"),
            ("task", "task_write", false, false, "read_memory,read_task,task_read,task_write"),
            ("task", "deny", false, false, ""),
            ("agent", "deny", false, false, ""),
        ] {
            assert_eq!(
                set(scope, workspace, project_chat, setup),
                expected,
                "{scope} {workspace} project_chat={project_chat} setup={setup}"
            );
        }
    }
    #[test]
    fn stored_shapes_round_trip_and_conflicts_fail_closed() {
        for document in [
            r#"["read_project","propose_task"]"#,
            r#"{"allowed":["read_project","propose_task"]}"#,
            r#"{"permissions":["read_project","propose_task"]}"#,
            r#"{"permissions":["propose_task","read_project"],"allowed":["read_project","propose_task","read_project"]}"#,
            "{}",
            "[]",
            "null",
        ] {
            let set = parse_permissions(document).unwrap();
            let canonical = serde_json::json!({"permissions":set}).to_string();
            assert_eq!(parse_permissions(&canonical).unwrap(), set);
        }
        let conflicting = r#"{"permissions":["read_project"],"allowed":["propose_task"]}"#;
        assert_eq!(
            parse_permissions(conflicting),
            Err(PermissionDocumentError::Conflicting)
        );
        assert!(permission_set(conflicting).is_empty());
        for invalid in [
            "not-json",
            r#"{"permissions":true}"#,
            r#"["read_project",false]"#,
        ] {
            assert_eq!(
                parse_permissions(invalid),
                Err(PermissionDocumentError::Invalid)
            );
            assert!(permission_set(invalid).is_empty());
        }
    }
}
