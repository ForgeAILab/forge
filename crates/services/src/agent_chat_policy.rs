//! Server-side authority policy for persistent Main and Project Agent Chats.
//!
//! The policy is intentionally independent of the chat repositories. It is
//! used at both admission and native-tool composition time, so a caller's
//! prompt, a referenced Project/Task id, or a replayed action cannot widen
//! the authority that was issued for the chat's binding.

use std::fmt;

use serde_json::json;

use crate::{Result, ServiceError};

/// The two persistent Agent Chat scopes. Task execution is deliberately not
/// represented here: Task workers and reviewers receive their authority from
/// the existing Task assignment/workflow path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentChatScope {
    Main {
        account_id: String,
    },
    Project {
        account_id: String,
        project_id: String,
    },
}

impl AgentChatScope {
    pub fn main(account_id: impl Into<String>) -> Self {
        Self::Main {
            account_id: account_id.into(),
        }
    }

    pub fn project(account_id: impl Into<String>, project_id: impl Into<String>) -> Self {
        Self::Project {
            account_id: account_id.into(),
            project_id: project_id.into(),
        }
    }

    pub fn account_id(&self) -> &str {
        match self {
            Self::Main { account_id } | Self::Project { account_id, .. } => account_id,
        }
    }

    pub fn project_id(&self) -> Option<&str> {
        match self {
            Self::Main { .. } => None,
            Self::Project { project_id, .. } => Some(project_id),
        }
    }
}

/// Operations that can be issued to an embedded Agent Chat session.
///
/// Repository operations are separate from Task management so the deny-all
/// filesystem guarantee remains explicit for both persistent chat kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentChatOperation {
    Discovery,
    WebSearch,
    ProjectLifecycle,
    PortfolioRead,
    HandoffPublish,
    ProjectRead,
    TaskManagement,
    RepositoryRead,
    RepositoryWrite,
}

/// A bounded, redaction-safe policy denial. The error intentionally does not
/// include caller-supplied IDs or prompt content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentChatPolicyError {
    MainTaskDenied,
    ProjectTaskTargetRequired,
    ProjectTaskOutsideBinding,
    RepositoryDenied,
    OperationUnavailable,
}

impl fmt::Display for AgentChatPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::MainTaskDenied => "Main Agent Chat cannot manage Tasks",
            Self::ProjectTaskTargetRequired => {
                "Project Agent Task management requires the bound Project"
            }
            Self::ProjectTaskOutsideBinding => "Project Agent Task target is outside the binding",
            Self::RepositoryDenied => "Agent Chat sessions do not have repository access",
            Self::OperationUnavailable => "operation is unavailable for this Agent Chat",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for AgentChatPolicyError {}

/// Redaction-safe canonical content admitted to an Agent Chat ledger or
/// handoff. Protected values are rejected before persistence or publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardedAgentChatContent {
    pub content: String,
    pub guard_json: String,
    pub sensitivity: String,
}

pub fn guard_agent_chat_content(content: &str) -> Result<GuardedAgentChatContent> {
    guard_content(content, true)
}

/// Validate text that is persisted as an ordinary runtime value (for example
/// a profile prompt or a tool-policy string).  Runtime configuration is
/// allowed to be empty, while Agent Chat messages are not; both paths share
/// the same protected-value classifier so credentials cannot be smuggled
/// through a non-chat field.
pub(crate) fn guard_runtime_content(content: &str) -> Result<GuardedAgentChatContent> {
    guard_content(content, false)
}

/// Shortest token after `sk-` that can plausibly be a key rather than a typo.
const MIN_SECRET_TOKEN_LEN: usize = 4;

/// `api key` marks a protected value only when an assignment carries a value
/// after it. Collapsing the whole message to alphanumerics instead makes
/// ordinary prose — `REST API. Key endpoints`, `api key rotation` — look like a
/// credential, which blocks the Agent Chat turn that contains it.
fn has_api_key_assignment(lower: &str) -> bool {
    lower.match_indices("api").any(|(index, _)| {
        let after_marker = &lower[index + "api".len()..];
        let separators = after_marker
            .chars()
            .take_while(|character| matches!(character, ' ' | '_' | '-'))
            .count();
        let Some(after_key) = after_marker[separators..].strip_prefix("key") else {
            return false;
        };
        let assignment = after_key.trim_start();
        assignment
            .strip_prefix('=')
            .or_else(|| assignment.strip_prefix(':'))
            .is_some_and(|value| !value.trim().is_empty())
    })
}

/// `sk-` marks an OpenAI-shaped key only when it opens a word. A bare substring
/// match also fires inside `task-1`, `risk-free` and `disk-based` — words a
/// Project Agent writes constantly — because there the `sk-` follows a letter.
fn has_openai_key_marker(lower: &str) -> bool {
    lower.match_indices("sk-").any(|(index, _)| {
        let opens_word = index == 0 || !lower.as_bytes()[index - 1].is_ascii_alphanumeric();
        let token_len = lower[index + "sk-".len()..]
            .chars()
            .take_while(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
            .count();
        opens_word && token_len >= MIN_SECRET_TOKEN_LEN
    })
}

fn guard_content(content: &str, reject_empty: bool) -> Result<GuardedAgentChatContent> {
    let trimmed = content.trim();
    if reject_empty && trimmed.is_empty() {
        return Err(ServiceError::invalid_operation(
            "Agent Chat content cannot be empty",
        ));
    }
    let lower = trimmed.to_ascii_lowercase();
    let compact: String = lower
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect();
    let has_bearer_marker = lower
        .split(|character: char| !character.is_ascii_alphabetic())
        .any(|word| word == "bearer");
    let has_github_token_marker = ["ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_"]
        .iter()
        .any(|marker| lower.contains(marker));
    let has_pem_marker = lower.contains("-----begin")
        && (lower.contains("private key") || lower.contains("openssh"));
    if has_bearer_marker
        || compact.contains("bearer")
        || has_api_key_assignment(&lower)
        || has_openai_key_marker(&lower)
        || has_github_token_marker
        || has_pem_marker
    {
        return Err(ServiceError::invalid_operation(
            "protected values cannot be stored in Agent Chat content",
        ));
    }
    Ok(GuardedAgentChatContent {
        content: trimmed.to_owned(),
        guard_json: json!({
            "classifier": "forge-content-guard-v1",
            "action": "admitted",
            "trust": "user_or_runtime_output",
        })
        .to_string(),
        sensitivity: "internal".to_owned(),
    })
}

impl AgentChatScope {
    /// Authorize one server-issued operation for this chat.
    ///
    /// `target_project_id` is an untrusted reference. It is compared to the
    /// binding, never used to resolve a broader scope. Main Agent Task
    /// requests fail before any target lookup, which prevents forged IDs and
    /// prompt text from changing the denial result.
    pub fn authorize(
        &self,
        operation: AgentChatOperation,
        target_project_id: Option<&str>,
    ) -> std::result::Result<(), AgentChatPolicyError> {
        match self {
            Self::Main { .. } => match operation {
                AgentChatOperation::Discovery
                | AgentChatOperation::WebSearch
                | AgentChatOperation::ProjectLifecycle
                | AgentChatOperation::PortfolioRead
                | AgentChatOperation::HandoffPublish => Ok(()),
                AgentChatOperation::TaskManagement => Err(AgentChatPolicyError::MainTaskDenied),
                AgentChatOperation::RepositoryRead | AgentChatOperation::RepositoryWrite => {
                    Err(AgentChatPolicyError::RepositoryDenied)
                }
                AgentChatOperation::ProjectRead => Err(AgentChatPolicyError::OperationUnavailable),
            },
            Self::Project { project_id, .. } => match operation {
                AgentChatOperation::ProjectRead | AgentChatOperation::WebSearch => Ok(()),
                AgentChatOperation::TaskManagement => {
                    let Some(target_project_id) = target_project_id else {
                        return Err(AgentChatPolicyError::ProjectTaskTargetRequired);
                    };
                    if target_project_id == project_id {
                        Ok(())
                    } else {
                        Err(AgentChatPolicyError::ProjectTaskOutsideBinding)
                    }
                }
                AgentChatOperation::RepositoryRead | AgentChatOperation::RepositoryWrite => {
                    Err(AgentChatPolicyError::RepositoryDenied)
                }
                AgentChatOperation::Discovery
                | AgentChatOperation::ProjectLifecycle
                | AgentChatOperation::PortfolioRead
                | AgentChatOperation::HandoffPublish => {
                    Err(AgentChatPolicyError::OperationUnavailable)
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn main_chat_denies_task_management_before_target_lookup() {
        let scope = AgentChatScope::main("account-1");
        for target in [None, Some("task-project"), Some("forged-project")] {
            assert_eq!(
                scope.authorize(AgentChatOperation::TaskManagement, target),
                Err(AgentChatPolicyError::MainTaskDenied)
            );
        }
    }

    #[test]
    fn main_chat_denies_repository_access() {
        let scope = AgentChatScope::main("account-1");
        assert_eq!(
            scope.authorize(AgentChatOperation::RepositoryRead, Some("project-1")),
            Err(AgentChatPolicyError::RepositoryDenied)
        );
        assert_eq!(
            scope.authorize(AgentChatOperation::RepositoryWrite, None),
            Err(AgentChatPolicyError::RepositoryDenied)
        );
    }

    #[test]
    fn main_chat_allows_bounded_public_web_search() {
        let scope = AgentChatScope::main("account-1");
        assert_eq!(
            scope.authorize(AgentChatOperation::WebSearch, Some("forged-project")),
            Ok(())
        );
    }

    #[test]
    fn project_chat_can_manage_only_its_bound_project() {
        let scope = AgentChatScope::project("account-1", "project-1");
        assert_eq!(
            scope.authorize(AgentChatOperation::TaskManagement, Some("project-1")),
            Ok(())
        );
        assert_eq!(
            scope.authorize(AgentChatOperation::TaskManagement, Some("project-2")),
            Err(AgentChatPolicyError::ProjectTaskOutsideBinding)
        );
        assert_eq!(
            scope.authorize(AgentChatOperation::TaskManagement, None),
            Err(AgentChatPolicyError::ProjectTaskTargetRequired)
        );
    }

    #[test]
    fn project_chat_has_no_repository_access() {
        let scope = AgentChatScope::project("account-1", "project-1");
        assert_eq!(
            scope.authorize(AgentChatOperation::RepositoryRead, Some("project-1")),
            Err(AgentChatPolicyError::RepositoryDenied)
        );
        assert_eq!(
            scope.authorize(AgentChatOperation::RepositoryWrite, Some("project-1")),
            Err(AgentChatPolicyError::RepositoryDenied)
        );
    }

    #[test]
    fn project_chat_allows_bounded_public_web_search_only_as_project_scope() {
        let scope = AgentChatScope::project("account-1", "project-1");
        assert_eq!(scope.authorize(AgentChatOperation::WebSearch, None), Ok(()));
        assert_eq!(scope.project_id(), Some("project-1"));
    }

    #[test]
    fn policy_denials_do_not_include_untrusted_identifiers() {
        let scope = AgentChatScope::project("account-1", "project-1");
        let error = scope
            .authorize(AgentChatOperation::TaskManagement, Some("secret-project"))
            .expect_err("cross-project action must be denied")
            .to_string();
        assert!(!error.contains("secret-project"));
        assert!(!error.contains("project-1"));
    }

    #[test]
    fn content_guard_rejects_protected_values_before_persistence() {
        for content in [
            "Authorization: Bearer super-secret",
            "api_key=super-secret",
            "ghp_0123456789abcdef",
            "-----BEGIN OPENSSH PRIVATE KEY-----",
        ] {
            assert!(guard_agent_chat_content(content).is_err(), "{content}");
        }
    }

    #[test]
    fn content_guard_admits_task_prose_that_looks_like_a_key_prefix() {
        for content in [
            "Task-1: Backend & Storage (server.py)",
            "I will work through this task-by-task",
            "risk-free rollout after the disk-based cache lands",
            "the REST/JSON API. Key endpoints are listed below",
            "api key rotation is out of scope for this milestone",
        ] {
            assert!(guard_agent_chat_content(content).is_ok(), "{content}");
        }
    }

    #[test]
    fn content_guard_still_rejects_key_shaped_values() {
        for content in [
            "sk-proj-0123456789abcdefghij",
            "use sk-ant-api03-0123456789abcdef for the call",
            "sk-placeholder-key",
        ] {
            assert!(guard_agent_chat_content(content).is_err(), "{content}");
        }
    }

    #[test]
    fn content_guard_bounds_and_classifies_admitted_text() {
        let guarded = guard_agent_chat_content("  Continue with the Project Agent.  ")
            .expect("ordinary content is admitted");
        assert_eq!(guarded.content, "Continue with the Project Agent.");
        assert_eq!(guarded.sensitivity, "internal");
        assert!(guarded.guard_json.contains("forge-content-guard-v1"));
    }
}
