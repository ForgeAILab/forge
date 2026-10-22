//! Shared canonical-scope authorization and target derivation.
//!
//! Transport adapters receive an already selected canonical scope, but they
//! must not turn that scope into a domain target with adapter-owned SQL.  This
//! service is the shared boundary for Main-account and Project-target
//! resolution used by native orchestration tools and the command/query
//! services beneath them.

use std::sync::Arc;

use db::SqliteDb;
use forge_agent_host::{CanonicalScope, CanonicalScopeType};

use crate::{Result, ServiceError};

/// Server-derived targets for canonical orchestration operations.
#[derive(Clone)]
pub struct OrchestrationAuthorizationService {
    db: Arc<SqliteDb>,
}

impl OrchestrationAuthorizationService {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// Resolve and authorize the account represented by a Main scope.
    ///
    /// The account is always derived from the persisted identity/chat binding;
    /// model-supplied account identifiers are never consulted.
    pub async fn main_account_id(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
    ) -> Result<String> {
        let owner_id = self
            .db
            .orchestration_actor_account_owner(actor_identity_id)
            .await?
            .flatten()
            .ok_or_else(|| {
                ServiceError::not_found("agent_identity", actor_identity_id.to_owned())
            })?;

        let account_id = match scope.scope_type {
            CanonicalScopeType::Account => {
                if scope.scope_id != owner_id {
                    return Err(ServiceError::AuthorizationDenied {
                        message: "Main Agent scope is not bound to this identity".to_owned(),
                    });
                }
                scope.scope_id.clone()
            }
            CanonicalScopeType::AgentChat => {
                let row = self
                    .db
                    .orchestration_main_chat_scope(&scope.scope_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("agent_chat", scope.scope_id.clone()))?;
                let kind: String = row.kind()?;
                if kind != "account_main" {
                    return Err(ServiceError::AuthorizationDenied {
                        message: "global Main Agent operations are unavailable in Project Chat"
                            .to_owned(),
                    });
                }
                row.account_id()?.ok_or_else(|| {
                    ServiceError::invalid_operation("Main Agent account is unavailable")
                })?
            }
            _ => {
                return Err(ServiceError::AuthorizationDenied {
                    message: "global Main Agent operation is unavailable in this scope".to_owned(),
                });
            }
        };

        if owner_id != account_id {
            return Err(ServiceError::AuthorizationDenied {
                message: "actor identity does not own the Main Agent scope".to_owned(),
            });
        }

        let binding =
            db::AccountMainAgentBindingRepo::get_active_main_binding(&*self.db, &account_id)
                .await?
                .ok_or_else(|| ServiceError::AuthorizationDenied {
                    message: "Main Agent identity is not actively bound to this account".to_owned(),
                })?;
        if binding.identity_id != actor_identity_id || binding.state != "active" {
            return Err(ServiceError::AuthorizationDenied {
                message: "Main Agent identity is not actively bound to this account".to_owned(),
            });
        }
        Ok(account_id)
    }

    /// Derive the account target after the shared evaluator admits the caller.
    /// This method loads no identity, Profile or binding policy.
    pub async fn main_account_target(&self, scope: &CanonicalScope) -> Result<String> {
        match scope.scope_type {
            CanonicalScopeType::Account => Ok(scope.scope_id.clone()),
            CanonicalScopeType::AgentChat => self
                .db
                .orchestration_main_chat_account_target(&scope.scope_id)
                .await?
                .flatten()
                .ok_or_else(|| ServiceError::AuthorizationDenied {
                    message: "global Main Agent operations are unavailable in Project Chat".into(),
                }),
            _ => Err(ServiceError::AuthorizationDenied {
                message: "global Main Agent operation is unavailable in this scope".into(),
            }),
        }
    }

    /// Resolve the Project target for a direct command.
    ///
    /// This lookup is deliberately structural.  The command service performs
    /// the authoritative receipt-first authorization check before mutation;
    /// keeping the target derivation here avoids a transport-owned SQL path.
    pub async fn direct_project_target(&self, scope: &CanonicalScope) -> Result<String> {
        match scope.scope_type {
            CanonicalScopeType::Project => Ok(scope.scope_id.clone()),
            CanonicalScopeType::AgentChat => {
                let project_id = self
                    .db
                    .orchestration_direct_chat_project_target(&scope.scope_id)
                    .await?
                    .flatten();
                project_id.ok_or_else(|| {
                    ServiceError::invalid_operation(
                        "Project Agent Chat has no owning Project".to_owned(),
                    )
                })
            }
            _ => Err(ServiceError::AuthorizationDenied {
                message: "direct Project commands require a Project or Project Agent Chat scope"
                    .to_owned(),
            }),
        }
    }

    /// Resolve the Project owned by the authenticated Project-Agent binding.
    pub async fn project_orchestration_target(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
    ) -> Result<String> {
        match scope.scope_type {
            CanonicalScopeType::Project => {
                let project_id = self
                    .db
                    .orchestration_bound_project_target(actor_identity_id, &scope.scope_id)
                    .await?
                    .flatten();
                project_id.ok_or_else(|| ServiceError::AuthorizationDenied {
                    message: "Project Agent binding does not own this Project scope".to_owned(),
                })
            }
            CanonicalScopeType::AgentChat => {
                let row = self
                    .db
                    .orchestration_bound_project_chat_scope(actor_identity_id, &scope.scope_id)
                    .await?
                    .ok_or_else(|| ServiceError::AuthorizationDenied {
                        message: "Project Agent Chat is not bound to this identity".to_owned(),
                    })?;
                let _kind: String = row.kind()?;
                row.project_id()?
                    .ok_or_else(|| ServiceError::AuthorizationDenied {
                        message: "Project Agent Chat has no owning Project".to_owned(),
                    })
            }
            _ => Err(ServiceError::AuthorizationDenied {
                message: "Project orchestration is unavailable outside the bound Project scope"
                    .to_owned(),
            }),
        }
    }
}
