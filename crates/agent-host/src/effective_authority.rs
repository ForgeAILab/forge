//! Per-admission provider boundary. Fresh facts can only narrow its ceiling.
use crate::{AgentHostError, CanonicalScope, ForgeToolProvider, RuntimeScopeBinding};
use async_trait::async_trait;
use operation_registry::authority::{AuthorityDenial, EffectiveAuthority};
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug)]
struct AuthorityBoundProvider {
    db: Arc<db::SqliteDb>,
    inner: Arc<dyn ForgeToolProvider>,
    admitted: EffectiveAuthority,
    remaining: std::sync::Mutex<EffectiveAuthority>,
    workspace: &'static str,
}
pub fn authority_bound_provider(
    db: Arc<db::SqliteDb>,
    binding: &RuntimeScopeBinding,
    provider: Arc<dyn ForgeToolProvider>,
) -> Arc<dyn ForgeToolProvider> {
    let Some(admitted) = binding.authority.clone() else {
        return provider;
    };
    let workspace = match binding.scope.workspace_access {
        crate::WorkspaceAccess::AccountScratch => "account_scratch",
        crate::WorkspaceAccess::ProjectVerify => "project_verify",
        _ => "deny",
    };
    Arc::new(AuthorityBoundProvider {
        db,
        inner: provider,
        remaining: std::sync::Mutex::new(admitted.clone()),
        admitted,
        workspace,
    })
}
impl AuthorityBoundProvider {
    async fn check(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        let scope_type = match scope.scope_type {
            crate::CanonicalScopeType::Account => "account",
            crate::CanonicalScopeType::Project => "project",
            crate::CanonicalScopeType::AgentChat => "agent_chat",
            crate::CanonicalScopeType::Task => "task",
        };
        if scope_type != self.admitted.scope_type || scope.scope_id != self.admitted.scope_id {
            return Err(AgentHostError::Authority(
                "invocation scope differs from admitted authority".into(),
            ));
        }
        let spec = operation_registry::READ_CATALOG
            .lookup(operation)
            .or_else(|| operation_registry::main_proposals::CATALOG.lookup(operation));
        let Some(spec) = spec else {
            return Ok(());
        };
        let current = self
            .db
            .resolve_effective_authority(
                actor,
                Some(&self.admitted.admitted_profile_id),
                &self.admitted.scope_type,
                &self.admitted.scope_id,
                self.workspace,
            )
            .await;
        let current = match current {
            Ok(current) => current,
            Err(db::DbError::NotFound) => {
                let mut remaining = self
                    .remaining
                    .lock()
                    .map_err(|_| AgentHostError::ProtectedPersistence)?;
                remaining.active = false;
                remaining.clone()
            }
            Err(_) => return Err(AgentHostError::ProtectedPersistence),
        };
        let bounded = {
            let mut remaining = self
                .remaining
                .lock()
                .map_err(|_| AgentHostError::ProtectedPersistence)?;
            match remaining.narrowed(&current) {
                Ok(bounded) => {
                    *remaining = bounded.clone();
                    Ok(bounded)
                }
                Err(denial) => {
                    remaining.active = false;
                    Err(denial)
                }
            }
        };
        let result = self.admitted.evaluate(spec).and_then(|_| {
            bounded
                .and_then(|bounded| bounded.evaluate(spec))
                .map_err(|denial| match denial {
                    AuthorityDenial::PermissionMissing(_) => AuthorityDenial::Revoked,
                    denial => denial,
                })
        });
        result.map_err(|denial| {
            let cause = match denial {
                AuthorityDenial::PermissionMissing(permission) => {
                    api_types::DeniedBy::PermissionMissing(permission)
                }
                AuthorityDenial::StateChanged => api_types::DeniedBy::CharterNotAdopted,
                AuthorityDenial::Revoked => api_types::DeniedBy::AuthorityRevoked,
                AuthorityDenial::PrincipalMismatch => api_types::DeniedBy::OperationNotInScope,
            };
            AgentHostError::StructuredOutcome(Box::new(
                api_types::OrchestrationOutcome::terminal_denial(
                    operation,
                    api_types::CanonicalScopeRef::new(
                        match scope.scope_type {
                            crate::CanonicalScopeType::Account => {
                                api_types::OutcomeScopeType::Account
                            }
                            crate::CanonicalScopeType::Project => {
                                api_types::OutcomeScopeType::Project
                            }
                            crate::CanonicalScopeType::AgentChat => {
                                api_types::OutcomeScopeType::AgentChat
                            }
                            crate::CanonicalScopeType::Task => api_types::OutcomeScopeType::Task,
                        },
                        &scope.scope_id,
                    ),
                    "authority",
                    cause,
                ),
            ))
        })
    }
}
#[async_trait]
impl ForgeToolProvider for AuthorityBoundProvider {
    fn admitted_authority(&self) -> Option<&EffectiveAuthority> {
        Some(&self.admitted)
    }
    async fn read(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        self.check(actor, scope, operation).await?;
        self.inner.read(actor, scope, operation, arguments).await
    }
    async fn proposal_denial(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        self.check(actor, scope, operation).await?;
        self.inner.proposal_denial(actor, scope, operation).await
    }
    async fn read_denial(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        self.check(actor, scope, operation).await?;
        self.inner.read_denial(actor, scope, operation).await
    }
    async fn propose(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        runtime: &str,
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        self.check(actor, scope, operation).await?;
        let authority = self
            .remaining
            .lock()
            .map_err(|_| AgentHostError::ProtectedPersistence)?
            .clone();
        self.inner
            .propose_admitted(
                actor, scope, runtime, operation, arguments, &authority, false,
            )
            .await
    }
    async fn propose_prepared(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        runtime: &str,
        operation: &str,
        arguments: Value,
    ) -> Result<Value, AgentHostError> {
        self.check(actor, scope, operation).await?;
        let authority = self
            .remaining
            .lock()
            .map_err(|_| AgentHostError::ProtectedPersistence)?
            .clone();
        self.inner
            .propose_admitted(
                actor, scope, runtime, operation, arguments, &authority, true,
            )
            .await
    }
    async fn record_terminal_denial(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        session: &str,
        operation: &str,
        cause: &api_types::DeniedBy,
    ) -> Result<(), AgentHostError> {
        self.inner
            .record_terminal_denial(actor, scope, session, operation, cause)
            .await
    }
    async fn clear_terminal_denials(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        session: &str,
        operation: &str,
    ) -> Result<(), AgentHostError> {
        self.inner
            .clear_terminal_denials(actor, scope, session, operation)
            .await
    }
    async fn observe_command(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        observation: crate::CommandObservation,
    ) -> Result<Value, AgentHostError> {
        self.inner.observe_command(actor, scope, observation).await
    }
    fn public_search_configured(&self) -> bool {
        self.inner.public_search_configured()
    }
    async fn public_search(
        &self,
        actor: &str,
        scope: &CanonicalScope,
        search: crate::PublicSearchScope,
        query: &str,
        limit: u64,
    ) -> Result<Value, AgentHostError> {
        self.inner
            .public_search(actor, scope, search, query, limit)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{AgentRepo, AgentStatus, CreateAgent};
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Debug, Default)]
    struct CountingProvider(AtomicUsize, AtomicUsize);
    #[async_trait]
    impl ForgeToolProvider for CountingProvider {
        async fn record_terminal_denial(
            &self,
            _: &str,
            _: &CanonicalScope,
            _: &str,
            _: &str,
            _: &api_types::DeniedBy,
        ) -> Result<(), AgentHostError> {
            self.1.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn read(
            &self,
            _: &str,
            _: &CanonicalScope,
            _: &str,
            _: Value,
        ) -> Result<Value, AgentHostError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(serde_json::json!({}))
        }
        async fn propose(
            &self,
            _: &str,
            _: &CanonicalScope,
            _: &str,
            _: &str,
            _: Value,
        ) -> Result<Value, AgentHostError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(serde_json::json!({}))
        }
    }
    async fn select_policy(db: &db::SqliteDb, policy: &str) -> db::Agent {
        let identity = db::AgentRepo::get_by_id(db, "agent")
            .await
            .unwrap()
            .unwrap();
        let now = db::now_rfc3339();
        let profile_id = db::new_uuid_v4();
        let (_, identity) = db::AgentProfileRepo::create_and_select_profile(
            db,
            db::CreateAgentProfile {
                id: profile_id.clone(),
                identity_id: "agent".into(),
                backend_kind: "cli".into(),
                executor_type: "codex".into(),
                provider: None,
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "[]".into(),
                tool_policy_json: policy.into(),
                config_json: "{}".into(),
                credential_ref: None,
                daemon_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            db::SelectAgentProfile {
                identity_id: "agent".into(),
                profile_id,
                expected_version: identity.version,
                updated_at: now,
            },
        )
        .await
        .unwrap();
        identity
    }
    #[tokio::test]
    async fn pinned_provider_refuses_widening_and_current_revocation_without_effects() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = Arc::new(db::SqliteDb::new(pool));
        let now = db::now_rfc3339();
        db::UserRepo::create_user(
            &*db,
            &db::User {
                id: "owner".into(),
                email: "owner@example.test".into(),
                password_hash: "test".into(),
                display_name: None,
                is_admin: false,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        let identity = AgentRepo::create(
            &*db,
            CreateAgent {
                id: "agent".into(),
                name: "Agent".into(),
                description: None,
                executor_type: "codex".into(),
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "[]".into(),
                config_json: "{}".into(),
                credential_ref: None,
                daemon_id: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: Some("owner".into()),
                visibility: "account".into(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        db::AccountMainAgentBindingRepo::create_main_binding(
            &*db,
            db::CreateAccountMainAgentBinding {
                id: "binding".into(),
                account_id: "owner".into(),
                identity_id: "agent".into(),
                profile_id: identity.profile_id,
                autonomy_policy_json: "{}".into(),
                tool_policy_revision: "test".into(),
                created_at: db::now_rfc3339(),
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .unwrap();
        let identity = select_policy(&db, r#"{"permissions":["read_account"]}"#).await;
        let admitted = db
            .resolve_effective_authority(
                "agent",
                Some(&identity.profile_id),
                "account",
                "owner",
                "deny",
            )
            .await
            .unwrap();
        let inner = Arc::new(CountingProvider::default());
        let provider = AuthorityBoundProvider {
            db: db.clone(),
            inner: inner.clone(),
            remaining: std::sync::Mutex::new(admitted.clone()),
            admitted,
            workspace: "deny",
        };
        let scope = CanonicalScope {
            scope_type: crate::CanonicalScopeType::Account,
            scope_id: "owner".into(),
            workspace_access: crate::WorkspaceAccess::Deny,
        };
        provider
            .read("agent", &scope, "account.summary", serde_json::json!({}))
            .await
            .unwrap();
        select_policy(&db, r#"{"permissions":["read_account","propose_project"]}"#).await;
        let next = db
            .resolve_effective_authority("agent", None, "account", "owner", "deny")
            .await
            .unwrap();
        assert!(
            next.evaluate(
                operation_registry::main_proposals::CATALOG
                    .lookup("project.create")
                    .unwrap()
            )
            .is_ok()
        );
        assert!(
            provider
                .propose(
                    "agent",
                    &scope,
                    "runtime",
                    "project.create",
                    serde_json::json!({})
                )
                .await
                .is_err()
        );
        assert_eq!(inner.0.load(Ordering::SeqCst), 1);
        select_policy(&db, r#"{"permissions":[]}"#).await;
        let AgentHostError::StructuredOutcome(outcome) = provider
            .read("agent", &scope, "account.summary", serde_json::json!({}))
            .await
            .unwrap_err()
        else {
            panic!("typed narrowing");
        };
        assert_eq!(
            outcome.denied_by,
            Some(api_types::DeniedBy::AuthorityRevoked)
        );
        select_policy(&db, r#"{"permissions":["read_account","propose_project"]}"#).await;
        assert!(
            provider
                .read("agent", &scope, "account.summary", serde_json::json!({}))
                .await
                .is_err(),
            "same turn cannot restore a revoked grant"
        );
        let fresh = db
            .resolve_effective_authority("agent", None, "account", "owner", "deny")
            .await
            .unwrap();
        let replacement_turn = AuthorityBoundProvider {
            db: db.clone(),
            inner: inner.clone(),
            remaining: std::sync::Mutex::new(fresh.clone()),
            admitted: fresh,
            workspace: "deny",
        };
        sqlx::query(
            "UPDATE account_main_agent_binding SET state = 'replaced' WHERE id = 'binding'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let AgentHostError::StructuredOutcome(outcome) = replacement_turn
            .read("agent", &scope, "account.summary", serde_json::json!({}))
            .await
            .unwrap_err()
        else {
            panic!("typed binding revocation");
        };
        assert_eq!(
            outcome.denied_by,
            Some(api_types::DeniedBy::AuthorityRevoked)
        );
        // The real composed tool transports one typed revocation before
        // malformed root arguments, and records it only once in the turn.
        let granted = replacement_turn.admitted.ceiling.clone();
        let composition = crate::ScopeToolComposition::for_scope_with_permissions(
            "agent",
            scope.clone(),
            None,
            None,
            &granted,
            Some(Arc::new(replacement_turn)),
        )
        .unwrap();
        let tool = composition
            .tools()
            .into_iter()
            .find(|tool| tool.spec().name == "forge_scope_read")
            .unwrap();
        for call in ["first", "repeated"] {
            use agent_runtime::core::{
                cancel::Cancellation,
                clock::{Deadline, SystemClock},
                ids::{RequestId, SessionId, ToolCallId, TurnId},
                prelude::{InvocationContext, PreparationContext},
                workspace::DenyAllWorkspace,
            };
            let preparation = PreparationContext {
                session: SessionId::new("session"),
                turn: Some(TurnId::new("turn")),
                call_id: ToolCallId::new(call),
                request: RequestId::new("request"),
                workspace: Arc::new(DenyAllWorkspace),
                clock: Arc::new(SystemClock),
                cancel: Cancellation::new(),
                deadline: Deadline::never(),
            };
            let prepared = tool
                .prepare(
                    serde_json::json!({"operation":"account.summary","unexpected":true}),
                    &preparation,
                )
                .await
                .unwrap();
            let invocation = InvocationContext {
                session: preparation.session,
                turn: preparation.turn,
                call_id: preparation.call_id,
                request: preparation.request,
                workspace: preparation.workspace,
                clock: preparation.clock,
                cancel: preparation.cancel,
                deadline: preparation.deadline,
                output_limit: 128 * 1024,
            };
            let outcome = tool.invoke(prepared, &invocation).await.unwrap();
            assert!(outcome.is_error);
            assert_eq!(outcome.value["denied_by"], "authority_revoked");
        }
        assert_eq!(
            inner.1.load(Ordering::SeqCst),
            1,
            "one turn revocation record"
        );
        sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = 'agent'")
            .execute(db.pool())
            .await
            .unwrap();
        let AgentHostError::StructuredOutcome(outcome) = provider
            .read("agent", &scope, "account.summary", serde_json::json!({}))
            .await
            .unwrap_err()
        else {
            panic!("typed revocation");
        };
        assert_eq!(
            outcome.denied_by,
            Some(api_types::DeniedBy::AuthorityRevoked)
        );
        assert_eq!(inner.0.load(Ordering::SeqCst), 1);
    }
}
