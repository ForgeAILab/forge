//! Read-only Main Agent Charter queries.
//!
//! These projections deliberately do not pass through `AgentAction`.  They
//! derive the account and Main Chat binding from the server-owned identity and
//! canonical scope, then read only Genesis-owned Charter state.

use std::sync::Arc;

use api_types::{ProductGenesisLifecycle, ProjectCharterContent};
use db::{
    now_rfc3339, ProjectCharterRecord, ProjectCharterRevisionRecord, ProjectOrchestrationRepo,
    SqliteDb,
};
use forge_agent_host::{
    CanonicalScope, CanonicalScopeType, MAIN_CHARTER_APPROVAL_TARGET_OPERATION,
    MAIN_CHARTER_DIFF_OPERATION, MAIN_CHARTER_READINESS_OPERATION,
    MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION,
};
use operation_registry::main_reads::{
    CharterDiffQuery, CharterProjectionQuery, CharterReadQuery, GenesisProjectAgentsQuery,
};
use serde_json::{json, Value};
use sqlx::Row;

use crate::main_orchestration_actions::{parse_maturity, parse_project_mode};
use crate::{
    evaluate_project_charter_readiness, list_genesis_project_agents, resolve_genesis_project_agent,
    semantic_revision_diff, OrchestrationAuthorizationService, Result, ServiceError,
    CHARTER_READINESS_POLICY_VERSION,
};

/// Read-only query service for the account-owned Main Agent Charter surface.
#[derive(Clone)]
pub struct MainOrchestrationQueryService {
    db: Arc<SqliteDb>,
    authorization: OrchestrationAuthorizationService,
    admitted: Option<operation_registry::authority::EffectiveAuthority>,
}

impl MainOrchestrationQueryService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
            authorization: OrchestrationAuthorizationService::new(Arc::clone(&db)),
            db,
            admitted: None,
        }
    }

    pub(crate) fn for_admission(
        &self,
        admitted: Option<&operation_registry::authority::EffectiveAuthority>,
    ) -> Self {
        let mut service = self.clone();
        service.admitted = admitted.cloned();
        service
    }
    async fn main_target(&self, actor: &str, scope: &CanonicalScope) -> Result<String> {
        if !matches!(
            scope.scope_type,
            CanonicalScopeType::Account | CanonicalScopeType::AgentChat
        ) {
            return Err(ServiceError::AuthorizationDenied {
                message: "global Main Agent operation is unavailable in this scope".into(),
            });
        }
        let resolved;
        let authority = if let Some(admitted) = &self.admitted {
            admitted
        } else {
            resolved = self
                .db
                .resolve_effective_authority(
                    actor,
                    None,
                    if scope.scope_type == CanonicalScopeType::Account {
                        "account"
                    } else {
                        "agent_chat"
                    },
                    &scope.scope_id,
                    "deny",
                )
                .await?;
            &resolved
        };
        authority
            .evaluate(
                operation_registry::READ_CATALOG
                    .lookup("charter.read")
                    .unwrap(),
            )
            .map_err(|_| ServiceError::AuthorizationDenied {
                message: "Main query authority is unavailable in this scope".into(),
            })?;
        self.authorization.main_account_target(scope).await
    }

    pub async fn project_agents(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        query: GenesisProjectAgentsQuery,
    ) -> Result<Value> {
        let (_account_id, session) = self
            .main_genesis(
                actor_identity_id,
                scope,
                query.genesis_session_id.as_deref(),
                None,
            )
            .await?;
        let candidates = list_genesis_project_agents(&self.db, &session).await?;
        let resolved = resolve_genesis_project_agent(&self.db, &session).await?;
        Ok(json!({
            "operation": MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION,
            "genesis_session_id": session.id,
            "session_version": session.version,
            "preferred_project_agent_identity_id": session.preferred_project_agent_identity_id,
            "resolved_project_agent": resolved.map(selection_json),
            "items": candidates.into_iter().map(selection_json).collect::<Vec<_>>(),
        }))
    }

    pub async fn charter_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        query: CharterReadQuery,
    ) -> Result<Value> {
        let account_id = self.main_target(actor_identity_id, scope).await?;
        let limit = 20_i64;
        let rows = sqlx::query(
            "SELECT id, genesis_session_id, current_draft_revision_id,
                    current_approved_revision_id, project_mode, maturity,
                    lifecycle, version, updated_at
             FROM project_charter
             WHERE account_id = ? AND project_id IS NULL
               AND (? IS NULL OR id = ?)
               AND (? IS NULL OR genesis_session_id = ?)
               AND (? IS NULL OR current_draft_revision_id = ?
                    OR current_approved_revision_id = ?)
             ORDER BY updated_at DESC, id DESC LIMIT ?",
        )
        .bind(account_id)
        .bind(&query.charter_id)
        .bind(&query.charter_id)
        .bind(&query.genesis_session_id)
        .bind(&query.genesis_session_id)
        .bind(&query.revision_id)
        .bind(&query.revision_id)
        .bind(&query.revision_id)
        .bind(limit)
        .fetch_all(self.db.pool())
        .await?;
        Ok(json!({
            "scope": "main",
            "items": rows.into_iter().map(|row| json!({
                "id": row.try_get::<String, _>("id").unwrap_or_default(),
                "genesis_session_id": row.try_get::<Option<String>, _>("genesis_session_id").ok().flatten(),
                "current_draft_revision_id": row.try_get::<Option<String>, _>("current_draft_revision_id").ok().flatten(),
                "current_approved_revision_id": row.try_get::<Option<String>, _>("current_approved_revision_id").ok().flatten(),
                "project_mode": row.try_get::<String, _>("project_mode").unwrap_or_default(),
                "maturity": row.try_get::<String, _>("maturity").unwrap_or_default(),
                "lifecycle": row.try_get::<String, _>("lifecycle").unwrap_or_default(),
                "version": row.try_get::<i64, _>("version").unwrap_or_default(),
                "updated_at": row.try_get::<String, _>("updated_at").unwrap_or_default(),
            })).collect::<Vec<_>>()
        }))
    }

    pub async fn charter_readiness(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        projection: CharterProjectionQuery,
    ) -> Result<Value> {
        let (_account_id, session) = self
            .main_genesis(
                actor_identity_id,
                scope,
                projection.genesis_session_id.as_deref(),
                Some(&projection.charter_id),
            )
            .await?;
        let charter = self
            .charter_for(&session.account_id, &projection.charter_id)
            .await?;
        if session.charter_id.as_deref() != Some(charter.id.as_str()) {
            return Err(ServiceError::invalid_operation(
                "Charter readiness target is not owned by this Genesis session",
            ));
        }
        let revision = self
            .charter_revision_for(&charter, &projection.revision_id)
            .await?;
        validate_projection_freshness(&charter, &revision, &projection)?;
        let content: ProjectCharterContent = serde_json::from_str(&revision.content_json)
            .map_err(|_| ServiceError::invalid_operation("persisted Charter content is invalid"))?;
        let project_mode = parse_project_mode(&charter.project_mode)?;
        let maturity = parse_maturity(&charter.maturity)?;
        let readiness = evaluate_project_charter_readiness(
            &content,
            project_mode,
            maturity,
            CHARTER_READINESS_POLICY_VERSION,
            &now_rfc3339(),
        );
        Ok(json!({
            "operation": MAIN_CHARTER_READINESS_OPERATION,
            "genesis_session_id": session.id,
            "charter_id": charter.id,
            "revision_id": revision.id,
            "readiness": readiness,
        }))
    }

    pub async fn charter_diff(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        projection: CharterDiffQuery,
    ) -> Result<Value> {
        let (_account_id, session) = self
            .main_genesis(
                actor_identity_id,
                scope,
                projection.genesis_session_id.as_deref(),
                Some(&projection.charter_id),
            )
            .await?;
        let charter = self
            .charter_for(&session.account_id, &projection.charter_id)
            .await?;
        if session.charter_id.as_deref() != Some(charter.id.as_str()) {
            return Err(ServiceError::invalid_operation(
                "Charter diff target is not owned by this Genesis session",
            ));
        }
        let current = self
            .charter_revision_for(&charter, &projection.candidate_revision_id)
            .await?;
        let current_content: ProjectCharterContent = serde_json::from_str(&current.content_json)
            .map_err(|_| ServiceError::invalid_operation("persisted Charter content is invalid"))?;
        let previous = self
            .charter_revision_for(&charter, &projection.base_revision_id)
            .await?;
        let previous_content = serde_json::from_str::<ProjectCharterContent>(
            &previous.content_json,
        )
        .map_err(|_| ServiceError::invalid_operation("persisted Charter content is invalid"))?;
        let diff = semantic_revision_diff(Some(&previous_content), &current_content);
        Ok(json!({
            "operation": MAIN_CHARTER_DIFF_OPERATION,
            "genesis_session_id": session.id,
            "charter_id": charter.id,
            "revision_id": current.id,
            "schema_version": diff.schema_version,
            "changed_sections": diff.changed_sections,
            "changes": diff.changes.into_iter().map(|change| json!({
                "section": change.section,
                "field": change.field,
                "before": change.before,
                "after": change.after,
            })).collect::<Vec<_>>(),
        }))
    }

    pub async fn charter_approval_target(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        projection: CharterProjectionQuery,
    ) -> Result<Value> {
        let (_account_id, session) = self
            .main_genesis(
                actor_identity_id,
                scope,
                projection.genesis_session_id.as_deref(),
                Some(&projection.charter_id),
            )
            .await?;
        let charter = self
            .charter_for(&session.account_id, &projection.charter_id)
            .await?;
        if session.charter_id.as_deref() != Some(charter.id.as_str()) {
            return Err(ServiceError::invalid_operation(
                "Charter approval target is not owned by this Genesis session",
            ));
        }
        let revision = self
            .charter_revision_for(&charter, &projection.revision_id)
            .await?;
        validate_projection_freshness(&charter, &revision, &projection)?;
        let content: ProjectCharterContent = serde_json::from_str(&revision.content_json)
            .map_err(|_| ServiceError::invalid_operation("persisted Charter content is invalid"))?;
        let project_mode = parse_project_mode(&charter.project_mode)?;
        let maturity = parse_maturity(&charter.maturity)?;
        let readiness = evaluate_project_charter_readiness(
            &content,
            project_mode,
            maturity,
            CHARTER_READINESS_POLICY_VERSION,
            &now_rfc3339(),
        );
        let selected = resolve_genesis_project_agent(&self.db, &session)
            .await?
            .map(|selection| {
                json!({
                    "identity_id": selection.identity_id,
                    "display_name": selection.display_name,
                    "profile_revision_id": selection.profile_revision_id,
                    "operating_skill_revision": selection.operating_skill_revision,
                    "policy_digest": selection.policy_digest,
                })
            });
        Ok(json!({
            "operation": MAIN_CHARTER_APPROVAL_TARGET_OPERATION,
            "genesis_session_id": session.id,
            "charter_id": charter.id,
            "revision_id": revision.id,
            "expected_charter_version": charter.version,
            "approved_project_name": content.identity.working_name,
            "approved_project_slug": content.identity.slug_proposal,
            "project_mode": project_mode,
            "maturity": maturity,
            "content_digest": revision.content_digest,
            "render_digest": revision.rendered_digest,
            "readiness": readiness,
            "selected_project_agent": selected,
        }))
    }

    async fn main_genesis(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        session_id: Option<&str>,
        charter_id: Option<&str>,
    ) -> Result<(String, api_types::ProductGenesisSession)> {
        let account_id = self.main_target(actor_identity_id, scope).await?;
        let session_id = match session_id {
            Some(session_id) if !session_id.trim().is_empty() => session_id.to_owned(),
            _ => {
                let query = if charter_id.is_some() {
                    "SELECT id FROM product_genesis_session
                     WHERE account_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
                     ORDER BY CASE WHEN charter_id = ? THEN 0
                                   WHEN charter_id IS NULL THEN 1
                                   ELSE 2 END,
                              updated_at DESC, id DESC LIMIT 1"
                } else {
                    "SELECT id FROM product_genesis_session
                     WHERE account_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
                     ORDER BY updated_at DESC, id DESC LIMIT 1"
                };
                let mut request = sqlx::query_scalar::<_, String>(query).bind(&account_id);
                if let Some(charter_id) = charter_id {
                    request = request.bind(charter_id);
                }
                request
                    .fetch_optional(self.db.pool())
                    .await?
                    .ok_or_else(|| {
                        ServiceError::not_found("product_genesis_session", account_id.clone())
                    })?
            }
        };
        let session = crate::ProductGenesisService::for_sqlite(Arc::clone(&self.db))
            .get(&session_id)
            .await?;
        if session.account_id != account_id {
            return Err(ServiceError::not_found(
                "product_genesis_session",
                session_id.to_owned(),
            ));
        }
        if scope.scope_type == CanonicalScopeType::AgentChat
            && scope.scope_id != session.main_chat_id
        {
            return Err(ServiceError::AuthorizationDenied {
                message: "Main query scope does not match the Genesis Main Chat".to_owned(),
            });
        }
        if !matches!(
            session.lifecycle,
            ProductGenesisLifecycle::Discovering | ProductGenesisLifecycle::ReadyForProject
        ) {
            return Err(ServiceError::invalid_operation(
                "Main Charter orchestration is only available during active Product Genesis",
            ));
        }
        Ok((account_id, session))
    }

    async fn charter_for(
        &self,
        account_id: &str,
        charter_id: &str,
    ) -> Result<ProjectCharterRecord> {
        ProjectOrchestrationRepo::get_project_charter_for_account(&*self.db, charter_id, account_id)
            .await?
            .filter(|charter| charter.project_id.is_none())
            .ok_or_else(|| ServiceError::not_found("project_charter", charter_id.to_owned()))
    }

    async fn charter_revision_for(
        &self,
        charter: &ProjectCharterRecord,
        revision_id: &str,
    ) -> Result<ProjectCharterRevisionRecord> {
        ProjectOrchestrationRepo::get_project_charter_revision(&*self.db, revision_id)
            .await?
            .filter(|revision| revision.charter_id == charter.id)
            .ok_or_else(|| {
                ServiceError::not_found("project_charter_revision", revision_id.to_owned())
            })
    }
}

fn selection_json(selection: crate::GenesisAgentSelection) -> Value {
    json!({
        "identity_id": selection.identity_id,
        "display_name": selection.display_name,
        "profile_revision_id": selection.profile_revision_id,
        "operating_skill_revision": selection.operating_skill_revision,
        "policy_digest": selection.policy_digest,
    })
}

fn validate_projection_freshness(
    charter: &ProjectCharterRecord,
    revision: &ProjectCharterRevisionRecord,
    projection: &CharterProjectionQuery,
) -> Result<()> {
    if charter.version != projection.expected_charter_version {
        return Err(ServiceError::Db(db::DbError::VersionConflict));
    }
    if revision.content_digest != projection.content_digest {
        return Err(ServiceError::conflict(
            "Charter target content digest is stale",
        ));
    }
    if revision.rendered_digest != projection.render_digest {
        return Err(ServiceError::conflict(
            "Charter target render digest is stale",
        ));
    }
    Ok(())
}
