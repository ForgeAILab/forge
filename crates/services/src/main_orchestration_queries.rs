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
    MAIN_CHARTER_DIFF_OPERATION, MAIN_CHARTER_READINESS_OPERATION, MAIN_CHARTER_READ_OPERATION,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;

use crate::main_orchestration_actions::{parse_maturity, parse_project_mode};
use crate::{
    evaluate_project_charter_readiness, resolve_genesis_project_agent, semantic_revision_diff,
    OrchestrationAuthorizationService, Result, ServiceError, CHARTER_READINESS_POLICY_VERSION,
};

/// Read-only query service for the account-owned Main Agent Charter surface.
#[derive(Clone)]
pub struct MainOrchestrationQueryService {
    db: Arc<SqliteDb>,
    authorization: OrchestrationAuthorizationService,
}

impl MainOrchestrationQueryService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
            authorization: OrchestrationAuthorizationService::new(Arc::clone(&db)),
            db,
        }
    }

    /// Execute one server-bound Main Charter query.
    pub async fn execute(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
        arguments: Value,
    ) -> Result<Value> {
        match operation {
            MAIN_CHARTER_READ_OPERATION => {
                self.charter_read(actor_identity_id, scope, arguments).await
            }
            MAIN_CHARTER_READINESS_OPERATION => {
                self.charter_readiness(actor_identity_id, scope, arguments)
                    .await
            }
            MAIN_CHARTER_DIFF_OPERATION => {
                self.charter_diff(actor_identity_id, scope, arguments).await
            }
            MAIN_CHARTER_APPROVAL_TARGET_OPERATION => {
                self.charter_approval_target(actor_identity_id, scope, arguments)
                    .await
            }
            _ => Err(ServiceError::invalid_operation(
                "Main Charter query operation is not implemented",
            )),
        }
    }

    async fn charter_read(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
    ) -> Result<Value> {
        let query: CharterReadQuery = parse_query(&arguments, MAIN_CHARTER_READ_OPERATION)?;
        let account_id = self
            .authorization
            .main_account_id(actor_identity_id, scope)
            .await?;
        let limit = query.limit.unwrap_or(20).clamp(1, 50) as i64;
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

    async fn charter_readiness(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
    ) -> Result<Value> {
        let projection: CharterProjectionQuery = parse_query(&arguments, "charter.readiness")?;
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

    async fn charter_diff(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
    ) -> Result<Value> {
        let projection: CharterDiffQuery = parse_query(&arguments, "charter.diff")?;
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

    async fn charter_approval_target(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        arguments: Value,
    ) -> Result<Value> {
        let projection: CharterProjectionQuery =
            parse_query(&arguments, "charter.approval_target")?;
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
        let account_id = self
            .authorization
            .main_account_id(actor_identity_id, scope)
            .await?;
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CharterProjectionQuery {
    #[serde(default)]
    genesis_session_id: Option<String>,
    charter_id: String,
    revision_id: String,
    content_digest: String,
    render_digest: String,
    expected_charter_version: i64,
}

#[derive(Debug, Deserialize, Default)]
struct CharterReadQuery {
    #[serde(default)]
    charter_id: Option<String>,
    #[serde(default)]
    revision_id: Option<String>,
    #[serde(default)]
    genesis_session_id: Option<String>,
    #[serde(default)]
    limit: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CharterDiffQuery {
    #[serde(default)]
    genesis_session_id: Option<String>,
    charter_id: String,
    base_revision_id: String,
    candidate_revision_id: String,
}

fn parse_query<T: for<'de> Deserialize<'de>>(arguments: &Value, operation: &str) -> Result<T> {
    serde_json::from_value(arguments.clone()).map_err(|error| {
        ServiceError::invalid_operation(format!("{operation} query arguments are invalid: {error}"))
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
