//! Immutable review provenance and canonical source reads.
use crate::{DbError, Result, SqliteDb};
use api_types::{ReviewConformance, ReviewContract};
use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::{Row, SqliteConnection};

#[async_trait]
pub trait ReviewConformanceRepo: Send + Sync {
    async fn review_source(&self, task_id: &str) -> Result<Value>;
    async fn lock_review_integration(&self, task_id: &str) -> Result<ReviewIntegrationGuard>;
    async fn create_review_contract(&self, contract: &ReviewContract) -> Result<()>;
    async fn review_contract(&self, execution_id: &str) -> Result<Option<ReviewContract>>;
    async fn record_review_conformance(&self, result: &ReviewConformance) -> Result<()>;
    async fn review_conformance(&self, execution_id: &str) -> Result<Option<ReviewConformance>>;
}

/// Holds the authority write lock only across the local, bounded integration step.
/// No provider or test command may run while this guard is held.
pub struct ReviewIntegrationGuard {
    pub contract: Option<ReviewContract>,
    transaction: sqlx::Transaction<'static, sqlx::Sqlite>,
}
impl ReviewIntegrationGuard {
    pub async fn release(self) -> Result<()> {
        self.transaction.commit().await?;
        Ok(())
    }
}

fn json_error(error: serde_json::Error) -> DbError {
    DbError::Check(error.to_string())
}

pub(crate) async fn review_source_in_tx(
    conn: &mut SqliteConnection,
    task_id: &str,
) -> Result<Value> {
    let row = sqlx::query(
        "SELECT t.id, t.project_id, t.repo_id, t.title, t.description, t.task_type,
                t.plan, t.task_state_config, t.merge_config, r.default_branch, p.workflow_definition, p.settings,
                p.charter_status, p.charter_setup_required, p.current_charter_revision_id,
                g.charter_revision_id AS task_charter_revision_id, g.document_revisions_json,
                g.plan_item_id, g.milestone_id, g.capability_class
         FROM task t JOIN project p ON p.id = t.project_id
         LEFT JOIN repo r ON r.id = t.repo_id
         LEFT JOIN project_task_governance g ON g.task_id = t.id AND g.project_id = t.project_id
         WHERE t.id = ? AND t.deleted_at IS NULL")
        .bind(task_id).fetch_optional(&mut *conn).await?.ok_or(DbError::NotFound)?;
    let project_id: String = row.try_get("project_id")?;
    let charter_id: Option<String> = row.try_get("current_charter_revision_id")?;
    let charter = if let Some(id) = &charter_id {
        let r = sqlx::query("SELECT r.*, c.project_id, c.current_approved_revision_id FROM project_charter_revision r JOIN project_charter c ON c.id = r.charter_id WHERE r.id = ? AND c.project_id = ?")
            .bind(id).bind(&project_id).fetch_optional(&mut *conn).await?.ok_or(DbError::NotFound)?;
        Some(
            json!({"id": id, "approved_id": r.try_get::<Option<String>,_>("current_approved_revision_id")?,
            "content": serde_json::from_str::<Value>(&r.try_get::<String,_>("content_json")?).map_err(json_error)?,
            "content_digest": r.try_get::<String,_>("content_digest")?}),
        )
    } else {
        None
    };
    let refs: Vec<String> = serde_json::from_str(
        row.try_get::<Option<&str>, _>("document_revisions_json")?
            .unwrap_or("[]"),
    )
    .map_err(json_error)?;
    let mut documents = Vec::new();
    for id in refs {
        let r = sqlx::query("SELECT r.* FROM project_document_revision r JOIN project_document d ON d.id = r.document_id WHERE r.id = ? AND d.project_id = ?")
            .bind(&id).bind(&project_id).fetch_optional(&mut *conn).await?.ok_or(DbError::NotFound)?;
        documents.push(json!({"id": id, "content_digest": r.try_get::<String,_>("content_digest")?,
            "content": serde_json::from_str::<Value>(&r.try_get::<String,_>("content_json")?).map_err(json_error)?}));
    }
    let config: Value = serde_json::from_str(
        row.try_get::<Option<&str>, _>("task_state_config")?
            .unwrap_or("{}"),
    )
    .map_err(json_error)?;
    let reviewer = sqlx::query("SELECT assignee_type, assignee_id FROM task_role_assignment WHERE task_id = ? AND role_name = 'reviewer'")
        .bind(task_id).fetch_optional(&mut *conn).await?;
    let reviewer = reviewer.map(|r| Ok::<_, DbError>(json!({"type": r.try_get::<Option<String>,_>("assignee_type")?, "id": r.try_get::<Option<String>,_>("assignee_id")?}))).transpose()?;
    let worklog = sqlx::query(
        "SELECT id, author_type, author_id, author_name, content, execution_id, role,
                worklog_kind, created_at
         FROM task_comment
         WHERE task_id = ? AND worklog_kind IS NOT NULL
         ORDER BY created_at ASC, id ASC
         LIMIT 100",
    )
    .bind(task_id)
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|entry| {
        Ok::<_, DbError>(json!({
            "id": entry.try_get::<String, _>("id")?,
            "author_type": entry.try_get::<String, _>("author_type")?,
            "author_id": entry.try_get::<Option<String>, _>("author_id")?,
            "author_name": entry.try_get::<String, _>("author_name")?,
            "content": entry.try_get::<String, _>("content")?,
            "execution_id": entry.try_get::<Option<String>, _>("execution_id")?,
            "role": entry.try_get::<Option<String>, _>("role")?,
            "kind": entry.try_get::<String, _>("worklog_kind")?,
            "created_at": entry.try_get::<String, _>("created_at")?,
        }))
    })
    .collect::<Result<Vec<_>>>()?;
    let media = sqlx::query(
        "SELECT id, asset_id, display_filename, content_type, byte_size, author_type,
                author_id, author_name, created_at
         FROM task_media
         WHERE task_id = ? AND deleted_at IS NULL
         ORDER BY created_at ASC, id ASC
         LIMIT 100",
    )
    .bind(task_id)
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|entry| {
        Ok::<_, DbError>(json!({
            "id": entry.try_get::<String, _>("id")?,
            "asset_id": entry.try_get::<Option<String>, _>("asset_id")?,
            "filename": entry.try_get::<String, _>("display_filename")?,
            "content_type": entry.try_get::<String, _>("content_type")?,
            "byte_size": entry.try_get::<i64, _>("byte_size")?,
            "author_type": entry.try_get::<String, _>("author_type")?,
            "author_id": entry.try_get::<Option<String>, _>("author_id")?,
            "author_name": entry.try_get::<String, _>("author_name")?,
            "created_at": entry.try_get::<String, _>("created_at")?,
        }))
    })
    .collect::<Result<Vec<_>>>()?;
    let mut allocations = serde_json::Map::new();
    if let Some(entries) = config
        .pointer("/review/requirement_allocations")
        .and_then(Value::as_object)
    {
        for (requirement, value) in entries {
            let id = value
                .as_str()
                .ok_or_else(|| DbError::Check("requirement allocation must name a Task".into()))?;
            let allocated: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                    SELECT 1 FROM task
                    WHERE id = ? AND project_id = ? AND deleted_at IS NULL
                 )",
            )
            .bind(id)
            .bind(&project_id)
            .fetch_one(&mut *conn)
            .await?;
            if !allocated {
                return Err(DbError::NotFound);
            }
            allocations.insert(requirement.clone(), json!({"task_id": id}));
        }
    }
    Ok(
        json!({"project_id": project_id, "task_id": task_id, "repo_id": row.try_get::<Option<String>,_>("repo_id")?,
        "charter_status": row.try_get::<String,_>("charter_status")?, "charter_setup_required": row.try_get::<i64,_>("charter_setup_required")?,
        "task_charter_revision_id": row.try_get::<Option<String>,_>("task_charter_revision_id")?, "charter": charter,
        "task_scope": {"title": row.try_get::<String,_>("title")?, "description": row.try_get::<Option<String>,_>("description")?, "task_type": row.try_get::<String,_>("task_type")?, "plan": row.try_get::<Option<String>,_>("plan")?, "merge_config": row.try_get::<Option<String>,_>("merge_config")?, "default_branch": row.try_get::<Option<String>,_>("default_branch")?, "config": config,
            "plan_item_id": row.try_get::<Option<String>,_>("plan_item_id")?, "milestone_id": row.try_get::<Option<String>,_>("milestone_id")?, "capability_class": row.try_get::<Option<String>,_>("capability_class")?, "allocations": allocations,
            "evidence": {"worklog": worklog, "media": media}},
        "reviewer_assignment": reviewer, "documents": documents, "workflow": serde_json::from_str::<Value>(&row.try_get::<String,_>("workflow_definition")?).map_err(json_error)?,
        "project_settings": serde_json::from_str::<Value>(&row.try_get::<String,_>("settings")?).map_err(json_error)?}),
    )
}

pub(crate) async fn verify_review_source(
    conn: &mut SqliteConnection,
    contract: &ReviewContract,
) -> Result<()> {
    let source = review_source_in_tx(conn, &contract.context.task_id).await?;
    if api_types::canonical_digest(&source).map_err(json_error)? != contract.context.source_digest {
        return Err(DbError::Check(
            "review governing context changed; fresh review required".into(),
        ));
    }
    Ok(())
}

#[async_trait]
impl ReviewConformanceRepo for SqliteDb {
    async fn lock_review_integration(&self, task_id: &str) -> Result<ReviewIntegrationGuard> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let source = review_source_in_tx(&mut tx, task_id).await?;
        let assigned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_role_assignment WHERE task_id = ? AND role_name = 'reviewer' AND assignee_type = 'agent' AND assignee_id IS NOT NULL)")
            .bind(task_id).fetch_one(&mut *tx).await?;
        let has_review_role = source
            .pointer("/workflow/states")
            .and_then(Value::as_array)
            .map(|states| states.iter().any(|s| s["role"] == "reviewer"))
            .unwrap_or(true);
        let contract = if assigned && has_review_role {
            let raw: Option<String> = sqlx::query_scalar("SELECT CASE WHEN status = 'passed' THEN step_results_json ELSE '{}' END FROM review WHERE task_id = ? ORDER BY attempt_number DESC, id DESC LIMIT 1")
                .bind(task_id).fetch_optional(&mut *tx).await?;
            let details: Value =
                serde_json::from_str(raw.as_deref().unwrap_or("{}")).map_err(json_error)?;
            let result: ReviewConformance = serde_json::from_value(details.get("conformance").cloned().unwrap_or_else(|| json!({"status":"not_assessed","contract":null,"assessment":null,"checks":[],"reason":null}))).map_err(json_error)?;
            if result.status != api_types::ConformanceStatus::Passed {
                return Err(DbError::Check(
                    "fresh conformance review required before integration".into(),
                ));
            }
            let contract = result
                .contract
                .ok_or_else(|| DbError::Check("review acceptance has no contract".into()))?;
            if contract.policy != api_types::REVIEW_CONFORMANCE_POLICY {
                return Err(DbError::Check(
                    "review acceptance uses an obsolete policy; fresh review required".into(),
                ));
            }
            if contract.context.task_id != task_id
                || api_types::canonical_digest(&source).map_err(json_error)?
                    != contract.context.source_digest
            {
                return Err(DbError::Check(
                    "review acceptance is stale; fresh review required".into(),
                ));
            }
            let persisted: Option<String> = sqlx::query_scalar(
                "SELECT conformance_json FROM execution_review_assessment WHERE execution_id = ?",
            )
            .bind(&contract.execution_id)
            .fetch_optional(&mut *tx)
            .await?;
            let persisted: ReviewConformance =
                serde_json::from_str(persisted.as_deref().ok_or_else(|| {
                    DbError::Check("review acceptance has no immutable evidence".into())
                })?)
                .map_err(json_error)?;
            if persisted.status != api_types::ConformanceStatus::Passed
                || persisted.contract.as_ref() != Some(&contract)
            {
                return Err(DbError::Check(
                    "review acceptance does not match immutable evidence".into(),
                ));
            }
            Some(contract)
        } else {
            None
        };
        Ok(ReviewIntegrationGuard {
            contract,
            transaction: tx,
        })
    }

    async fn review_source(&self, task_id: &str) -> Result<Value> {
        let mut tx = self.pool().begin().await?;
        let source = review_source_in_tx(&mut tx, task_id).await?;
        tx.commit().await?;
        Ok(source)
    }
    async fn create_review_contract(&self, contract: &ReviewContract) -> Result<()> {
        if contract.policy != api_types::REVIEW_CONFORMANCE_POLICY {
            return Err(DbError::Check(
                "review contract uses an unsupported policy".into(),
            ));
        }
        let mut tx = crate::begin_immediate(self.pool()).await?;
        verify_review_source(&mut tx, contract).await?;
        let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution WHERE id = ? AND task_id = ? AND status = 'running')")
            .bind(&contract.execution_id).bind(&contract.context.task_id).fetch_one(&mut *tx).await?;
        if !valid {
            return Err(DbError::Check(
                "review contract requires its running Task execution".into(),
            ));
        }
        let serialized = serde_json::to_string(contract).map_err(json_error)?;
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT contract_json FROM execution_review_contract WHERE execution_id = ?",
        )
        .bind(&contract.execution_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(existing) = existing {
            if existing != serialized {
                return Err(DbError::Check(
                    "review execution already has a different contract".into(),
                ));
            }
        } else {
            sqlx::query("INSERT INTO execution_review_contract VALUES (?, ?, ?, ?, ?, ?)")
                .bind(&contract.execution_id)
                .bind(&contract.context.task_id)
                .bind(&contract.digest)
                .bind(&contract.context.source_digest)
                .bind(serialized)
                .bind(crate::now_rfc3339())
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn review_contract(&self, execution_id: &str) -> Result<Option<ReviewContract>> {
        let value: Option<String> = sqlx::query_scalar(
            "SELECT contract_json FROM execution_review_contract WHERE execution_id = ?",
        )
        .bind(execution_id)
        .fetch_optional(self.pool())
        .await?;
        value
            .map(|s| serde_json::from_str(&s).map_err(json_error))
            .transpose()
    }
    async fn record_review_conformance(&self, result: &ReviewConformance) -> Result<()> {
        let contract = result
            .contract
            .as_ref()
            .ok_or_else(|| DbError::Check("assessment requires a contract".into()))?;
        let mut tx = crate::begin_immediate(self.pool()).await?;
        if result.status == api_types::ConformanceStatus::Passed {
            verify_review_source(&mut tx, contract).await?;
        }
        let frozen: String = sqlx::query_scalar("SELECT contract_json FROM execution_review_contract WHERE execution_id = ? AND contract_digest = ?")
            .bind(&contract.execution_id).bind(&contract.digest).fetch_optional(&mut *tx).await?.ok_or(DbError::NotFound)?;
        if serde_json::from_str::<ReviewContract>(&frozen).map_err(json_error)? != *contract {
            return Err(DbError::Check(
                "assessment contract differs from admission".into(),
            ));
        }
        let value = serde_json::to_string(result).map_err(json_error)?;
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT conformance_json FROM execution_review_assessment WHERE execution_id = ?",
        )
        .bind(&contract.execution_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(existing) = existing {
            if existing != value {
                return Err(DbError::Check("assessment is already frozen".into()));
            }
        } else {
            sqlx::query("INSERT INTO execution_review_assessment VALUES (?, ?, ?)")
                .bind(&contract.execution_id)
                .bind(value)
                .bind(crate::now_rfc3339())
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    async fn review_conformance(&self, execution_id: &str) -> Result<Option<ReviewConformance>> {
        let value: Option<String> = sqlx::query_scalar(
            "SELECT conformance_json FROM execution_review_assessment WHERE execution_id = ?",
        )
        .bind(execution_id)
        .fetch_optional(self.pool())
        .await?;
        value
            .map(|s| serde_json::from_str(&s).map_err(json_error))
            .transpose()
    }
}
