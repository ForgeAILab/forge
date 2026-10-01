//! Immutable review provenance and canonical source reads.
use crate::{DbError, Result, SqliteDb};
use api_types::{ReviewConformance, ReviewContract};
use async_trait::async_trait;
use serde_json::{json, Value};
use sqlx::{Row, SqliteConnection};

#[async_trait]
pub trait ReviewConformanceRepo: Send + Sync {
    async fn review_source(&self, task_id: &str, execution_id: Option<&str>) -> Result<Value>;
    async fn lock_review_integration(&self, task_id: &str) -> Result<ReviewIntegrationGuard>;
    async fn create_review_contract(&self, contract: &ReviewContract) -> Result<()>;
    async fn review_contract(&self, execution_id: &str) -> Result<Option<ReviewContract>>;
    async fn record_review_conformance(&self, result: &ReviewConformance) -> Result<()>;
    async fn review_conformance(&self, execution_id: &str) -> Result<Option<ReviewConformance>>;
    /// The still-valid passed review whose authority the Review row that was
    /// just opened for `task_id` may carry forward. `DbError::Check` names why
    /// it may not.
    async fn review_carry_base(&self, task_id: &str) -> Result<ReviewCarryBase>;
}

/// Integration candidate carried under a passed review's authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewCarryKind {
    CleanRebase,
    ConflictRepair,
}

impl std::fmt::Display for ReviewCarryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::CleanRebase => "clean_rebase",
            Self::ConflictRepair => "conflict_repair",
        })
    }
}

impl std::str::FromStr for ReviewCarryKind {
    type Err = DbError;
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "clean_rebase" => Ok(Self::CleanRebase),
            "conflict_repair" => Ok(Self::ConflictRepair),
            other => Err(DbError::Check(format!(
                "unknown review carry kind: {other}"
            ))),
        }
    }
}

/// A mechanical integration step recorded against a passed review's contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewReviewAuthorityCarry {
    pub task_id: String,
    pub contract_execution_id: String,
    pub commit_sha: String,
    pub base_sha: String,
    pub kind: ReviewCarryKind,
    pub changed_paths: Vec<String>,
}

/// The commit and target tip a merge must find, whether they come from the
/// reviewed contract itself or from a later carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewCandidate {
    pub commit_sha: String,
    pub base_sha: String,
}

/// What a carry may build on: the previous passed review's contract and
/// details, and how many carries already ride on that contract.
#[derive(Debug, Clone)]
pub struct ReviewCarryBase {
    /// The Running Review row opened for the current entry.
    pub review_id: String,
    pub contract: ReviewContract,
    /// The previous passed Review's details (`conformance`, `auditor`, ...).
    pub prior_details: Value,
    pub carries_since_review: i64,
}

/// Holds the authority write lock only across the local, bounded integration step.
/// No provider or test command may run while this guard is held.
pub struct ReviewIntegrationGuard {
    pub contract: Option<ReviewContract>,
    /// The commit and target tip integration must match. Equal to the
    /// contract's own `commit_sha`/`base_sha` unless a carry recorded against
    /// this contract superseded them.
    pub candidate: Option<ReviewCandidate>,
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
    execution_id: Option<&str>,
) -> Result<Value> {
    let row = sqlx::query(
        "SELECT t.id, t.project_id, r.id AS repo_id, e.id AS repository_execution_id,
                t.title, t.description, t.task_type,
                t.plan, t.task_state_config, t.merge_config, r.default_branch, p.workflow_definition, p.settings,
                p.charter_status, p.charter_setup_required, p.current_charter_revision_id,
                g.charter_revision_id AS task_charter_revision_id, g.document_revisions_json,
                g.plan_item_id, g.milestone_id, g.capability_class
         FROM task t JOIN project p ON p.id = t.project_id
         LEFT JOIN execution e ON e.id = COALESCE(
             ?,
             (SELECT e2.id FROM execution e2
              JOIN task candidate_task ON candidate_task.id = e2.task_id
              WHERE e2.status IN ('completed', 'running')
                AND (
                  (
                      t.parent_task_id IS NULL
                      AND EXISTS (
                          SELECT 1 FROM task direct_child
                          WHERE direct_child.parent_task_id = t.id
                            AND direct_child.deleted_at IS NULL
                      )
                      AND candidate_task.parent_task_id = t.id
                  )
                  OR (
                      (
                          t.parent_task_id IS NOT NULL
                          OR NOT EXISTS (
                              SELECT 1 FROM task direct_child
                              WHERE direct_child.parent_task_id = t.id
                                AND direct_child.deleted_at IS NULL
                          )
                      )
                      AND e2.task_id = t.id
                  )
              )
              ORDER BY e2.created_at DESC, e2.id DESC LIMIT 1)
         ) AND (
             e.task_id = t.id
             OR EXISTS (
                 SELECT 1 FROM task candidate_task
                 WHERE candidate_task.id = e.task_id
                   AND candidate_task.parent_task_id = t.id
                   AND candidate_task.deleted_at IS NULL
             )
         )
         LEFT JOIN workspace w ON w.id = e.workspace_id
         LEFT JOIN repo r ON r.id = CASE
             WHEN e.id IS NULL THEN p.primary_repo_id
             ELSE w.repo_id
         END AND r.project_id = t.project_id
         LEFT JOIN project_task_governance g ON g.task_id = t.id AND g.project_id = t.project_id
         WHERE t.id = ? AND t.deleted_at IS NULL")
        .bind(execution_id)
        .bind(task_id)
        .fetch_optional(&mut *conn)
        .await?
        .ok_or(DbError::NotFound)?;
    let resolved_execution_id: Option<String> = row.try_get("repository_execution_id")?;
    if execution_id.is_some_and(|expected| resolved_execution_id.as_deref() != Some(expected)) {
        return Err(DbError::NotFound);
    }
    if resolved_execution_id.is_some() && row.try_get::<Option<String>, _>("repo_id")?.is_none() {
        return Err(DbError::Check(
            "review execution has no Workspace repository provenance".to_owned(),
        ));
    }
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
    // The reviewed evidence is what existed when the reviewing run started.
    // Anything recorded after that is the reviewer's own report; counting it
    // would change the frozen context under the reviewer and void every
    // verdict that reports what it checked.
    let evidence_cutoff: Option<String> = match execution_id {
        Some(id) => {
            sqlx::query_scalar(
                "SELECT created_at FROM execution
             WHERE id = ? AND role IN ('reviewer', 'auditor')",
            )
            .bind(id)
            .fetch_optional(&mut *conn)
            .await?
        }
        None => None,
    };
    let worklog = sqlx::query(
        "SELECT id, author_type, author_id, author_name, content, execution_id, role,
                worklog_kind, created_at
         FROM task_comment
         WHERE task_id = ? AND worklog_kind IS NOT NULL
           AND (? IS NULL OR created_at < ?)
         ORDER BY created_at ASC, id ASC
         LIMIT 100",
    )
    .bind(task_id)
    .bind(evidence_cutoff.as_deref())
    .bind(evidence_cutoff.as_deref())
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
           AND (? IS NULL OR created_at < ?)
         ORDER BY created_at ASC, id ASC
         LIMIT 100",
    )
    .bind(task_id)
    .bind(evidence_cutoff.as_deref())
    .bind(evidence_cutoff.as_deref())
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
    let source = review_source_in_tx(
        conn,
        &contract.context.task_id,
        Some(&contract.execution_id),
    )
    .await?;
    if api_types::review_source_digest(&source, contract.context.source_digest_version.unwrap_or(1))
        .map_err(DbError::Check)?
        != contract.context.source_digest
    {
        return Err(DbError::Check(
            "review governing context changed; fresh review required".into(),
        ));
    }
    Ok(())
}

/// Every check that makes a passed review still current authority for its
/// Task, shared by integration and by authority carry: the Review recorded a
/// passed conformance under the current policy, the governing context still
/// hashes to the contract's digest, and the frozen assessment matches.
async fn verified_passed_contract(
    conn: &mut SqliteConnection,
    task_id: &str,
    raw: Option<&str>,
) -> Result<ReviewContract> {
    let details: Value = serde_json::from_str(raw.unwrap_or("{}")).map_err(json_error)?;
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
    let contract_source = review_source_in_tx(conn, task_id, Some(&contract.execution_id)).await?;
    if contract.context.task_id != task_id
        || api_types::review_source_digest(
            &contract_source,
            contract.context.source_digest_version.unwrap_or(1),
        )
        .map_err(DbError::Check)?
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
    .fetch_optional(&mut *conn)
    .await?;
    let persisted: ReviewConformance = serde_json::from_str(
        persisted
            .as_deref()
            .ok_or_else(|| DbError::Check("review acceptance has no immutable evidence".into()))?,
    )
    .map_err(json_error)?;
    if persisted.status != api_types::ConformanceStatus::Passed
        || persisted.contract.as_ref() != Some(&contract)
    {
        return Err(DbError::Check(
            "review acceptance does not match immutable evidence".into(),
        ));
    }
    Ok(contract)
}

#[async_trait]
impl ReviewConformanceRepo for SqliteDb {
    async fn lock_review_integration(&self, task_id: &str) -> Result<ReviewIntegrationGuard> {
        let mut tx = crate::begin_immediate(self.pool()).await?;
        let paused_project_id: Option<String> = sqlx::query_scalar(
            "SELECT p.id
             FROM task t
             JOIN project p ON p.id = t.project_id
             WHERE t.id = ? AND t.deleted_at IS NULL AND p.paused_at IS NOT NULL",
        )
        .bind(task_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(project_id) = paused_project_id {
            return Err(DbError::ProjectPaused { project_id });
        }
        let source = review_source_in_tx(&mut tx, task_id, None).await?;
        let assigned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_role_assignment WHERE task_id = ? AND role_name = 'reviewer' AND assignee_type = 'agent' AND assignee_id IS NOT NULL)")
            .bind(task_id).fetch_one(&mut *tx).await?;
        let has_review_role = source
            .pointer("/workflow/states")
            .and_then(Value::as_array)
            .map(|states| states.iter().any(|s| s["role"] == "reviewer"))
            .unwrap_or(true);
        let contract = if assigned && has_review_role {
            let review_passed_at: Option<String> = sqlx::query_scalar(
                "SELECT review_passed_at
                 FROM task
                 WHERE id = ? AND deleted_at IS NULL",
            )
            .bind(task_id)
            .fetch_one(&mut *tx)
            .await?;
            if review_passed_at
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(DbError::Check(
                    "current review authority required before integration".into(),
                ));
            }
            let raw: Option<String> = sqlx::query_scalar("SELECT CASE WHEN status = 'passed' THEN step_results_json ELSE '{}' END FROM review WHERE task_id = ? ORDER BY attempt_number DESC, id DESC LIMIT 1")
                .bind(task_id).fetch_optional(&mut *tx).await?;
            let contract = verified_passed_contract(&mut tx, task_id, raw.as_deref()).await?;
            Some(contract)
        } else {
            None
        };
        let candidate = match &contract {
            Some(contract) => {
                let carried = sqlx::query(
                    "SELECT commit_sha, base_sha FROM review_authority_carry
                     WHERE task_id = ? AND contract_execution_id = ?
                     ORDER BY created_at DESC, rowid DESC LIMIT 1",
                )
                .bind(task_id)
                .bind(&contract.execution_id)
                .fetch_optional(&mut *tx)
                .await?;
                Some(match carried {
                    Some(row) => ReviewCandidate {
                        commit_sha: row.try_get("commit_sha")?,
                        base_sha: row.try_get("base_sha")?,
                    },
                    None => ReviewCandidate {
                        commit_sha: contract.commit_sha.clone(),
                        base_sha: contract.base_sha.clone(),
                    },
                })
            }
            None => None,
        };
        Ok(ReviewIntegrationGuard {
            contract,
            candidate,
            transaction: tx,
        })
    }

    async fn review_source(&self, task_id: &str, execution_id: Option<&str>) -> Result<Value> {
        let mut tx = self.pool().begin().await?;
        let source = review_source_in_tx(&mut tx, task_id, execution_id).await?;
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

    async fn review_carry_base(&self, task_id: &str) -> Result<ReviewCarryBase> {
        let mut tx = self.pool().begin().await?;
        let rows = sqlx::query(
            "SELECT id, status, step_results_json FROM review WHERE task_id = ?
             ORDER BY attempt_number DESC, created_at DESC, id DESC LIMIT 2",
        )
        .bind(task_id)
        .fetch_all(&mut *tx)
        .await?;
        let [current, previous] = rows.as_slice() else {
            return Err(DbError::Check(
                "no previous review whose authority could be carried".into(),
            ));
        };
        if current.try_get::<String, _>("status")? != "running" {
            return Err(DbError::Check(
                "the current review attempt is not open for a carry".into(),
            ));
        }
        // Only the review immediately before this entry counts. A Failed or
        // cancelled attempt in between means something other than the
        // mechanical step happened since the reviewer last looked.
        if previous.try_get::<String, _>("status")? != "passed" {
            return Err(DbError::Check(
                "the previous review attempt did not pass".into(),
            ));
        }
        let raw: String = previous.try_get("step_results_json")?;
        let contract = verified_passed_contract(&mut tx, task_id, Some(&raw)).await?;
        let prior_details: Value = serde_json::from_str(&raw).map_err(json_error)?;
        let carries_since_review: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM review_authority_carry
             WHERE task_id = ? AND contract_execution_id = ?",
        )
        .bind(task_id)
        .bind(&contract.execution_id)
        .fetch_one(&mut *tx)
        .await?;
        let review_id: String = current.try_get("id")?;
        tx.commit().await?;
        Ok(ReviewCarryBase {
            review_id,
            contract,
            prior_details,
            carries_since_review,
        })
    }
}
