use super::*;
fn map_charter(row: SqliteRow) -> Result<ProjectCharterRecord> {
    Ok(ProjectCharterRecord {
        id: required_string(&row, "id")?,
        account_id: required_string(&row, "account_id")?,
        genesis_session_id: optional_string(&row, "genesis_session_id")?,
        project_id: optional_string(&row, "project_id")?,
        current_draft_revision_id: optional_string(&row, "current_draft_revision_id")?,
        current_approved_revision_id: optional_string(&row, "current_approved_revision_id")?,
        project_mode: required_string(&row, "project_mode")?,
        maturity: required_string(&row, "maturity")?,
        lifecycle: required_string(&row, "lifecycle")?,
        version: row.try_get("version")?,
        created_at: required_string(&row, "created_at")?,
        updated_at: required_string(&row, "updated_at")?,
    })
}

fn map_charter_revision(row: SqliteRow) -> Result<ProjectCharterRevisionRecord> {
    Ok(ProjectCharterRevisionRecord {
        id: required_string(&row, "id")?,
        charter_id: required_string(&row, "charter_id")?,
        revision: row.try_get("revision")?,
        base_revision: row.try_get("base_revision")?,
        base_revision_id: optional_string(&row, "base_revision_id")?,
        lifecycle: required_string(&row, "lifecycle")?,
        schema_version: required_string(&row, "schema_version")?,
        render_version: required_string(&row, "render_version")?,
        content_json: required_string(&row, "content_json")?,
        rendered_view: required_string(&row, "rendered_view")?,
        change_summary: required_string(&row, "change_summary")?,
        author_type: required_string(&row, "author_type")?,
        author_id: optional_string(&row, "author_id")?,
        source_message_id: optional_string(&row, "source_message_id")?,
        source_turn_job_id: optional_string(&row, "source_turn_job_id")?,
        source_refs_json: required_string(&row, "source_refs_json")?,
        content_digest: required_string(&row, "content_digest")?,
        rendered_digest: required_string(&row, "rendered_digest")?,
        created_at: required_string(&row, "created_at")?,
    })
}

impl SqliteDb {
    pub async fn charter_command_revision_version(&self, charter_id: &str) -> crate::Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT version FROM project_charter WHERE id = ? LIMIT 1")
                .bind(charter_id)
                .fetch_one(self.pool())
                .await?,
        )
    }
}

pub struct MainCharterReadSourceRow {
    row: SqliteRow,
}
impl MainCharterReadSourceRow {
    pub fn id(&self) -> crate::Result<String> {
        Ok(self.row.try_get("id")?)
    }
    pub fn genesis_session_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("genesis_session_id")?)
    }
    pub fn current_draft_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("current_draft_revision_id")?)
    }
    pub fn current_approved_revision_id(&self) -> crate::Result<Option<String>> {
        Ok(self.row.try_get("current_approved_revision_id")?)
    }
    pub fn project_mode(&self) -> crate::Result<String> {
        Ok(self.row.try_get("project_mode")?)
    }
    pub fn maturity(&self) -> crate::Result<String> {
        Ok(self.row.try_get("maturity")?)
    }
    pub fn lifecycle(&self) -> crate::Result<String> {
        Ok(self.row.try_get("lifecycle")?)
    }
    pub fn version(&self) -> crate::Result<i64> {
        Ok(self.row.try_get("version")?)
    }
    pub fn updated_at(&self) -> crate::Result<String> {
        Ok(self.row.try_get("updated_at")?)
    }
}

impl SqliteDb {
    pub async fn main_unowned_charter_read_sources(
        &self,
        account_id: String,
        charter_id: &Option<String>,
        genesis_session_id: &Option<String>,
        revision_id: &Option<String>,
        limit: i64,
    ) -> crate::Result<Vec<MainCharterReadSourceRow>> {
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
        .bind(charter_id)
        .bind(charter_id)
        .bind(genesis_session_id)
        .bind(genesis_session_id)
        .bind(revision_id)
        .bind(revision_id)
        .bind(revision_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| MainCharterReadSourceRow { row })
            .collect())
    }
}
pub(super) async fn get_project_charter(
    db: &SqliteDb,
    id: &str,
) -> Result<Option<ProjectCharterRecord>> {
    select_one(
        "SELECT * FROM project_charter WHERE id = ?",
        db.pool(),
        id,
        map_charter,
    )
    .await
}
pub(super) async fn get_project_charter_by_project_id(
    db: &SqliteDb,
    project_id: &str,
) -> Result<Option<ProjectCharterRecord>> {
    select_one(
        "SELECT * FROM project_charter
             WHERE project_id = ?",
        db.pool(),
        project_id,
        map_charter,
    )
    .await
}
pub(super) async fn get_project_charter_for_account(
    db: &SqliteDb,
    id: &str,
    account_id: &str,
) -> Result<Option<ProjectCharterRecord>> {
    sqlx::query("SELECT * FROM project_charter WHERE id = ? AND account_id = ?")
        .bind(id)
        .bind(account_id)
        .fetch_optional(db.pool())
        .await?
        .map(map_charter)
        .transpose()
}
pub(super) async fn create_project_charter(
    db: &SqliteDb,
    input: CreateProjectCharter,
) -> Result<ProjectCharterRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;
    if let Some(genesis_session_id) = input.genesis_session_id.as_deref() {
        let genesis = sqlx::query(
            "SELECT account_id, lifecycle, version
                 FROM product_genesis_session WHERE id = ?",
        )
        .bind(genesis_session_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::NotFound)?;
        let genesis_account: String = genesis.try_get("account_id")?;
        let genesis_lifecycle: String = genesis.try_get("lifecycle")?;
        if genesis_account != input.account_id
            || !matches!(
                genesis_lifecycle.as_str(),
                "discovering" | "ready_for_project"
            )
        {
            return Err(DbError::VersionConflict);
        }
    }
    sqlx::query(
        "INSERT INTO project_charter (
                id, account_id, genesis_session_id, project_mode, maturity,
                lifecycle, version, created_at, updated_at
             ) VALUES (?, ?, ?, ?, ?, 'draft', 1, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.account_id)
    .bind(input.genesis_session_id.as_deref())
    .bind(&input.project_mode)
    .bind(&input.maturity)
    .bind(&input.created_at)
    .bind(&input.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(check_error)?;
    if let Some(genesis_session_id) = input.genesis_session_id.as_deref() {
        let updated = sqlx::query(
            "UPDATE product_genesis_session
                 SET charter_id = ?, charter_version = 1, version = version + 1, updated_at = ?
                 WHERE id = ? AND account_id = ?
                   AND lifecycle IN ('discovering', 'ready_for_project')",
        )
        .bind(&input.id)
        .bind(&input.updated_at)
        .bind(genesis_session_id)
        .bind(&input.account_id)
        .execute(&mut *tx)
        .await
        .map_err(check_error)?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
    }
    let row = sqlx::query("SELECT * FROM project_charter WHERE id = ?")
        .bind(&input.id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    map_charter(row)
}
pub(super) async fn get_project_charter_revision(
    db: &SqliteDb,
    id: &str,
) -> Result<Option<ProjectCharterRevisionRecord>> {
    select_one(
        "SELECT * FROM project_charter_revision WHERE id = ?",
        db.pool(),
        id,
        map_charter_revision,
    )
    .await
}
pub(super) async fn list_project_charter_revisions(
    db: &SqliteDb,
    charter_id: &str,
) -> Result<Vec<ProjectCharterRevisionRecord>> {
    sqlx::query(
        "SELECT * FROM project_charter_revision
             WHERE charter_id = ? ORDER BY revision ASC, id ASC",
    )
    .bind(charter_id)
    .fetch_all(db.pool())
    .await?
    .into_iter()
    .map(map_charter_revision)
    .collect()
}
pub(super) async fn create_project_charter_revision(
    db: &SqliteDb,
    input: CreateProjectCharterRevision,
) -> Result<ProjectCharterRevisionRecord> {
    let mut transaction = crate::begin_immediate(db.pool()).await?;
    if let Some(receipt) =
        resolve_command_replay(db, &mut transaction, input.command_receipt.as_ref()).await?
    {
        let authoritative_revision_id = command_outcome_string(&receipt, "revision_id")?;
        validate_command_outcome_identity(&receipt, &[("charter_id", input.charter_id.as_str())])?;
        validate_replay_action_bundle(&mut transaction, &receipt, input.action_execution.as_ref())
            .await?;
        let row = sqlx::query(
            "SELECT r.*, c.account_id, c.project_id
                 FROM project_charter_revision r
                 JOIN project_charter c ON c.id = r.charter_id
                 WHERE r.id = ? AND r.charter_id = ?",
        )
        .bind(&authoritative_revision_id)
        .bind(&input.charter_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        let project_id: Option<String> = row.try_get("project_id")?;
        let account_id: String = row.try_get("account_id")?;
        if let Some(project_id) = project_id {
            validate_command_scope(input.command_receipt.as_ref(), "project", &project_id)?;
        } else {
            validate_command_scope(input.command_receipt.as_ref(), "account", &account_id)?;
        }
        let record = map_charter_revision(row)?;
        transaction.commit().await?;
        return Ok(record);
    }
    let charter = sqlx::query(
        "SELECT account_id, version, current_draft_revision_id
             FROM project_charter WHERE id = ?",
    )
    .bind(&input.charter_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    let charter_account_id: String = charter.try_get("account_id")?;
    let charter_version: i64 = charter.try_get("version")?;
    let current_draft: Option<String> = charter.try_get("current_draft_revision_id")?;
    if charter_version != input.expected_charter_version {
        return Err(DbError::VersionConflict);
    }
    if input.base_revision > 0 {
        let Some(current_draft) = current_draft else {
            return Err(DbError::VersionConflict);
        };
        let Some(base_revision_id) = input.base_revision_id.as_deref() else {
            return Err(DbError::VersionConflict);
        };
        if current_draft != base_revision_id {
            return Err(DbError::VersionConflict);
        }
        let base_ok: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_charter_revision
                 WHERE id = ? AND charter_id = ? AND revision = ? LIMIT 1",
        )
        .bind(base_revision_id)
        .bind(&input.charter_id)
        .bind(input.base_revision)
        .fetch_optional(&mut *transaction)
        .await?;
        if base_ok.is_none() {
            return Err(DbError::VersionConflict);
        }
    } else if input.base_revision_id.is_some() || current_draft.is_some() {
        return Err(DbError::VersionConflict);
    }
    let revision: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_charter_revision WHERE charter_id = ?",
    )
    .bind(&input.charter_id)
    .fetch_one(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO project_charter_revision (
                id, charter_id, revision, base_revision, base_revision_id, lifecycle,
                schema_version, render_version, content_json, rendered_view,
                change_summary, author_type, author_id, source_message_id,
                source_turn_job_id, source_refs_json, content_digest,
                rendered_digest, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.charter_id)
    .bind(revision)
    .bind(input.base_revision)
    .bind(input.base_revision_id.as_deref())
    .bind(&input.lifecycle)
    .bind(&input.schema_version)
    .bind(&input.render_version)
    .bind(&input.content_json)
    .bind(&input.rendered_view)
    .bind(&input.change_summary)
    .bind(&input.author_type)
    .bind(input.author_id.as_deref())
    .bind(input.source_message_id.as_deref())
    .bind(input.source_turn_job_id.as_deref())
    .bind(&input.source_refs_json)
    .bind(&input.content_digest)
    .bind(&input.rendered_digest)
    .bind(&input.created_at)
    .execute(&mut *transaction)
    .await
    .map_err(check_error)?;
    let charter_update = sqlx::query(
        "UPDATE project_charter
             SET current_draft_revision_id = ?, project_mode = ?, maturity = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
    )
    .bind(&input.id)
    .bind(&input.project_mode)
    .bind(&input.maturity)
    .bind(&input.created_at)
    .bind(&input.charter_id)
    .bind(input.expected_charter_version)
    .execute(&mut *transaction)
    .await
    .map_err(check_error)?;
    if charter_update.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let genesis: Option<(String, String)> = sqlx::query_as(
        "SELECT id, account_id FROM product_genesis_session
             WHERE id = (SELECT genesis_session_id FROM project_charter WHERE id = ?)
               AND lifecycle IN ('discovering', 'ready_for_project')",
    )
    .bind(&input.charter_id)
    .fetch_optional(&mut *transaction)
    .await?;
    if let Some((genesis_id, genesis_account)) = genesis {
        let genesis_update = sqlx::query(
            "UPDATE product_genesis_session
                 SET charter_version = ?, version = version + 1, updated_at = ?
                 WHERE id = ? AND account_id = ?",
        )
        .bind(input.expected_charter_version + 1)
        .bind(&input.created_at)
        .bind(genesis_id)
        .bind(genesis_account)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        if genesis_update.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
    }
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project_charter.revision_created".to_owned(),
        entity_type: "project_charter_revision".to_owned(),
        entity_id: input.id.clone(),
        actor_type: input
            .command_receipt
            .as_ref()
            .map(|receipt| receipt.principal_type.clone())
            .unwrap_or_else(|| input.author_type.clone()),
        actor_id: input
            .command_receipt
            .as_ref()
            .map(|receipt| receipt.principal_id.clone())
            .or_else(|| input.author_id.clone()),
        scope_type: "account".to_owned(),
        scope_id: charter_account_id,
        correlation_id: input
            .command_receipt
            .as_ref()
            .map(|receipt| receipt.correlation_id.clone())
            .unwrap_or_else(|| input.id.clone()),
        causation_id: input
            .command_receipt
            .as_ref()
            .and_then(|receipt| receipt.causation_id.clone())
            .or_else(|| input.source_message_id.clone()),
        causation_depth: input
            .command_receipt
            .as_ref()
            .map_or(0, |receipt| receipt.causation_depth),
        dedupe_key: Some(format!("project-charter-revision-created:{}", input.id)),
        payload_json: serde_json::json!({
            "charter_id": input.charter_id.clone(),
            "revision_id": input.id.clone(),
            "revision": revision,
            "content_digest": input.content_digest.clone(),
            "rendered_digest": input.rendered_digest.clone(),
        })
        .to_string(),
        created_at: input.created_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut transaction, &event).await?;
    finalize_command_in_tx(
        db,
        &mut transaction,
        &event.id,
        input.command_receipt.clone(),
        input.action_execution.clone(),
    )
    .await?;
    transaction.commit().await?;
    db.get_project_charter_revision(&input.id)
        .await?
        .ok_or(DbError::NotFound)
}
pub(super) async fn create_project_charter_revision_atomically(
    db: &SqliteDb,
    input: CreateProjectCharterRevisionAtomically,
) -> Result<ProjectCharterRevisionRecord> {
    let mut transaction = crate::begin_immediate(db.pool()).await?;
    let command_receipt = input
        .command_receipt
        .as_ref()
        .or(input.revision.command_receipt.as_ref());
    let action_execution = input
        .action_execution
        .as_ref()
        .or(input.revision.action_execution.as_ref());
    if let Some(receipt) = resolve_command_replay(db, &mut transaction, command_receipt).await? {
        let authoritative_charter_id = command_outcome_string(&receipt, "charter_id")?;
        let authoritative_revision_id = command_outcome_string(&receipt, "revision_id")?;
        validate_replay_action_bundle(&mut transaction, &receipt, action_execution).await?;
        let row = sqlx::query(
            "SELECT r.*, c.account_id, c.project_id, c.genesis_session_id
                 FROM project_charter_revision r
                 JOIN project_charter c ON c.id = r.charter_id
                 WHERE r.id = ? AND r.charter_id = ?",
        )
        .bind(&authoritative_revision_id)
        .bind(&authoritative_charter_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        let project_id: Option<String> = row.try_get("project_id")?;
        let account_id: String = row.try_get("account_id")?;
        if account_id != input.account_id
            || project_id.as_deref() != input.project_id.as_deref()
            || row
                .try_get::<Option<String>, _>("genesis_session_id")?
                .as_deref()
                != input.genesis_session_id.as_deref()
        {
            return Err(DbError::VersionConflict);
        }
        if let Some(project_id) = project_id {
            validate_command_scope(command_receipt, "project", &project_id)?;
        } else {
            validate_command_scope(command_receipt, "account", &account_id)?;
        }
        let record = map_charter_revision(row)?;
        transaction.commit().await?;
        return Ok(record);
    }
    if input.charter.id != input.revision.charter_id
        || input.charter.account_id != input.account_id
        || input.charter.genesis_session_id != input.genesis_session_id
        || input.revision.expected_charter_version != 1
        || input.revision.base_revision != 0
        || input.revision.base_revision_id.is_some()
        || (input.project_id.is_none() && input.genesis_session_id.is_none())
        || (input.project_id.is_some() && input.genesis_session_id.is_some())
    {
        return Err(DbError::VersionConflict);
    }
    if let Some(project_id) = input.project_id.as_deref() {
        let project = sqlx::query("SELECT id, owner_id FROM project WHERE id = ?")
            .bind(project_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let owner_id: Option<String> = project.try_get("owner_id")?;
        let privileged_member: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project_member
                 WHERE project_id = ? AND user_id = ? AND role IN ('owner', 'admin')
                 LIMIT 1",
        )
        .bind(project_id)
        .bind(&input.account_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if owner_id.as_deref() != Some(input.account_id.as_str()) && privileged_member.is_none() {
            return Err(DbError::VersionConflict);
        }
    } else if let Some(genesis_session_id) = input.genesis_session_id.as_deref() {
        let genesis = sqlx::query(
            "SELECT account_id, lifecycle, charter_id
                 FROM product_genesis_session WHERE id = ?",
        )
        .bind(genesis_session_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let genesis_account_id: String = genesis.try_get("account_id")?;
        let genesis_lifecycle: String = genesis.try_get("lifecycle")?;
        let genesis_charter_id: Option<String> = genesis.try_get("charter_id")?;
        if genesis_account_id != input.account_id
            || !matches!(
                genesis_lifecycle.as_str(),
                "discovering" | "ready_for_project"
            )
            || genesis_charter_id.is_some_and(|id| id != input.charter.id)
        {
            return Err(DbError::VersionConflict);
        }
    }

    let existing = sqlx::query(
        "SELECT account_id, project_id, genesis_session_id, project_mode, maturity
             FROM project_charter WHERE id = ?",
    )
    .bind(&input.charter.id)
    .fetch_optional(&mut *transaction)
    .await?;

    if let Some(existing) = existing {
        let account_id: String = existing.try_get("account_id")?;
        let project_id: Option<String> = existing.try_get("project_id")?;
        let genesis_session_id: Option<String> = existing.try_get("genesis_session_id")?;
        let project_mode: String = existing.try_get("project_mode")?;
        let maturity: String = existing.try_get("maturity")?;
        if account_id != input.account_id
            || project_id
                .as_deref()
                .is_some_and(|existing| input.project_id.as_deref() != Some(existing))
            || genesis_session_id
                .as_deref()
                .is_some_and(|existing| input.genesis_session_id.as_deref() != Some(existing))
            || project_mode != input.revision.project_mode
            || maturity != input.revision.maturity
        {
            return Err(DbError::VersionConflict);
        }
        if input.project_id.is_some() && project_id.is_none() {
            let claimed = sqlx::query(
                "UPDATE project_charter SET project_id = ?, updated_at = ?
                     WHERE id = ? AND account_id = ? AND project_id IS NULL
                       AND genesis_session_id IS NULL",
            )
            .bind(input.project_id.as_deref())
            .bind(&input.revision.created_at)
            .bind(&input.charter.id)
            .bind(&input.account_id)
            .execute(&mut *transaction)
            .await
            .map_err(orchestration_write_error)?;
            if claimed.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        } else if input.genesis_session_id.is_some() && genesis_session_id.is_none() {
            let claimed = sqlx::query(
                "UPDATE project_charter SET genesis_session_id = ?, updated_at = ?
                     WHERE id = ? AND account_id = ? AND project_id IS NULL
                       AND genesis_session_id IS NULL",
            )
            .bind(input.genesis_session_id.as_deref())
            .bind(&input.revision.created_at)
            .bind(&input.charter.id)
            .bind(&input.account_id)
            .execute(&mut *transaction)
            .await
            .map_err(orchestration_write_error)?;
            if claimed.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        }
    } else {
        sqlx::query(
            "INSERT INTO project_charter (
                    id, account_id, genesis_session_id, project_id,
                    project_mode, maturity, lifecycle, version,
                    created_at, updated_at
                 ) VALUES (?, ?, ?, ?, ?, ?, 'draft', 1, ?, ?)",
        )
        .bind(&input.charter.id)
        .bind(&input.account_id)
        .bind(input.genesis_session_id.as_deref())
        .bind(&input.project_id)
        .bind(&input.revision.project_mode)
        .bind(&input.revision.maturity)
        .bind(&input.charter.created_at)
        .bind(&input.charter.updated_at)
        .execute(&mut *transaction)
        .await
        .map_err(orchestration_write_error)?;
        if let Some(genesis_session_id) = input.genesis_session_id.as_deref() {
            let linked = sqlx::query(
                "UPDATE product_genesis_session
                     SET charter_id = ?, charter_version = 1,
                         version = version + 1, updated_at = ?
                     WHERE id = ? AND account_id = ? AND charter_id IS NULL
                       AND lifecycle IN ('discovering', 'ready_for_project')",
            )
            .bind(&input.charter.id)
            .bind(&input.revision.created_at)
            .bind(genesis_session_id)
            .bind(&input.account_id)
            .execute(&mut *transaction)
            .await
            .map_err(orchestration_write_error)?;
            if linked.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        }
    }

    if let Some(genesis_session_id) = input.genesis_session_id.as_deref() {
        let linked_charter_id: Option<String> =
            sqlx::query_scalar("SELECT charter_id FROM product_genesis_session WHERE id = ?")
                .bind(genesis_session_id)
                .fetch_one(&mut *transaction)
                .await?;
        match linked_charter_id {
            Some(existing) if existing != input.charter.id => {
                return Err(DbError::VersionConflict);
            }
            Some(_) => {}
            None => {
                let linked = sqlx::query(
                    "UPDATE product_genesis_session
                         SET charter_id = ?, charter_version = 1,
                             version = version + 1, updated_at = ?
                         WHERE id = ? AND account_id = ? AND charter_id IS NULL
                           AND lifecycle IN ('discovering', 'ready_for_project')",
                )
                .bind(&input.charter.id)
                .bind(&input.revision.created_at)
                .bind(genesis_session_id)
                .bind(&input.account_id)
                .execute(&mut *transaction)
                .await
                .map_err(orchestration_write_error)?;
                if linked.rows_affected() != 1 {
                    return Err(DbError::VersionConflict);
                }
            }
        }
    }

    let ownership_column = if input.project_id.is_some() {
        "project_id"
    } else {
        "genesis_session_id"
    };
    let charter = sqlx::query(&format!(
        "SELECT account_id, version, current_draft_revision_id
             FROM project_charter WHERE id = ? AND {ownership_column} = ?"
    ))
    .bind(&input.revision.charter_id)
    .bind(
        input
            .project_id
            .as_deref()
            .or(input.genesis_session_id.as_deref()),
    )
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(DbError::VersionConflict)?;
    let charter_account_id: String = charter.try_get("account_id")?;
    let charter_version: i64 = charter.try_get("version")?;
    let current_draft: Option<String> = charter.try_get("current_draft_revision_id")?;
    if charter_account_id != input.account_id
        || charter_version != input.revision.expected_charter_version
        || current_draft.is_some()
    {
        return Err(DbError::VersionConflict);
    }

    let revision_number: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(revision), 0) + 1
             FROM project_charter_revision WHERE charter_id = ?",
    )
    .bind(&input.revision.charter_id)
    .fetch_one(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO project_charter_revision (
                id, charter_id, revision, base_revision, base_revision_id, lifecycle,
                schema_version, render_version, content_json, rendered_view,
                change_summary, author_type, author_id, source_message_id,
                source_turn_job_id, source_refs_json, content_digest,
                rendered_digest, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.revision.id)
    .bind(&input.revision.charter_id)
    .bind(revision_number)
    .bind(input.revision.base_revision)
    .bind(input.revision.base_revision_id.as_deref())
    .bind(&input.revision.lifecycle)
    .bind(&input.revision.schema_version)
    .bind(&input.revision.render_version)
    .bind(&input.revision.content_json)
    .bind(&input.revision.rendered_view)
    .bind(&input.revision.change_summary)
    .bind(&input.revision.author_type)
    .bind(input.revision.author_id.as_deref())
    .bind(input.revision.source_message_id.as_deref())
    .bind(input.revision.source_turn_job_id.as_deref())
    .bind(&input.revision.source_refs_json)
    .bind(&input.revision.content_digest)
    .bind(&input.revision.rendered_digest)
    .bind(&input.revision.created_at)
    .execute(&mut *transaction)
    .await
    .map_err(orchestration_write_error)?;
    let charter_update = if let Some(project_id) = input.project_id.as_deref() {
        sqlx::query(
            "UPDATE project_charter
                 SET current_draft_revision_id = ?, project_mode = ?, maturity = ?,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND project_id = ? AND version = ?
                   AND current_draft_revision_id IS NULL",
        )
        .bind(&input.revision.id)
        .bind(&input.revision.project_mode)
        .bind(&input.revision.maturity)
        .bind(&input.revision.created_at)
        .bind(&input.revision.charter_id)
        .bind(project_id)
        .bind(input.revision.expected_charter_version)
        .execute(&mut *transaction)
        .await
        .map_err(orchestration_write_error)?
    } else {
        let genesis_session_id = input
            .genesis_session_id
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        sqlx::query(
            "UPDATE project_charter
                 SET current_draft_revision_id = ?, project_mode = ?, maturity = ?,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND genesis_session_id = ? AND version = ?
                   AND current_draft_revision_id IS NULL",
        )
        .bind(&input.revision.id)
        .bind(&input.revision.project_mode)
        .bind(&input.revision.maturity)
        .bind(&input.revision.created_at)
        .bind(&input.revision.charter_id)
        .bind(genesis_session_id)
        .bind(input.revision.expected_charter_version)
        .execute(&mut *transaction)
        .await
        .map_err(orchestration_write_error)?
    };
    if charter_update.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }

    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project_charter.revision_created".to_owned(),
        entity_type: "project_charter_revision".to_owned(),
        entity_id: input.revision.id.clone(),
        actor_type: command_receipt
            .map(|receipt| receipt.principal_type.clone())
            .unwrap_or_else(|| input.revision.author_type.clone()),
        actor_id: command_receipt
            .map(|receipt| receipt.principal_id.clone())
            .or_else(|| input.revision.author_id.clone()),
        scope_type: "account".to_owned(),
        scope_id: charter_account_id,
        correlation_id: command_receipt
            .map(|receipt| receipt.correlation_id.clone())
            .unwrap_or_else(|| input.revision.id.clone()),
        causation_id: command_receipt
            .and_then(|receipt| receipt.causation_id.clone())
            .or_else(|| input.revision.source_message_id.clone()),
        causation_depth: command_receipt.map_or(0, |receipt| receipt.causation_depth),
        dedupe_key: Some(format!(
            "project-charter-revision-created:{}",
            input.revision.id
        )),
        payload_json: serde_json::json!({
            "charter_id": input.revision.charter_id.clone(),
            "revision_id": input.revision.id.clone(),
            "revision": revision_number,
            "content_digest": input.revision.content_digest.clone(),
            "rendered_digest": input.revision.rendered_digest.clone(),
        })
        .to_string(),
        created_at: input.revision.created_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut transaction, &event).await?;
    finalize_command_in_tx(
        db,
        &mut transaction,
        &event.id,
        command_receipt.cloned(),
        action_execution.cloned(),
    )
    .await?;
    let row = sqlx::query("SELECT * FROM project_charter_revision WHERE id = ?")
        .bind(&input.revision.id)
        .fetch_one(&mut *transaction)
        .await?;
    let record = map_charter_revision(row)?;
    transaction.commit().await?;
    Ok(record)
}
pub(super) async fn finalize_project_charter_revision_noop(
    db: &SqliteDb,
    input: FinalizeProjectCharterRevisionNoop,
) -> Result<ProjectCharterRevisionRecord> {
    let mut transaction = crate::begin_immediate(db.pool()).await?;
    if let Some(receipt) =
        resolve_command_replay(db, &mut transaction, Some(&input.command_receipt)).await?
    {
        let authoritative_charter_id = command_outcome_string(&receipt, "charter_id")?;
        let authoritative_revision_id = command_outcome_string(&receipt, "revision_id")?;
        if authoritative_charter_id != input.charter_id
            || authoritative_revision_id != input.revision_id
        {
            return Err(DbError::IdempotencyConflict);
        }
        validate_replay_action_bundle(&mut transaction, &receipt, input.action_execution.as_ref())
            .await?;
        let row = sqlx::query(
            "SELECT r.*, c.account_id, c.project_id
                 FROM project_charter_revision r
                 JOIN project_charter c ON c.id = r.charter_id
                 WHERE r.id = ? AND r.charter_id = ?",
        )
        .bind(&authoritative_revision_id)
        .bind(&authoritative_charter_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        let account_id: String = row.try_get("account_id")?;
        let project_id: Option<String> = row.try_get("project_id")?;
        if account_id != input.account_id
            || project_id.as_deref() != Some(input.project_id.as_str())
        {
            return Err(DbError::VersionConflict);
        }
        let content_digest: String = row.try_get("content_digest")?;
        let rendered_digest: String = row.try_get("rendered_digest")?;
        if content_digest != input.content_digest || rendered_digest != input.rendered_digest {
            return Err(DbError::IdempotencyConflict);
        }
        let record = map_charter_revision(row)?;
        transaction.commit().await?;
        return Ok(record);
    }

    let row = sqlx::query(
        "SELECT r.*, c.account_id, c.project_id, c.version AS charter_version,
                    c.current_draft_revision_id
             FROM project_charter_revision r
             JOIN project_charter c ON c.id = r.charter_id
             WHERE r.id = ? AND r.charter_id = ?",
    )
    .bind(&input.revision_id)
    .bind(&input.charter_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    let account_id: String = row.try_get("account_id")?;
    let project_id: Option<String> = row.try_get("project_id")?;
    let charter_version: i64 = row.try_get("charter_version")?;
    let current_draft_revision_id: Option<String> = row.try_get("current_draft_revision_id")?;
    let base_revision: i64 = row.try_get("base_revision")?;
    let base_revision_id: Option<String> = row.try_get("base_revision_id")?;
    let content_digest: String = row.try_get("content_digest")?;
    let rendered_digest: String = row.try_get("rendered_digest")?;
    if account_id != input.account_id
        || project_id.as_deref() != Some(input.project_id.as_str())
        || charter_version <= 1
        || current_draft_revision_id.as_deref() != Some(input.revision_id.as_str())
        || base_revision != 0
        || base_revision_id.is_some()
    {
        return Err(DbError::VersionConflict);
    }
    if content_digest != input.content_digest || rendered_digest != input.rendered_digest {
        return Err(DbError::IdempotencyConflict);
    }
    let outcome: serde_json::Value = serde_json::from_str(&input.command_receipt.outcome_json)
        .map_err(|_| DbError::IdempotencyConflict)?;
    for (key, expected) in [
        ("charter_id", input.charter_id.as_str()),
        ("revision_id", input.revision_id.as_str()),
    ] {
        if outcome.get(key).and_then(serde_json::Value::as_str) != Some(expected) {
            return Err(DbError::IdempotencyConflict);
        }
    }
    if outcome
        .get("charter_version")
        .and_then(serde_json::Value::as_i64)
        != Some(charter_version)
    {
        return Err(DbError::VersionConflict);
    }

    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project_charter.revision_noop".to_owned(),
        entity_type: "project_charter_revision".to_owned(),
        entity_id: input.revision_id.clone(),
        actor_type: input.command_receipt.principal_type.clone(),
        actor_id: Some(input.command_receipt.principal_id.clone()),
        scope_type: "account".to_owned(),
        scope_id: account_id,
        correlation_id: input.command_receipt.correlation_id.clone(),
        causation_id: input.command_receipt.causation_id.clone(),
        causation_depth: input.command_receipt.causation_depth,
        dedupe_key: Some(format!(
            "project-charter-revision-noop:{}",
            input.command_receipt.id
        )),
        payload_json: serde_json::json!({
            "charter_id": input.charter_id,
            "revision_id": input.revision_id,
            "charter_version": charter_version,
            "content_digest": input.content_digest,
            "rendered_digest": input.rendered_digest,
            "semantic_noop": true,
        })
        .to_string(),
        created_at: input.command_receipt.committed_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut transaction, &event).await?;
    finalize_command_in_tx(
        db,
        &mut transaction,
        &event.id,
        Some(input.command_receipt),
        input.action_execution,
    )
    .await?;
    let record = map_charter_revision(row)?;
    transaction.commit().await?;
    Ok(record)
}
pub(super) async fn get_project_charter_approval(
    db: &SqliteDb,
    id: &str,
) -> Result<Option<ProjectCharterApprovalRecord>> {
    select_one(
        "SELECT * FROM project_charter_approval WHERE id = ?",
        db.pool(),
        id,
        map_charter_approval,
    )
    .await
}
pub(super) async fn approve_project_charter(
    db: &SqliteDb,
    input: ApproveProjectCharter,
) -> Result<ProjectCharterApprovalRecord> {
    if !valid_authorization_timestamp(&input.authorization_occurred_at) {
        return Err(DbError::VersionConflict);
    }
    let mut transaction = crate::begin_immediate(db.pool()).await?;
    let charter_scope =
        sqlx::query("SELECT account_id, project_id FROM project_charter WHERE id = ?")
            .bind(&input.charter_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
    let account_id: String = charter_scope.try_get("account_id")?;
    let project_id: Option<String> = charter_scope.try_get("project_id")?;
    let scope_id = project_id.unwrap_or_else(|| format!("account:{account_id}"));
    let storage_idempotency_key = orchestration_scoped_idempotency_key(
        "charter-approval",
        &scope_id,
        &input.approving_principal_id,
        &input.idempotency_key,
    );

    if let Some(existing) =
        sqlx::query("SELECT * FROM project_charter_approval WHERE idempotency_key = ?")
            .bind(&storage_idempotency_key)
            .fetch_optional(&mut *transaction)
            .await?
            .map(map_charter_approval)
            .transpose()?
    {
        let same = existing.charter_id == input.charter_id
            && existing.revision_id == input.revision_id
            && existing.content_digest == input.content_digest
            && existing.rendered_digest == input.rendered_digest
            && existing.expected_charter_version == input.expected_charter_version
            && existing.approval_type == input.approval_type
            && existing.approved_name == input.approved_name
            && existing.approved_slug == input.approved_slug
            && existing.approved_project_mode == input.approved_project_mode
            && existing.selected_identity_id == input.selected_identity_id
            && existing.selected_profile_id == input.selected_profile_id
            && existing.selected_operating_skill_revision_id
                == input.selected_operating_skill_revision_id
            && existing.selected_policy_revision == input.selected_policy_revision
            && existing.selected_policy_digest == input.selected_policy_digest
            && existing.approving_principal_type == input.approving_principal_type
            && existing.approving_principal_id == input.approving_principal_id
            && existing.authorization_basis == input.authorization_basis
            && existing.authorization_action == input.authorization_action
            && existing.explicit_event == input.explicit_event
            && existing.authorization_occurred_at == input.authorization_occurred_at
            && existing.source_action == input.source_action;
        if !same {
            return Err(DbError::VersionConflict);
        }
        transaction.commit().await?;
        return Ok(existing);
    }

    let target = sqlx::query(
        "SELECT c.version AS charter_version, c.account_id, c.project_mode,
                    c.current_approved_revision_id AS previous_approved_revision_id,
                    r.charter_id, r.lifecycle, r.content_digest, r.rendered_digest
             FROM project_charter_revision r
             JOIN project_charter c ON c.id = r.charter_id
             WHERE r.id = ? AND r.charter_id = ?",
    )
    .bind(&input.revision_id)
    .bind(&input.charter_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(DbError::NotFound)?;
    let version: i64 = target.try_get("charter_version")?;
    let lifecycle: String = target.try_get("lifecycle")?;
    let content_digest: String = target.try_get("content_digest")?;
    let rendered_digest: String = target.try_get("rendered_digest")?;
    let project_mode: String = target.try_get("project_mode")?;
    if version != input.expected_charter_version
        || content_digest != input.content_digest
        || rendered_digest != input.rendered_digest
        || project_mode != input.approved_project_mode
        || !matches!(lifecycle.as_str(), "draft" | "proposed" | "approved")
    {
        return Err(DbError::VersionConflict);
    }

    if input.selected_identity_id.is_some() != input.selected_profile_id.is_some() {
        return Err(DbError::Check(
            "Project Agent identity and profile must be selected together".to_owned(),
        ));
    }
    if input.approval_type == "project_creation"
        && (input.selected_identity_id.is_none()
            || input.selected_profile_id.is_none()
            || input.selected_operating_skill_revision_id.is_none()
            || input.selected_policy_revision.is_none()
            || input.selected_policy_digest.is_none())
    {
        return Err(DbError::VersionConflict);
    }
    if let (Some(identity_id), Some(profile_id)) =
        (&input.selected_identity_id, &input.selected_profile_id)
    {
        let skill_revision_id = input
            .selected_operating_skill_revision_id
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        let selected = sqlx::query(
            "SELECT p.tool_policy_json, i.paused, i.archived_at,
                        i.selected_profile_id, sr.id AS skill_revision_id,
                        sr.skill_key, s.current_revision_id, s.lifecycle
                 FROM agent_profile p
                 JOIN agent_identity i ON i.id = p.identity_id
                 JOIN project_charter c ON c.account_id = i.owner_id
                 JOIN operating_skill_revision sr ON sr.id = ?
                 JOIN operating_skill s ON s.id = sr.operating_skill_id
                 WHERE p.id = ? AND p.identity_id = ? AND c.id = ?
                   AND i.selected_profile_id = p.id
                 LIMIT 1",
        )
        .bind(skill_revision_id)
        .bind(profile_id)
        .bind(identity_id)
        .bind(&input.charter_id)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(selected) = selected else {
            return Err(DbError::Check(
                "selected Project Agent profile is not owned by the Charter account".to_owned(),
            ));
        };
        let selected_paused: i64 = selected.try_get("paused")?;
        let selected_archived: Option<String> = selected.try_get("archived_at")?;
        let selected_profile_id: Option<String> = selected.try_get("selected_profile_id")?;
        let selected_skill_revision_id: String = selected.try_get("skill_revision_id")?;
        let selected_skill_key: String = selected.try_get("skill_key")?;
        let selected_skill_current_revision_id: Option<String> =
            selected.try_get("current_revision_id")?;
        let selected_skill_lifecycle: String = selected.try_get("lifecycle")?;
        let selected_tool_policy_json: String = selected.try_get("tool_policy_json")?;
        let selected_policy_digest = profile_policy_digest(&selected_tool_policy_json);
        if selected_paused != 0
            || selected_archived.is_some()
            || selected_profile_id.as_deref() != Some(profile_id.as_str())
            || selected_skill_revision_id != skill_revision_id
            || selected_skill_current_revision_id.as_deref() != Some(skill_revision_id)
            || selected_skill_lifecycle != "active"
            || selected_skill_key != PROJECT_OPERATING_SKILL_KEY
            || input.selected_policy_digest.as_deref() != Some(selected_policy_digest.as_str())
        {
            return Err(DbError::VersionConflict);
        }
    }

    let previous_active_approval: Option<String> = sqlx::query_scalar(
        "SELECT id FROM project_charter_approval
             WHERE charter_id = ? AND lifecycle = 'active' AND id != ? LIMIT 1",
    )
    .bind(&input.charter_id)
    .bind(&input.id)
    .fetch_optional(&mut *transaction)
    .await?;
    if let Some(previous_approval_id) = previous_active_approval {
        let revoked = sqlx::query(
            "UPDATE project_charter_approval SET lifecycle = 'revoked',
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND lifecycle = 'active'",
        )
        .bind(&input.updated_at)
        .bind(&previous_approval_id)
        .execute(&mut *transaction)
        .await
        .map_err(check_error)?;
        if revoked.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
                "INSERT INTO project_charter_approval_event (
                    id, approval_id, lifecycle, principal_type, principal_id,
                    authorization_basis, action, explicit_event, reason,
                    idempotency_key, occurred_at, created_at
                 ) VALUES (?, ?, 'revoked', ?, ?, ?, ?, ?, 'Superseded by newer approval', ?, ?, ?)",
            )
            .bind(new_uuid_v4())
            .bind(&previous_approval_id)
            .bind(&input.approving_principal_type)
            .bind(&input.approving_principal_id)
            .bind(&input.authorization_basis)
            .bind(&input.authorization_action)
            .bind(&input.explicit_event)
            .bind(format!(
                "{}:revoke:{}",
                storage_idempotency_key, previous_approval_id
            ))
            .bind(&input.authorization_occurred_at)
            .bind(&input.updated_at)
            .execute(&mut *transaction)
            .await
            .map_err(check_error)?;
    }
    let previous_approved_revision: Option<String> =
        target.try_get("previous_approved_revision_id")?;
    if let Some(previous_revision_id) = previous_approved_revision {
        if previous_revision_id != input.revision_id {
            let superseded = sqlx::query(
                "UPDATE project_charter_revision SET lifecycle = 'superseded'
                     WHERE id = ? AND charter_id = ? AND lifecycle = 'approved'",
            )
            .bind(previous_revision_id)
            .bind(&input.charter_id)
            .execute(&mut *transaction)
            .await
            .map_err(check_error)?;
            if superseded.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        }
    }

    let approved_revision = sqlx::query(
        "UPDATE project_charter_revision
             SET lifecycle = CASE WHEN id = ? THEN 'approved' ELSE lifecycle END
             WHERE id = ?",
    )
    .bind(&input.revision_id)
    .bind(&input.revision_id)
    .execute(&mut *transaction)
    .await
    .map_err(check_error)?;
    if approved_revision.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let charter_update = sqlx::query(
        "UPDATE project_charter
             SET current_approved_revision_id = ?, lifecycle = 'ready_for_approval',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND version = ?",
    )
    .bind(&input.revision_id)
    .bind(&input.updated_at)
    .bind(&input.charter_id)
    .bind(input.expected_charter_version)
    .execute(&mut *transaction)
    .await
    .map_err(|error| {
        if error.to_string().contains("UNIQUE") {
            DbError::VersionConflict
        } else {
            check_error(error)
        }
    })?;
    if charter_update.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }

    sqlx::query(
        "INSERT INTO project_charter_approval (
                id, approval_type, charter_id, revision_id, content_digest,
                rendered_digest, expected_charter_version, approved_name,
                approved_slug, selected_identity_id, selected_profile_id,
                selected_operating_skill_revision_id, selected_policy_revision,
                selected_policy_digest, approving_principal_type,
                approving_principal_id, authorization_basis, authorization_action,
                explicit_event, authorization_occurred_at, source_action,
                lifecycle, idempotency_key, version,
                created_at, updated_at, approved_project_mode, approval_event_id
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       'active', ?, 1, ?, ?, ?, ?)",
    )
    .bind(&input.id)
    .bind(&input.approval_type)
    .bind(&input.charter_id)
    .bind(&input.revision_id)
    .bind(&input.content_digest)
    .bind(&input.rendered_digest)
    .bind(input.expected_charter_version)
    .bind(input.approved_name.as_deref())
    .bind(input.approved_slug.as_deref())
    .bind(input.selected_identity_id.as_deref())
    .bind(input.selected_profile_id.as_deref())
    .bind(input.selected_operating_skill_revision_id.as_deref())
    .bind(input.selected_policy_revision.as_deref())
    .bind(input.selected_policy_digest.as_deref())
    .bind(&input.approving_principal_type)
    .bind(&input.approving_principal_id)
    .bind(&input.authorization_basis)
    .bind(&input.authorization_action)
    .bind(&input.explicit_event)
    .bind(&input.authorization_occurred_at)
    .bind(&input.source_action)
    .bind(&storage_idempotency_key)
    .bind(&input.created_at)
    .bind(&input.updated_at)
    .bind(&input.approved_project_mode)
    .bind(Option::<String>::None)
    .execute(&mut *transaction)
    .await
    .map_err(|error| {
        if error.to_string().contains("UNIQUE") {
            DbError::VersionConflict
        } else {
            check_error(error)
        }
    })?;
    sqlx::query(
        "INSERT INTO project_charter_approval_event (
                id, approval_id, lifecycle, principal_type, principal_id,
                authorization_basis, action, explicit_event, idempotency_key,
                occurred_at, created_at
             ) VALUES (?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&input.event_id)
    .bind(&input.id)
    .bind(&input.approving_principal_type)
    .bind(&input.approving_principal_id)
    .bind(&input.authorization_basis)
    .bind(&input.authorization_action)
    .bind(&input.explicit_event)
    .bind(format!("{storage_idempotency_key}:active"))
    .bind(&input.authorization_occurred_at)
    .bind(&input.created_at)
    .execute(&mut *transaction)
    .await
    .map_err(check_error)?;
    let receipt_event = sqlx::query(
        "UPDATE project_charter_approval
             SET approval_event_id = ?
             WHERE id = ? AND approval_event_id IS NULL",
    )
    .bind(&input.event_id)
    .bind(&input.id)
    .execute(&mut *transaction)
    .await
    .map_err(check_error)?;
    if receipt_event.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let genesis_update = sqlx::query(
        "UPDATE product_genesis_session
             SET charter_revision_id = ?, charter_approval_id = ?, charter_version = ?,
                 lifecycle = 'ready_for_project', version = version + 1, updated_at = ?
             WHERE charter_id = ? AND account_id = ?
               AND lifecycle IN ('discovering', 'ready_for_project')",
    )
    .bind(&input.revision_id)
    .bind(&input.id)
    .bind(input.expected_charter_version + 1)
    .bind(&input.updated_at)
    .bind(&input.charter_id)
    .bind(&target.try_get::<String, _>("account_id")?)
    .execute(&mut *transaction)
    .await
    .map_err(check_error)?;
    if genesis_update.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project_charter.approved".to_owned(),
        entity_type: "project_charter_approval".to_owned(),
        entity_id: input.id.clone(),
        actor_type: input.approving_principal_type.clone(),
        actor_id: Some(input.approving_principal_id.clone()),
        scope_type: "account".to_owned(),
        scope_id: target.try_get::<String, _>("account_id")?,
        correlation_id: input.id.clone(),
        causation_id: Some(input.event_id.clone()),
        causation_depth: 0,
        dedupe_key: Some(format!("project-charter-approved:{}", input.id)),
        payload_json: serde_json::json!({
            "approval_id": input.id.clone(),
            "charter_id": input.charter_id.clone(),
            "revision_id": input.revision_id.clone(),
            "approval_event_id": input.event_id.clone(),
            "content_digest": input.content_digest.clone(),
            "rendered_digest": input.rendered_digest.clone(),
            "approved_project_mode": input.approved_project_mode.clone(),
        })
        .to_string(),
        created_at: input.created_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut transaction, &event).await?;
    transaction.commit().await?;
    db.get_project_charter_approval(&input.id)
        .await?
        .ok_or(DbError::NotFound)
}
pub(super) async fn apply_project_charter_approval_command(
    db: &SqliteDb,
    input: ApplyProjectCharterApprovalCommand,
) -> Result<AppliedProjectCharterApprovalRecord> {
    let mut tx = crate::begin_immediate(db.pool()).await?;

    // Resolve the durable command receipt before any mutable lifecycle,
    // version, binding, or approval validation.  A lost response must
    // replay even after the Project has moved to its new Charter state;
    // an altered principal/digest/key is rejected by the receipt lookup.
    if let Some(receipt) =
        resolve_command_replay(db, &mut tx, input.command_receipt.as_ref()).await?
    {
        validate_command_scope(input.command_receipt.as_ref(), "project", &input.project_id)?;
        validate_command_outcome_identity(
            &receipt,
            &[
                ("project_id", input.project_id.as_str()),
                ("charter_id", input.approval.charter_id.as_str()),
                ("revision_id", input.approval.revision_id.as_str()),
            ],
        )?;
        let approval_id = command_outcome_string(&receipt, "approval_id")?;
        let binding_id =
            command_outcome_string_any(&receipt, &["project_agent_binding_id", "binding_id"])?;
        let chat_id = command_outcome_optional_string(&receipt, "project_chat_id")?;
        validate_replay_action_bundle(&mut tx, &receipt, input.action_execution.as_ref()).await?;
        let approval_row = sqlx::query("SELECT * FROM project_charter_approval WHERE id = ?")
            .bind(&approval_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?;
        let approval = map_charter_approval(approval_row)?;
        let project_row = sqlx::query(
            "SELECT version, charter_status, charter_setup_required,
                        current_charter_id, current_charter_revision_id
                 FROM project WHERE id = ?",
        )
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        let project_version: i64 = project_row.try_get("version")?;
        let project_charter_status: String = project_row.try_get("charter_status")?;
        let project_charter_setup_required: i64 = project_row.try_get("charter_setup_required")?;
        let project_charter_id: Option<String> = project_row.try_get("current_charter_id")?;
        let project_charter_revision_id: Option<String> =
            project_row.try_get("current_charter_revision_id")?;
        let binding_id: String = sqlx::query_scalar(
            "SELECT id FROM project_agent_binding
                 WHERE id = ? AND project_id = ?
                 LIMIT 1",
        )
        .bind(&binding_id)
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::IdempotencyConflict)?;
        let chat_id: String = if let Some(chat_id) = chat_id {
            sqlx::query_scalar(
                "SELECT id FROM agent_chat
                     WHERE id = ? AND kind = 'project' AND project_id = ?",
            )
            .bind(chat_id)
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?
        } else {
            sqlx::query_scalar(
                "SELECT id FROM agent_chat
                     WHERE kind = 'project' AND project_id = ? LIMIT 1",
            )
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::IdempotencyConflict)?
        };
        let outcome: serde_json::Value = serde_json::from_str(&receipt.outcome_json)
            .map_err(|_| DbError::IdempotencyConflict)?;
        let bootstrap_message_id = outcome
            .get("bootstrap_message_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        let amendment_id = outcome
            .get("amendment_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        if let Some(message_id) = bootstrap_message_id.as_deref() {
            let exists: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM agent_chat_message
                     WHERE id = ? AND chat_id = ? LIMIT 1",
            )
            .bind(message_id)
            .bind(&chat_id)
            .fetch_optional(&mut *tx)
            .await?;
            if exists.is_none() {
                return Err(DbError::IdempotencyConflict);
            }
        }
        if let Some(amendment_id) = amendment_id.as_deref() {
            let exists: Option<i64> = sqlx::query_scalar(
                "SELECT 1 FROM project_charter_amendment
                     WHERE id = ? AND project_id = ? AND approval_id = ? LIMIT 1",
            )
            .bind(amendment_id)
            .bind(&input.project_id)
            .bind(&approval_id)
            .fetch_optional(&mut *tx)
            .await?;
            if exists.is_none() {
                return Err(DbError::IdempotencyConflict);
            }
        }
        let project_charter_id = project_charter_id.ok_or(DbError::IdempotencyConflict)?;
        let project_charter_revision_id =
            project_charter_revision_id.ok_or(DbError::IdempotencyConflict)?;
        let record = AppliedProjectCharterApprovalRecord {
            approval,
            project_id: input.project_id,
            project_version,
            project_charter_status,
            project_charter_setup_required: project_charter_setup_required != 0,
            project_charter_id,
            project_charter_revision_id,
            project_agent_binding_id: binding_id,
            project_chat_id: chat_id,
            bootstrap_message_id,
            amendment_id,
        };
        tx.commit().await?;
        return Ok(record);
    }

    validate_command_scope(input.command_receipt.as_ref(), "project", &input.project_id)?;
    let approval = &input.approval;
    if !matches!(
        approval.approval_type.as_str(),
        "adoption" | "charter_amendment"
    ) {
        return Err(DbError::Check(
            "Project Charter command requires an adoption or amendment approval".to_owned(),
        ));
    }
    if approval.id.trim().is_empty()
        || approval.charter_id.trim().is_empty()
        || approval.revision_id.trim().is_empty()
        || approval.approving_principal_type.trim().is_empty()
        || approval.approving_principal_id.trim().is_empty()
        || approval.authorization_basis.trim().is_empty()
        || approval.authorization_action.trim().is_empty()
        || approval.explicit_event.trim().is_empty()
        || !valid_authorization_timestamp(&approval.authorization_occurred_at)
    {
        return Err(DbError::VersionConflict);
    }
    if approval.selected_identity_id.is_none()
        || approval.selected_profile_id.is_none()
        || approval.selected_operating_skill_revision_id.is_none()
        || approval.selected_policy_revision.is_none()
        || approval.selected_policy_digest.is_none()
    {
        return Err(DbError::VersionConflict);
    }

    let project = sqlx::query(
        "SELECT owner_id, name, charter_status, charter_setup_required,
                    current_charter_id, current_charter_revision_id, version
             FROM project WHERE id = ?",
    )
    .bind(&input.project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let project_owner_id: Option<String> = project.try_get("owner_id")?;
    let project_charter_status: String = project.try_get("charter_status")?;
    let project_charter_setup_required: i64 = project.try_get("charter_setup_required")?;
    let current_charter_id: Option<String> = project.try_get("current_charter_id")?;
    let current_charter_revision_id: Option<String> =
        project.try_get("current_charter_revision_id")?;
    let project_version: i64 = project.try_get("version")?;
    if project_version != input.expected_project_version || approval.expected_charter_version < 1 {
        return Err(DbError::VersionConflict);
    }
    let expected_approval_type = if project_charter_status == "legacy_unverified"
        && project_charter_setup_required != 0
        && current_charter_id.is_none()
        && current_charter_revision_id.is_none()
    {
        "adoption"
    } else if project_charter_status == "charter_backed"
        && project_charter_setup_required == 0
        && current_charter_id.is_some()
        && current_charter_revision_id.is_some()
    {
        "charter_amendment"
    } else {
        return Err(DbError::VersionConflict);
    };
    if approval.approval_type != expected_approval_type
        || input.expected_current_charter_revision_id != current_charter_revision_id
    {
        return Err(DbError::VersionConflict);
    }

    let charter = sqlx::query(
        "SELECT account_id, project_id, version, project_mode, maturity,
                    current_approved_revision_id
             FROM project_charter WHERE id = ?",
    )
    .bind(&approval.charter_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let account_id: String = charter.try_get("account_id")?;
    let charter_project_id: Option<String> = charter.try_get("project_id")?;
    if charter_project_id.as_deref() != Some(input.project_id.as_str()) {
        return Err(DbError::VersionConflict);
    }
    if project_owner_id
        .as_deref()
        .is_some_and(|owner| owner != account_id)
    {
        return Err(DbError::VersionConflict);
    }
    let charter_version: i64 = charter.try_get("version")?;
    let charter_project_mode: String = charter.try_get("project_mode")?;
    if charter_version != approval.expected_charter_version
        || charter_project_mode != approval.approved_project_mode
    {
        return Err(DbError::VersionConflict);
    }
    let previous_approved_revision_id: Option<String> =
        charter.try_get("current_approved_revision_id")?;
    if expected_approval_type == "charter_amendment"
        && previous_approved_revision_id.as_deref() != current_charter_revision_id.as_deref()
    {
        return Err(DbError::VersionConflict);
    }
    if expected_approval_type == "adoption" && previous_approved_revision_id.is_some() {
        return Err(DbError::VersionConflict);
    }
    let revision = sqlx::query(
        "SELECT lifecycle, charter_id, content_digest, rendered_digest
             FROM project_charter_revision
             WHERE id = ? AND charter_id = ?",
    )
    .bind(&approval.revision_id)
    .bind(&approval.charter_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let revision_lifecycle: String = revision.try_get("lifecycle")?;
    let revision_charter_id: String = revision.try_get("charter_id")?;
    let revision_content_digest: String = revision.try_get("content_digest")?;
    let revision_rendered_digest: String = revision.try_get("rendered_digest")?;
    if revision_charter_id != approval.charter_id
        || revision_content_digest != approval.content_digest
        || revision_rendered_digest != approval.rendered_digest
        || !matches!(
            revision_lifecycle.as_str(),
            "draft" | "proposed" | "approved"
        )
    {
        return Err(DbError::VersionConflict);
    }
    if expected_approval_type == "charter_amendment"
        && approval.revision_id == current_charter_revision_id.as_deref().unwrap_or_default()
    {
        return Err(DbError::VersionConflict);
    }

    let identity_id = approval
        .selected_identity_id
        .as_deref()
        .ok_or(DbError::VersionConflict)?;
    let profile_id = approval
        .selected_profile_id
        .as_deref()
        .ok_or(DbError::VersionConflict)?;
    let skill_revision_id = approval
        .selected_operating_skill_revision_id
        .as_deref()
        .ok_or(DbError::VersionConflict)?;
    let selected = sqlx::query(
        "SELECT p.tool_policy_json, i.paused, i.archived_at,
                    i.selected_profile_id, sr.id AS skill_revision_id,
                    sr.skill_key, s.current_revision_id, s.lifecycle
             FROM agent_profile p
             JOIN agent_identity i ON i.id = p.identity_id
             JOIN operating_skill_revision sr ON sr.id = ?
             JOIN operating_skill s ON s.id = sr.operating_skill_id
             WHERE p.id = ? AND p.identity_id = ?
               AND i.owner_id = ? AND i.selected_profile_id = p.id
             LIMIT 1",
    )
    .bind(skill_revision_id)
    .bind(profile_id)
    .bind(identity_id)
    .bind(&account_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| DbError::VersionConflict)?;
    let selected_paused: i64 = selected.try_get("paused")?;
    let selected_archived: Option<String> = selected.try_get("archived_at")?;
    let selected_profile_id: Option<String> = selected.try_get("selected_profile_id")?;
    let selected_skill_revision_id: String = selected.try_get("skill_revision_id")?;
    let selected_skill_key: String = selected.try_get("skill_key")?;
    let selected_skill_current_revision_id: Option<String> =
        selected.try_get("current_revision_id")?;
    let selected_skill_lifecycle: String = selected.try_get("lifecycle")?;
    let selected_tool_policy_json: String = selected.try_get("tool_policy_json")?;
    let selected_policy_digest = profile_policy_digest(&selected_tool_policy_json);
    if selected_paused != 0
        || selected_archived.is_some()
        || selected_profile_id.as_deref() != Some(profile_id)
        || selected_skill_revision_id != skill_revision_id
        || selected_skill_current_revision_id.as_deref() != Some(skill_revision_id)
        || selected_skill_lifecycle != "active"
        || selected_skill_key != PROJECT_OPERATING_SKILL_KEY
        || approval.selected_policy_digest.as_deref() != Some(selected_policy_digest.as_str())
    {
        return Err(DbError::VersionConflict);
    }

    if expected_approval_type == "adoption" {
        let approved_name = approval
            .approved_name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .ok_or(DbError::VersionConflict)?;
        let name_taken: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM project
                 WHERE owner_id = ? AND name = ? AND id != ? LIMIT 1",
        )
        .bind(project_owner_id.as_deref().unwrap_or(account_id.as_str()))
        .bind(approved_name)
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?;
        if name_taken.is_some() {
            return Err(DbError::VersionConflict);
        }
    }

    // A prior active approval is an immutable receipt, so superseding it
    // records a lifecycle event rather than rewriting its target fields.
    let previous_active_approval: Option<String> = sqlx::query_scalar(
        "SELECT id FROM project_charter_approval
             WHERE charter_id = ? AND lifecycle = 'active' AND id != ? LIMIT 1",
    )
    .bind(&approval.charter_id)
    .bind(&approval.id)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(previous_id) = previous_active_approval {
        let revoked = sqlx::query(
            "UPDATE project_charter_approval
                 SET lifecycle = 'revoked', version = version + 1, updated_at = ?
                 WHERE id = ? AND lifecycle = 'active'",
        )
        .bind(&approval.updated_at)
        .bind(&previous_id)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if revoked.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_charter_approval_event (
                    id, approval_id, lifecycle, principal_type, principal_id,
                    authorization_basis, action, explicit_event, reason,
                    idempotency_key, occurred_at, created_at
                 ) VALUES (?, ?, 'revoked', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(new_uuid_v4())
        .bind(&previous_id)
        .bind(&approval.approving_principal_type)
        .bind(&approval.approving_principal_id)
        .bind(&approval.authorization_basis)
        .bind(&approval.authorization_action)
        .bind(&approval.explicit_event)
        .bind("Superseded by newer Project approval")
        .bind(format!(
            "{}:revoke:{}",
            approval.idempotency_key, previous_id
        ))
        .bind(&approval.authorization_occurred_at)
        .bind(&approval.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
    }

    if let Some(previous_revision_id) = previous_approved_revision_id.as_deref() {
        if previous_revision_id != approval.revision_id {
            let superseded = sqlx::query(
                "UPDATE project_charter_revision SET lifecycle = 'superseded'
                     WHERE id = ? AND charter_id = ? AND lifecycle = 'approved'",
            )
            .bind(previous_revision_id)
            .bind(&approval.charter_id)
            .execute(&mut *tx)
            .await
            .map_err(orchestration_write_error)?;
            if superseded.rows_affected() != 1 {
                return Err(DbError::VersionConflict);
            }
        }
    }
    let approved_revision = sqlx::query(
        "UPDATE project_charter_revision SET lifecycle = 'approved'
             WHERE id = ? AND charter_id = ? AND lifecycle != 'approved'",
    )
    .bind(&approval.revision_id)
    .bind(&approval.charter_id)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if approved_revision.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    let charter_update = sqlx::query(
        "UPDATE project_charter
             SET current_approved_revision_id = ?, lifecycle = 'attached',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND version = ?",
    )
    .bind(&approval.revision_id)
    .bind(&approval.updated_at)
    .bind(&approval.charter_id)
    .bind(&input.project_id)
    .bind(approval.expected_charter_version)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if charter_update.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }

    sqlx::query(
        "INSERT INTO project_charter_approval (
                id, approval_type, charter_id, revision_id, content_digest,
                rendered_digest, expected_charter_version, approved_name,
                approved_slug, selected_identity_id, selected_profile_id,
                selected_operating_skill_revision_id, selected_policy_revision,
                selected_policy_digest, approving_principal_type,
                approving_principal_id, authorization_basis, authorization_action,
                explicit_event, authorization_occurred_at, source_action,
                lifecycle, idempotency_key, version, created_at, updated_at,
                approved_project_mode, approval_event_id
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       'active', ?, 1, ?, ?, ?, NULL)",
    )
    .bind(&approval.id)
    .bind(&approval.approval_type)
    .bind(&approval.charter_id)
    .bind(&approval.revision_id)
    .bind(&approval.content_digest)
    .bind(&approval.rendered_digest)
    .bind(approval.expected_charter_version)
    .bind(approval.approved_name.as_deref())
    .bind(approval.approved_slug.as_deref())
    .bind(approval.selected_identity_id.as_deref())
    .bind(approval.selected_profile_id.as_deref())
    .bind(approval.selected_operating_skill_revision_id.as_deref())
    .bind(approval.selected_policy_revision.as_deref())
    .bind(approval.selected_policy_digest.as_deref())
    .bind(&approval.approving_principal_type)
    .bind(&approval.approving_principal_id)
    .bind(&approval.authorization_basis)
    .bind(&approval.authorization_action)
    .bind(&approval.explicit_event)
    .bind(&approval.authorization_occurred_at)
    .bind(&approval.source_action)
    .bind(&approval.idempotency_key)
    .bind(&approval.created_at)
    .bind(&approval.updated_at)
    .bind(&approval.approved_project_mode)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    sqlx::query(
        "INSERT INTO project_charter_approval_event (
                id, approval_id, lifecycle, principal_type, principal_id,
                authorization_basis, action, explicit_event, idempotency_key,
                occurred_at, created_at
             ) VALUES (?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&approval.event_id)
    .bind(&approval.id)
    .bind(&approval.approving_principal_type)
    .bind(&approval.approving_principal_id)
    .bind(&approval.authorization_basis)
    .bind(&approval.authorization_action)
    .bind(&approval.explicit_event)
    .bind(format!("{}:active", approval.idempotency_key))
    .bind(&approval.authorization_occurred_at)
    .bind(&approval.created_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    let linked = sqlx::query(
        "UPDATE project_charter_approval
             SET approval_event_id = ?
             WHERE id = ? AND lifecycle = 'active' AND approval_event_id IS NULL",
    )
    .bind(&approval.event_id)
    .bind(&approval.id)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if linked.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }

    let amendment_id = if expected_approval_type == "charter_amendment" {
        let base_revision_id = previous_approved_revision_id
            .clone()
            .ok_or(DbError::VersionConflict)?;
        let amendment_id = input.amendment_id.clone().ok_or(DbError::VersionConflict)?;
        let rationale = input
            .amendment_rationale
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(DbError::VersionConflict)?;
        let material_diff_json = input
            .amendment_material_diff_json
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        let affected_records_json = input
            .amendment_affected_records_json
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        if serde_json::from_str::<serde_json::Value>(material_diff_json).is_err()
            || serde_json::from_str::<serde_json::Value>(affected_records_json).is_err()
        {
            return Err(DbError::Check(
                "Charter amendment material diff and affected records must be JSON".to_owned(),
            ));
        }
        sqlx::query(
            "INSERT INTO project_charter_amendment (
                    id, project_id, base_charter_revision_id, candidate_revision_id,
                    lifecycle, rationale, material_diff_json, affected_records_json,
                    requested_principal_type, requested_principal_id,
                    expected_project_version, approval_id, version, created_at, updated_at
                 ) VALUES (?, ?, ?, ?, 'approved', ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(&amendment_id)
        .bind(&input.project_id)
        .bind(&base_revision_id)
        .bind(&approval.revision_id)
        .bind(rationale)
        .bind(material_diff_json)
        .bind(affected_records_json)
        .bind(&approval.approving_principal_type)
        .bind(&approval.approving_principal_id)
        .bind(input.expected_project_version)
        .bind(&approval.id)
        .bind(&approval.created_at)
        .bind(&approval.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        Some(amendment_id)
    } else {
        if input.amendment_id.is_some()
            || input.amendment_rationale.is_some()
            || input.amendment_material_diff_json.is_some()
            || input.amendment_affected_records_json.is_some()
        {
            return Err(DbError::VersionConflict);
        }
        None
    };

    let binding = sqlx::query(
        "SELECT id, state, version, autonomy_policy_json,
                    permission_ceiling_json, subscriptions_json, wake_budget
             FROM project_agent_binding
             WHERE id = ? AND project_id = ?
               AND state IN ('active', 'agent_setup_required')",
    )
    .bind(&input.existing_binding_id)
    .bind(&input.project_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(DbError::NotFound)?;
    let binding_id: String = binding.try_get("id")?;
    let binding_state: String = binding.try_get("state")?;
    let binding_version: i64 = binding.try_get("version")?;
    let binding_policy: String = binding.try_get("autonomy_policy_json")?;
    let binding_ceiling: String = binding.try_get("permission_ceiling_json")?;
    let binding_subscriptions: String = binding.try_get("subscriptions_json")?;
    let binding_wake_budget: i64 = binding.try_get("wake_budget")?;
    let selected_policy_revision = approval
        .selected_policy_revision
        .as_deref()
        .ok_or(DbError::VersionConflict)?;
    let selected_policy_digest = approval
        .selected_policy_digest
        .as_deref()
        .ok_or(DbError::VersionConflict)?;
    let project_agent_binding_id = if binding_state == "agent_setup_required" {
        if input.replacement_binding_id.is_some() {
            return Err(DbError::VersionConflict);
        }
        let updated = sqlx::query(
            "UPDATE project_agent_binding
                 SET identity_id = ?, profile_id = ?, state = 'active',
                     permission_ceiling_json = ?,
                     operating_skill_revision_id = ?, policy_revision = ?, policy_digest = ?,
                     charter_id = ?, charter_revision_id = ?, charter_setup_required = 1,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND project_id = ?
                   AND state = 'agent_setup_required' AND version = ?",
        )
        .bind(identity_id)
        .bind(profile_id)
        .bind(PROJECT_AGENT_PERMISSION_CEILING)
        .bind(skill_revision_id)
        .bind(selected_policy_revision)
        .bind(selected_policy_digest)
        .bind(&approval.charter_id)
        .bind(&approval.revision_id)
        .bind(&approval.updated_at)
        .bind(&binding_id)
        .bind(&input.project_id)
        .bind(binding_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if updated.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        binding_id.clone()
    } else {
        let replacement_id = input
            .replacement_binding_id
            .clone()
            .unwrap_or_else(new_uuid_v4);
        if replacement_id.trim().is_empty() || replacement_id == binding_id {
            return Err(DbError::VersionConflict);
        }
        let replaced = sqlx::query(
            "UPDATE project_agent_binding
                 SET state = 'replaced', replacement_reason = ?,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND project_id = ? AND state = 'active' AND version = ?",
        )
        .bind(if expected_approval_type == "charter_amendment" {
            "Project Charter amendment"
        } else {
            "Project Charter adoption"
        })
        .bind(&approval.updated_at)
        .bind(&binding_id)
        .bind(&input.project_id)
        .bind(binding_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if replaced.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        sqlx::query(
            "INSERT INTO project_agent_binding (
                    id, project_id, identity_id, profile_id, state,
                    autonomy_policy_json, permission_ceiling_json, subscriptions_json,
                    wake_budget, version, replaced_by_binding_id, replacement_reason,
                    operating_skill_revision_id, policy_revision, policy_digest,
                    charter_id, charter_revision_id, charter_setup_required,
                    created_at, updated_at
                 ) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, NULL, NULL,
                           ?, ?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(&replacement_id)
        .bind(&input.project_id)
        .bind(identity_id)
        .bind(profile_id)
        .bind(&binding_policy)
        .bind({
            operation_registry::authority::parse_permissions(&binding_ceiling)?;
            &binding_ceiling
        })
        .bind(&binding_subscriptions)
        .bind(binding_wake_budget)
        .bind(binding_version)
        .bind(skill_revision_id)
        .bind(selected_policy_revision)
        .bind(selected_policy_digest)
        .bind(&approval.charter_id)
        .bind(&approval.revision_id)
        .bind(&approval.created_at)
        .bind(&approval.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        let linked = sqlx::query(
            "UPDATE project_agent_binding SET replaced_by_binding_id = ?
                 WHERE id = ? AND project_id = ? AND state = 'replaced'",
        )
        .bind(&replacement_id)
        .bind(&binding_id)
        .bind(&input.project_id)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        if linked.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        replacement_id
    };

    let project_chat_id: String =
        sqlx::query_scalar("SELECT id FROM agent_chat WHERE kind = 'project' AND project_id = ?")
            .bind(&input.project_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DbError::NotFound)?;
    sqlx::query(
        "UPDATE agent_chat SET status = 'ready', version = version + 1,
                    updated_at = ?
             WHERE id = ? AND kind = 'project' AND project_id = ?
               AND status = 'agent_setup_required'",
    )
    .bind(&approval.updated_at)
    .bind(&project_chat_id)
    .bind(&input.project_id)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;

    let project_update = if expected_approval_type == "adoption" {
        let approved_name = approval
            .approved_name
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        sqlx::query(
            "UPDATE project
                 SET name = ?, charter_status = 'charter_backed',
                     charter_setup_required = 0, current_charter_id = ?,
                     current_charter_revision_id = ?, current_charter_version = ?,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND version = ? AND charter_status = 'legacy_unverified'
                   AND charter_setup_required = 1
                   AND current_charter_id IS NULL AND current_charter_revision_id IS NULL",
        )
        .bind(approved_name)
        .bind(&approval.charter_id)
        .bind(&approval.revision_id)
        .bind(approval.expected_charter_version + 1)
        .bind(&approval.updated_at)
        .bind(&input.project_id)
        .bind(input.expected_project_version)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?
    } else {
        sqlx::query(
            "UPDATE project
                 SET current_charter_id = ?, current_charter_revision_id = ?,
                     current_charter_version = ?, version = version + 1,
                     updated_at = ?
                 WHERE id = ? AND version = ? AND charter_status = 'charter_backed'
                   AND charter_setup_required = 0
                   AND current_charter_id = ? AND current_charter_revision_id = ?",
        )
        .bind(&approval.charter_id)
        .bind(&approval.revision_id)
        .bind(approval.expected_charter_version + 1)
        .bind(&approval.updated_at)
        .bind(&input.project_id)
        .bind(input.expected_project_version)
        .bind(&approval.charter_id)
        .bind(current_charter_revision_id.as_deref())
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?
    };
    if project_update.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }

    let bootstrap_message_id = if expected_approval_type == "adoption" {
        let message_id = input
            .bootstrap_message_id
            .clone()
            .ok_or(DbError::VersionConflict)?;
        let content = input
            .bootstrap_content
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        let guard = input
            .bootstrap_content_guard_json
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        let author_id = input
            .bootstrap_author_id
            .as_deref()
            .unwrap_or(approval.approving_principal_id.as_str());
        let correlation_id = input
            .bootstrap_correlation_id
            .as_deref()
            .unwrap_or(approval.idempotency_key.as_str());
        let source_metadata = input
            .bootstrap_source_metadata_json
            .as_deref()
            .unwrap_or("{}");
        if message_id.trim().is_empty()
            || content.trim().is_empty()
            || guard.trim().is_empty()
            || source_metadata.trim().is_empty()
        {
            return Err(DbError::Check(
                "Project Charter adoption bootstrap message is incomplete".to_owned(),
            ));
        }
        let sequence: i64 = sqlx::query_scalar(
            "UPDATE agent_chat
                 SET message_count = message_count + 1, last_message_at = ?,
                     version = version + 1, updated_at = ?
                 WHERE id = ? AND kind = 'project' AND project_id = ?
                 RETURNING message_count - 1",
        )
        .bind(&approval.updated_at)
        .bind(&approval.updated_at)
        .bind(&project_chat_id)
        .bind(&input.project_id)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO agent_chat_message (
                    id, chat_id, sequence, author_type, author_id, content,
                    content_guard_json, sensitivity, status, correlation_id,
                    source_type, source_id, source_metadata_json, created_at
                 ) VALUES (?, ?, ?, 'system', ?, ?, ?, 'internal', 'complete', ?,
                           'native', ?, ?, ?)",
        )
        .bind(&message_id)
        .bind(&project_chat_id)
        .bind(sequence)
        .bind(author_id)
        .bind(content)
        .bind(guard)
        .bind(correlation_id)
        .bind(&approval.id)
        .bind(source_metadata)
        .bind(&approval.created_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        super::chat_read_events::append(
            db,
            &mut tx,
            super::chat_read_events::ChatReadEvent {
                event_type: "agent_chat.message.appended",
                entity_type: "agent_chat_message",
                entity_id: &message_id,
                chat_id: &project_chat_id,
                status: Some("complete"),
                dedupe_key: Some(format!("chat-message:{message_id}")),
                created_at: &now_rfc3339(),
            },
        )
        .await?;
        Some(message_id)
    } else {
        if input.bootstrap_message_id.is_some()
            || input.bootstrap_content.is_some()
            || input.bootstrap_content_guard_json.is_some()
            || input.bootstrap_author_id.is_some()
            || input.bootstrap_correlation_id.is_some()
            || input.bootstrap_source_metadata_json.is_some()
        {
            return Err(DbError::VersionConflict);
        }
        None
    };

    if expected_approval_type == "charter_amendment" {
        // A Charter amendment invalidates execution authorized by the
        // superseded Charter revision. The governance row's runnable
        // projection is the CAS-versioned exception to its immutability.
        sqlx::query(
            "UPDATE project_task_governance
                 SET runnable = 0, version = version + 1, updated_at = ?
                 WHERE project_id = ? AND runnable = 1
                   AND (charter_revision_id IS NULL OR charter_revision_id != ?)",
        )
        .bind(&approval.updated_at)
        .bind(&input.project_id)
        .bind(&approval.revision_id)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
    }

    let (actor_type, actor_id, correlation_id, causation_id, causation_depth) =
        command_event_provenance(
            input.command_receipt.as_ref(),
            approval.approving_principal_type.clone(),
            Some(approval.approving_principal_id.clone()),
            approval.idempotency_key.clone(),
            Some(approval.event_id.clone()),
            0,
        );
    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "project.charter.approved".to_owned(),
        entity_type: "project_charter".to_owned(),
        entity_id: approval.charter_id.clone(),
        actor_type,
        actor_id,
        scope_type: "project".to_owned(),
        scope_id: input.project_id.clone(),
        correlation_id,
        causation_id,
        causation_depth,
        dedupe_key: Some(format!("project-charter-approval:{}", approval.id)),
        payload_json: serde_json::json!({
            "project_id": input.project_id.clone(),
            "charter_id": approval.charter_id.clone(),
            "revision_id": approval.revision_id.clone(),
            "approval_id": approval.id.clone(),
            "approval_type": approval.approval_type.clone(),
            "content_digest": approval.content_digest.clone(),
            "rendered_digest": approval.rendered_digest.clone(),
            "bootstrap_message_id": bootstrap_message_id.clone(),
            "amendment_id": amendment_id.clone(),
        })
        .to_string(),
        created_at: approval.created_at.clone(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut tx, &event).await?;

    let consumed = sqlx::query(
        "UPDATE project_charter_approval
             SET lifecycle = 'consumed', consumed_project_id = ?, consumed_at = ?,
                 version = version + 1, updated_at = ?
             WHERE id = ? AND lifecycle = 'active'",
    )
    .bind(&input.project_id)
    .bind(&approval.updated_at)
    .bind(&approval.updated_at)
    .bind(&approval.id)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if consumed.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }
    sqlx::query(
        "INSERT INTO project_charter_approval_event (
                id, approval_id, lifecycle, principal_type, principal_id,
                authorization_basis, action, explicit_event, reason,
                idempotency_key, occurred_at, created_at
             ) VALUES (?, ?, 'consumed', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&approval.id)
    .bind(&approval.approving_principal_type)
    .bind(&approval.approving_principal_id)
    .bind(&approval.authorization_basis)
    .bind(&approval.authorization_action)
    .bind(&approval.explicit_event)
    .bind(if expected_approval_type == "adoption" {
        "Project Charter adoption applied"
    } else {
        "Project Charter amendment applied"
    })
    .bind(format!("{}:consumed", approval.idempotency_key))
    .bind(&approval.authorization_occurred_at)
    .bind(&approval.updated_at)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;

    let admission_receipt_id = if expected_approval_type == "adoption" {
        let receipt_id = new_uuid_v4();
        sqlx::query(
            "INSERT INTO project_admission_receipt (
                    id, project_id, source_kind, handoff_id,
                    initial_charter_approval_id, initial_charter_id,
                    initial_charter_revision_id, payload_digest,
                    validation_schema_version, validated_at, created_at
                 ) VALUES (?, ?, 'charter_adoption', NULL, ?, ?, ?, ?,
                           'forge.project-admission/v1', ?, ?)",
        )
        .bind(&receipt_id)
        .bind(&input.project_id)
        .bind(&approval.id)
        .bind(&approval.charter_id)
        .bind(&approval.revision_id)
        .bind(&approval.content_digest)
        .bind(&approval.updated_at)
        .bind(&approval.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(orchestration_write_error)?;
        receipt_id
    } else {
        sqlx::query_scalar::<_, String>(
            "SELECT id FROM project_admission_receipt
                 WHERE project_id = ? LIMIT 1",
        )
        .bind(&input.project_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(DbError::VersionConflict)?
    };
    let completed_binding = sqlx::query(
        "UPDATE project_agent_binding
             SET admission_receipt_id = ?, charter_approval_id = ?,
                 charter_setup_required = 0
             WHERE id = ? AND project_id = ? AND state = 'active'
               AND charter_setup_required = 1",
    )
    .bind(&admission_receipt_id)
    .bind(&approval.id)
    .bind(&project_agent_binding_id)
    .bind(&input.project_id)
    .execute(&mut *tx)
    .await
    .map_err(orchestration_write_error)?;
    if completed_binding.rows_affected() != 1 {
        return Err(DbError::VersionConflict);
    }

    finalize_command_in_tx(
        db,
        &mut tx,
        &event.id,
        input.command_receipt.clone(),
        input.action_execution.clone(),
    )
    .await?;

    let approval_row = sqlx::query("SELECT * FROM project_charter_approval WHERE id = ?")
        .bind(&approval.id)
        .fetch_one(&mut *tx)
        .await?;
    let project_row = sqlx::query(
        "SELECT version, charter_status, charter_setup_required,
                    current_charter_id, current_charter_revision_id
             FROM project WHERE id = ?",
    )
    .bind(&input.project_id)
    .fetch_one(&mut *tx)
    .await?;
    let record = AppliedProjectCharterApprovalRecord {
        approval: map_charter_approval(approval_row)?,
        project_id: input.project_id.clone(),
        project_version: project_row.try_get("version")?,
        project_charter_status: project_row.try_get("charter_status")?,
        project_charter_setup_required: project_row.try_get::<i64, _>("charter_setup_required")?
            != 0,
        project_charter_id: project_row
            .try_get::<Option<String>, _>("current_charter_id")?
            .ok_or(DbError::VersionConflict)?,
        project_charter_revision_id: project_row
            .try_get::<Option<String>, _>("current_charter_revision_id")?
            .ok_or(DbError::VersionConflict)?,
        project_agent_binding_id,
        project_chat_id,
        bootstrap_message_id,
        amendment_id,
    };
    tx.commit().await?;
    Ok(record)
}
