use super::*;
use crate::new_uuid_v4;

struct TaskAuthorityUpdate {
    expected_version: i64,
    review_passed_at: Option<String>,
}

async fn validate_review_details(
    transaction: &mut sqlx::SqliteConnection,
    review: &Review,
    status: &ReviewStatus,
    details: &serde_json::Value,
) -> Result<()> {
    if matches!(status, ReviewStatus::Passed | ReviewStatus::AwaitingHuman) {
        if let Some(value) = details.get("conformance").filter(|v| !v.is_null()) {
            let conformance: api_types::ReviewConformance =
                serde_json::from_value(value.clone()).map_err(|e| DbError::Check(e.to_string()))?;
            if conformance.status == api_types::ConformanceStatus::Passed {
                let contract = conformance
                    .contract
                    .as_ref()
                    .ok_or_else(|| DbError::Check("accepted review has no contract".into()))?;
                if contract.context.task_id != review.task_id {
                    return Err(DbError::Check(
                        "review contract belongs to another Task".into(),
                    ));
                }
                crate::review_conformance::verify_review_source(transaction, contract).await?;
                let frozen: Option<String> = sqlx::query_scalar(
                    "SELECT conformance_json FROM execution_review_assessment WHERE execution_id = ?",
                )
                .bind(&contract.execution_id)
                .fetch_optional(&mut *transaction)
                .await?;
                if frozen
                    .as_deref()
                    .map(serde_json::from_str::<api_types::ReviewConformance>)
                    .transpose()
                    .map_err(|e| DbError::Check(e.to_string()))?
                    .as_ref()
                    != Some(&conformance)
                {
                    return Err(DbError::Check(
                        "review conformance has no matching immutable assessment".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn update_status_inner(
    db: &SqliteDb,
    id: &str,
    status: ReviewStatus,
    step_results_json: String,
    finished_at: Option<String>,
    updated_at: &str,
    task_authority: Option<TaskAuthorityUpdate>,
    expected_project_version: Option<i64>,
    expected_workflow_definition: Option<&str>,
    expected_review_status: Option<ReviewStatus>,
    expected_review_updated_at: Option<&str>,
    expected_candidate_execution_id: Option<&str>,
) -> Result<(Review, Option<Task>)> {
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    let review = sqlx::query("SELECT * FROM review WHERE id = ?")
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?
        .map(map_review)
        .transpose()?
        .ok_or(DbError::NotFound)?;
    if expected_review_status
        .as_ref()
        .is_some_and(|expected| &review.status != expected)
        || expected_review_updated_at.is_some_and(|expected| review.updated_at != expected)
    {
        return Err(DbError::VersionConflict);
    }
    let details: serde_json::Value =
        serde_json::from_str(&step_results_json).map_err(|e| DbError::ReviewDetailsCorrupt {
            review_id: review.id.clone(),
            reason: e.to_string(),
        })?;
    validate_review_details(&mut transaction, &review, &status, &details).await?;
    if !review_transition_allowed(&review.status, &status) {
        return Err(DbError::InvalidTransition);
    }
    if task_authority.is_some()
        && matches!(
            review.status,
            ReviewStatus::Passed | ReviewStatus::Failed | ReviewStatus::Cancelled
        )
    {
        // Terminal settlement is a one-shot authority transition. A replay
        // must use the reconciliation path rather than bumping the Task
        // version or rewriting its projection a second time.
        return Err(DbError::InvalidTransition);
    }

    let task_authority = if let Some(task_authority) = task_authority {
        if !matches!(status, ReviewStatus::Passed | ReviewStatus::Failed) {
            return Err(DbError::Check(
                "task review authority requires a terminal Review status".to_owned(),
            ));
        }
        if matches!(status, ReviewStatus::Passed) != task_authority.review_passed_at.is_some() {
            return Err(DbError::Check(
                "task review authority does not match Review status".to_owned(),
            ));
        }

        // A manual approval/rejection may have waited after loading the
        // latest row.  Bind the settlement to that exact latest Review before
        // changing either row; a newer attempt must win as a whole.
        let latest_review_id: Option<String> = sqlx::query_scalar(
            "SELECT id
             FROM review
             WHERE task_id = ?
             ORDER BY attempt_number DESC, created_at DESC, id DESC
             LIMIT 1",
        )
        .bind(&review.task_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if latest_review_id.as_deref() != Some(review.id.as_str()) {
            return Err(DbError::VersionConflict);
        }

        let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(&review.task_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
        let task = map_task(task_row)?;
        if task.deleted_at.is_some() || task.version != task_authority.expected_version {
            return Err(DbError::VersionConflict);
        }
        match (expected_project_version, expected_workflow_definition) {
            (Some(expected_project_version), Some(expected_workflow_definition)) => {
                let project = sqlx::query(
                    "SELECT version, workflow_definition
                     FROM project WHERE id = ?",
                )
                .bind(&task.project_id)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(DbError::NotFound)?;
                let project_version: i64 = project.try_get("version")?;
                let workflow_definition: String = project.try_get("workflow_definition")?;
                if project_version != expected_project_version
                    || workflow_definition != expected_workflow_definition
                {
                    return Err(DbError::VersionConflict);
                }
            }
            (None, None) => {}
            _ => {
                return Err(DbError::Check(
                    "Task review authority requires both Project version and workflow definition"
                        .to_owned(),
                ));
            }
        }
        if let Some(expected_candidate_execution_id) = expected_candidate_execution_id {
            validate_review_candidate_in_tx(
                &mut transaction,
                &task.id,
                expected_candidate_execution_id,
            )
            .await?;
        }
        Some((task_authority, task))
    } else {
        None
    };

    let result = sqlx::query(
        "UPDATE review SET status = ?, step_results_json = ?, finished_at = ?, updated_at = ? WHERE id = ?",
    )
    .bind(status.to_string())
    .bind(&step_results_json)
    .bind(finished_at.as_deref())
    .bind(updated_at)
    .bind(id)
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() == 0 {
        return Err(DbError::NotFound);
    }

    let task_project_id = if let Some((task_authority, task)) = &task_authority {
        let result = sqlx::query(
            "UPDATE task
             SET review_passed_at = ?, updated_at = ?, version = version + 1
             WHERE id = ? AND version = ? AND deleted_at IS NULL",
        )
        .bind(task_authority.review_passed_at.as_deref())
        .bind(updated_at)
        .bind(&task.id)
        .bind(task_authority.expected_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
        task.project_id.clone()
    } else {
        sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
            .bind(&review.task_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?
    };

    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "review.status_changed".to_owned(),
        entity_type: "review".to_owned(),
        entity_id: review.id.clone(),
        actor_type: "review_runner".to_owned(),
        actor_id: None,
        scope_type: "task".to_owned(),
        scope_id: review.task_id.clone(),
        correlation_id: review.task_id.clone(),
        causation_id: None,
        causation_depth: 0,
        dedupe_key: Some(format!(
            "review-status:{}:{}:{}",
            review.id,
            status,
            finished_at.as_deref().unwrap_or(updated_at)
        )),
        payload_json: serde_json::json!({
            "review_id": review.id,
            "task_id": review.task_id,
            "project_id": task_project_id,
            "attempt_number": review.attempt_number,
            "status": status.to_string(),
            "finished": finished_at.is_some(),
        })
        .to_string(),
        created_at: updated_at.to_owned(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut transaction, &event).await?;

    let updated_row = sqlx::query("SELECT * FROM review WHERE id = ?")
        .bind(id)
        .fetch_one(&mut *transaction)
        .await?;
    let updated_review = map_review(updated_row)?;
    let updated_task = if task_authority.is_some() {
        let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(&review.task_id)
            .fetch_one(&mut *transaction)
            .await?;
        Some(map_task(task_row)?)
    } else {
        None
    };
    transaction.commit().await?;
    Ok((updated_review, updated_task))
}

/// ReviewRunner's status boundary. Unlike the historical `update_status`
/// method, this always rechecks the Task/Project/workflow/candidate and the
/// exact prior Review row under the writer lock. A Task projection is optional
/// because CI-only failures/awaiting-human states do not change
/// `task.review_passed_at`.
#[allow(clippy::too_many_arguments)]
async fn update_status_with_review_authority_inner(
    db: &SqliteDb,
    id: &str,
    status: ReviewStatus,
    step_results_json: String,
    finished_at: Option<String>,
    updated_at: &str,
    expected_task_version: i64,
    expected_task_status: &str,
    expected_project_version: Option<i64>,
    expected_workflow_definition: Option<&str>,
    expected_review_status: ReviewStatus,
    expected_review_updated_at: &str,
    expected_candidate_execution_id: Option<&str>,
    task_projection: Option<Option<String>>,
) -> Result<(Review, Option<Task>)> {
    let mut transaction = crate::begin_immediate(&db.pool).await?;
    let review = sqlx::query("SELECT * FROM review WHERE id = ?")
        .bind(id)
        .fetch_optional(&mut *transaction)
        .await?
        .map(map_review)
        .transpose()?
        .ok_or(DbError::NotFound)?;
    if review.status != expected_review_status || review.updated_at != expected_review_updated_at {
        return Err(DbError::VersionConflict);
    }
    let details: serde_json::Value =
        serde_json::from_str(&step_results_json).map_err(|e| DbError::ReviewDetailsCorrupt {
            review_id: review.id.clone(),
            reason: e.to_string(),
        })?;
    validate_review_details(&mut transaction, &review, &status, &details).await?;
    if !review_transition_allowed(&review.status, &status) {
        return Err(DbError::InvalidTransition);
    }

    let latest_review_id: Option<String> = sqlx::query_scalar(
        "SELECT id
         FROM review
         WHERE task_id = ?
         ORDER BY attempt_number DESC, created_at DESC, id DESC
         LIMIT 1",
    )
    .bind(&review.task_id)
    .fetch_optional(&mut *transaction)
    .await?;
    if latest_review_id.as_deref() != Some(review.id.as_str()) {
        return Err(DbError::VersionConflict);
    }

    let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
        .bind(&review.task_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
    let task = map_task(task_row)?;
    if task.deleted_at.is_some()
        || task.version != expected_task_version
        || task.status != expected_task_status
    {
        return Err(DbError::VersionConflict);
    }
    match (expected_project_version, expected_workflow_definition) {
        (Some(expected_project_version), Some(expected_workflow_definition)) => {
            let project = sqlx::query(
                "SELECT version, workflow_definition
                 FROM project WHERE id = ?",
            )
            .bind(&task.project_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(DbError::NotFound)?;
            let project_version: i64 = project.try_get("version")?;
            let workflow_definition: String = project.try_get("workflow_definition")?;
            if project_version != expected_project_version
                || workflow_definition != expected_workflow_definition
            {
                return Err(DbError::VersionConflict);
            }
        }
        (None, None) => {}
        _ => {
            return Err(DbError::Check(
                "Review authority requires both Project version and workflow definition".to_owned(),
            ));
        }
    }
    if let Some(expected_candidate_execution_id) = expected_candidate_execution_id {
        validate_review_candidate_in_tx(
            &mut transaction,
            &task.id,
            expected_candidate_execution_id,
        )
        .await?;
    } else if latest_review_candidate_in_tx(&mut transaction, &task.id)
        .await?
        .is_some()
    {
        return Err(DbError::VersionConflict);
    }

    let result = sqlx::query(
        "UPDATE review
         SET status = ?, step_results_json = ?, finished_at = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(status.to_string())
    .bind(&step_results_json)
    .bind(finished_at.as_deref())
    .bind(updated_at)
    .bind(id)
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() == 0 {
        return Err(DbError::NotFound);
    }

    if let Some(review_passed_at) = task_projection.as_ref() {
        let result = sqlx::query(
            "UPDATE task
             SET review_passed_at = ?, updated_at = ?, version = version + 1
             WHERE id = ? AND version = ? AND deleted_at IS NULL",
        )
        .bind(review_passed_at.as_deref())
        .bind(updated_at)
        .bind(&task.id)
        .bind(expected_task_version)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(DbError::VersionConflict);
        }
    }

    let event = CreateDomainEvent {
        id: new_uuid_v4(),
        event_type: "review.status_changed".to_owned(),
        entity_type: "review".to_owned(),
        entity_id: review.id.clone(),
        actor_type: "review_runner".to_owned(),
        actor_id: None,
        scope_type: "task".to_owned(),
        scope_id: review.task_id.clone(),
        correlation_id: review.task_id.clone(),
        causation_id: None,
        causation_depth: 0,
        dedupe_key: Some(format!(
            "review-status:{}:{}:{}",
            review.id,
            status,
            finished_at.as_deref().unwrap_or(updated_at)
        )),
        payload_json: serde_json::json!({
            "review_id": review.id,
            "task_id": review.task_id,
            "project_id": task.project_id,
            "attempt_number": review.attempt_number,
            "status": status.to_string(),
            "finished": finished_at.is_some(),
        })
        .to_string(),
        created_at: updated_at.to_owned(),
    };
    DomainEventRepo::append_event_in_tx(db, &mut transaction, &event).await?;
    let updated_row = sqlx::query("SELECT * FROM review WHERE id = ?")
        .bind(id)
        .fetch_one(&mut *transaction)
        .await?;
    let updated_review = map_review(updated_row)?;
    let updated_task = if task_projection.is_some() {
        let task_row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM task WHERE id = ?"))
            .bind(&review.task_id)
            .fetch_one(&mut *transaction)
            .await?;
        Some(map_task(task_row)?)
    } else {
        None
    };
    transaction.commit().await?;
    Ok((updated_review, updated_task))
}

#[async_trait]
impl ReviewRepo for SqliteDb {
    async fn create(&self, input: CreateReview) -> Result<Review> {
        sqlx::query("INSERT INTO review (id, task_id, execution_id, attempt_number, status, step_results_json, started_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&input.id)
            .bind(&input.task_id)
            .bind(&input.execution_id)
            .bind(input.attempt_number)
            .bind(input.status.to_string())
            .bind(&input.step_results_json)
            .bind(&input.started_at)
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .execute(&self.pool)
            .await?;
        ReviewRepo::get_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn create_with_task_authority(
        &self,
        mut input: CreateReview,
        expected_task_version: i64,
        expected_task_status: &str,
        expected_project_version: Option<i64>,
        expected_workflow_definition: Option<&str>,
        expected_candidate_execution_id: Option<&str>,
    ) -> Result<Review> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let task = sqlx::query(
            "SELECT version, status, project_id, deleted_at
             FROM task WHERE id = ?",
        )
        .bind(&input.task_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let task_version: i64 = task.try_get("version")?;
        let task_status: String = task.try_get("status")?;
        let deleted_at: Option<String> = task.try_get("deleted_at")?;
        if deleted_at.is_some()
            || task_version != expected_task_version
            || task_status != expected_task_status
        {
            return Err(DbError::VersionConflict);
        }
        match (expected_project_version, expected_workflow_definition) {
            (Some(expected_project_version), Some(expected_workflow_definition)) => {
                let project_id: String = task.try_get("project_id")?;
                let project =
                    sqlx::query("SELECT version, workflow_definition FROM project WHERE id = ?")
                        .bind(&project_id)
                        .fetch_optional(&mut *transaction)
                        .await?
                        .ok_or(DbError::NotFound)?;
                let project_version: i64 = project.try_get("version")?;
                let workflow_definition: String = project.try_get("workflow_definition")?;
                if project_version != expected_project_version
                    || workflow_definition != expected_workflow_definition
                {
                    return Err(DbError::VersionConflict);
                }
            }
            (None, None) => {}
            _ => {
                return Err(DbError::Check(
                    "Task review authority requires both Project version and workflow definition"
                        .to_owned(),
                ));
            }
        }
        if let Some(expected_candidate_execution_id) = expected_candidate_execution_id {
            if input.execution_id != expected_candidate_execution_id {
                return Err(DbError::VersionConflict);
            }
            validate_review_candidate_in_tx(
                &mut transaction,
                &input.task_id,
                expected_candidate_execution_id,
            )
            .await?;
        }
        let next_attempt = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(attempt_number) FROM review WHERE task_id = ?",
        )
        .bind(&input.task_id)
        .fetch_one(&mut *transaction)
        .await?
        .unwrap_or(0)
            + 1;
        input.attempt_number = next_attempt;
        sqlx::query("INSERT INTO review (id, task_id, execution_id, attempt_number, status, step_results_json, started_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&input.id)
            .bind(&input.task_id)
            .bind(&input.execution_id)
            .bind(input.attempt_number)
            .bind(input.status.to_string())
            .bind(&input.step_results_json)
            .bind(&input.started_at)
            .bind(&input.created_at)
            .bind(&input.updated_at)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        ReviewRepo::get_by_id(self, &input.id)
            .await?
            .ok_or(DbError::NotFound)
    }

    async fn create_attempt_with_execution_and_lease(
        &self,
        mut review: CreateReview,
        execution: CreateExecution,
        lease: ClaimExecutionLease,
        admission: Option<ExecutionAdmission>,
    ) -> Result<(Review, Execution)> {
        if execution.status != ExecutionStatus::Running
            || lease.execution_id != execution.id
            || lease.expected_version != 1
            || lease.owner.trim().is_empty()
            || lease.lease_expires_at > lease.hard_deadline_at
            || review.task_id != execution.task_id
            // A Review identifies the candidate execution being reviewed.
            // The newly-created reviewer execution is its child, so the two
            // rows must share the exact candidate lineage.
            || execution.parent_execution_id.as_deref() != Some(review.execution_id.as_str())
        {
            return Err(DbError::Check(
                "review attempt must match a running child execution, candidate, and bounded owner claim"
                    .to_owned(),
            ));
        }

        let mut transaction = crate::begin_immediate(&self.pool).await?;
        validate_review_candidate_in_tx(&mut transaction, &review.task_id, &review.execution_id)
            .await?;
        Self::create_execution_in_tx(&mut transaction, &execution, admission.as_ref()).await?;
        let lease_result = sqlx::query(
            "UPDATE execution
             SET lease_owner = ?,
                 lease_expires_at = MIN(?, ?),
                 hard_deadline_at = ?,
                 last_heartbeat_at = ?,
                 execution_version = execution_version + 1,
                 updated_at = ?
             WHERE id = ? AND status = 'running'
               AND execution_version = 1 AND lease_owner IS NULL",
        )
        .bind(&lease.owner)
        .bind(&lease.lease_expires_at)
        .bind(&lease.hard_deadline_at)
        .bind(&lease.hard_deadline_at)
        .bind(&lease.now)
        .bind(&lease.now)
        .bind(&lease.execution_id)
        .execute(&mut *transaction)
        .await?;
        if lease_result.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }

        let next_attempt = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(attempt_number) FROM review WHERE task_id = ?",
        )
        .bind(&review.task_id)
        .fetch_one(&mut *transaction)
        .await?
        .unwrap_or(0)
            + 1;
        review.attempt_number = next_attempt;
        sqlx::query("INSERT INTO review (id, task_id, execution_id, reviewer_execution_id, attempt_number, status, step_results_json, started_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&review.id)
            .bind(&review.task_id)
            .bind(&review.execution_id)
            .bind(&execution.id)
            .bind(review.attempt_number)
            .bind(review.status.to_string())
            .bind(&review.step_results_json)
            .bind(&review.started_at)
            .bind(&review.created_at)
            .bind(&review.updated_at)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;

        let persisted_review = ReviewRepo::get_by_id(self, &review.id)
            .await?
            .ok_or(DbError::NotFound)?;
        let persisted_execution = ExecutionRepo::get_by_id(self, &execution.id)
            .await?
            .ok_or(DbError::NotFound)?;
        Ok((persisted_review, persisted_execution))
    }

    async fn update_status(
        &self,
        id: &str,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: &str,
    ) -> Result<Review> {
        let (review, _) = update_status_inner(
            self,
            id,
            status,
            step_results_json,
            finished_at,
            updated_at,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await?;
        Ok(review)
    }

    async fn cancel_if_unchanged(
        &self,
        id: &str,
        expected_status: ReviewStatus,
        expected_updated_at: &str,
        step_results_json: String,
        finished_at: &str,
        updated_at: &str,
    ) -> Result<Option<Review>> {
        match update_status_inner(
            self,
            id,
            ReviewStatus::Cancelled,
            step_results_json,
            Some(finished_at.to_owned()),
            updated_at,
            None,
            None,
            None,
            Some(expected_status),
            Some(expected_updated_at),
            None,
        )
        .await
        {
            Ok((review, _)) => Ok(Some(review)),
            Err(DbError::VersionConflict | DbError::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn update_status_with_task_authority(
        &self,
        id: &str,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: &str,
        expected_task_version: i64,
        review_passed_at: Option<String>,
    ) -> Result<(Review, Task)> {
        let (review, task) = update_status_inner(
            self,
            id,
            status,
            step_results_json,
            finished_at,
            updated_at,
            Some(TaskAuthorityUpdate {
                expected_version: expected_task_version,
                review_passed_at,
            }),
            None,
            None,
            None,
            None,
            None,
        )
        .await?;
        Ok((review, task.ok_or(DbError::NotFound)?))
    }

    async fn update_status_with_task_authority_and_candidate(
        &self,
        id: &str,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: &str,
        expected_task_version: i64,
        review_passed_at: Option<String>,
        expected_candidate_execution_id: &str,
    ) -> Result<(Review, Task)> {
        let (review, task) = update_status_inner(
            self,
            id,
            status,
            step_results_json,
            finished_at,
            updated_at,
            Some(TaskAuthorityUpdate {
                expected_version: expected_task_version,
                review_passed_at,
            }),
            None,
            None,
            None,
            None,
            Some(expected_candidate_execution_id),
        )
        .await?;
        Ok((review, task.ok_or(DbError::NotFound)?))
    }

    async fn update_status_with_task_authority_and_project_candidate(
        &self,
        id: &str,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: &str,
        expected_task_version: i64,
        review_passed_at: Option<String>,
        expected_project_version: Option<i64>,
        expected_workflow_definition: Option<&str>,
        expected_review_status: ReviewStatus,
        expected_review_updated_at: &str,
        expected_candidate_execution_id: &str,
    ) -> Result<(Review, Task)> {
        let (review, task) = update_status_inner(
            self,
            id,
            status,
            step_results_json,
            finished_at,
            updated_at,
            Some(TaskAuthorityUpdate {
                expected_version: expected_task_version,
                review_passed_at,
            }),
            expected_project_version,
            expected_workflow_definition,
            Some(expected_review_status),
            Some(expected_review_updated_at),
            Some(expected_candidate_execution_id),
        )
        .await?;
        Ok((review, task.ok_or(DbError::NotFound)?))
    }

    async fn update_status_with_review_authority(
        &self,
        id: &str,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: &str,
        expected_task_version: i64,
        expected_task_status: &str,
        expected_project_version: Option<i64>,
        expected_workflow_definition: Option<&str>,
        expected_review_status: ReviewStatus,
        expected_review_updated_at: &str,
        expected_candidate_execution_id: Option<&str>,
    ) -> Result<Review> {
        if !matches!(status, ReviewStatus::Running | ReviewStatus::AwaitingHuman) {
            return Err(DbError::Check(
                "review authority updates require a non-terminal Review status".to_owned(),
            ));
        }
        let (review, _) = update_status_with_review_authority_inner(
            self,
            id,
            status,
            step_results_json,
            finished_at,
            updated_at,
            expected_task_version,
            expected_task_status,
            expected_project_version,
            expected_workflow_definition,
            expected_review_status,
            expected_review_updated_at,
            expected_candidate_execution_id,
            None,
        )
        .await?;
        Ok(review)
    }

    async fn update_status_with_review_authority_and_task_projection(
        &self,
        id: &str,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: &str,
        expected_task_version: i64,
        expected_task_status: &str,
        expected_project_version: Option<i64>,
        expected_workflow_definition: Option<&str>,
        expected_review_status: ReviewStatus,
        expected_review_updated_at: &str,
        expected_candidate_execution_id: &str,
        task_projection: Option<Option<String>>,
    ) -> Result<Review> {
        let (review, _) = update_status_with_review_authority_inner(
            self,
            id,
            status,
            step_results_json,
            finished_at,
            updated_at,
            expected_task_version,
            expected_task_status,
            expected_project_version,
            expected_workflow_definition,
            expected_review_status,
            expected_review_updated_at,
            Some(expected_candidate_execution_id),
            task_projection,
        )
        .await?;
        Ok(review)
    }

    async fn get_by_id(&self, id: &str) -> Result<Option<Review>> {
        sqlx::query("SELECT * FROM review WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(map_review)
            .transpose()
    }

    async fn list_by_task(&self, task_id: &str) -> Result<Vec<Review>> {
        let rows = sqlx::query(
            "SELECT * FROM review WHERE task_id = ? ORDER BY attempt_number ASC, id ASC",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(map_review).collect()
    }

    async fn list_latest_reviews_for_tasks(&self, task_ids: &[&str]) -> Result<Vec<Review>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT * FROM (
                SELECT review.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY task_id
                           ORDER BY attempt_number DESC, created_at DESC, id DESC
                       ) AS rn
                FROM review
                WHERE task_id IN (",
        );
        let mut separated = query.separated(", ");
        for task_id in task_ids {
            separated.push_bind(*task_id);
        }
        separated.push_unseparated(
            ")
            ) ranked
            WHERE rn = 1
            ORDER BY task_id ASC",
        );
        let rows = query.build().fetch_all(&self.pool).await?;
        rows.into_iter().map(map_review).collect()
    }

    async fn next_attempt_number(&self, task_id: &str) -> Result<i64> {
        let latest = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(attempt_number) FROM review WHERE task_id = ?",
        )
        .bind(task_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(latest.unwrap_or(0) + 1)
    }
}
