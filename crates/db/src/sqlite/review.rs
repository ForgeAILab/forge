use super::*;
use crate::new_uuid_v4;

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

    async fn update_status(
        &self,
        id: &str,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: &str,
    ) -> Result<Review> {
        let mut transaction = crate::begin_immediate(&self.pool).await?;
        let review = sqlx::query("SELECT * FROM review WHERE id = ?")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
            .map(map_review)
            .transpose()?
            .ok_or(DbError::NotFound)?;
        if !review_transition_allowed(&review.status, &status) {
            return Err(DbError::InvalidTransition);
        }
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

        let task_project_id =
            sqlx::query_scalar::<_, String>("SELECT project_id FROM task WHERE id = ?")
                .bind(&review.task_id)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(DbError::NotFound)?;
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
        DomainEventRepo::append_event_in_tx(self, &mut transaction, &event).await?;

        let updated_row = sqlx::query("SELECT * FROM review WHERE id = ?")
            .bind(id)
            .fetch_one(&mut *transaction)
            .await?;
        let updated = map_review(updated_row)?;
        transaction.commit().await?;
        Ok(updated)
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
