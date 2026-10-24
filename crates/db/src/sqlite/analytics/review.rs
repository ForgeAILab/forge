//! Project review statistics queries.

use super::*;

#[async_trait]
impl ProjectAnalyticsRepo for SqliteDb {
    async fn get_project_ci_analytics(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<CiStepStats>> {
        let rows = list_review_step_results_json(self, project_id, from, to).await?;

        let mut by_command: BTreeMap<String, StepAggregate> = BTreeMap::new();
        for step_results_json in rows {
            let step_results = parse_step_results(&step_results_json);
            for step in step_results {
                let StepResult {
                    command,
                    exit_code,
                    started_at,
                    finished_at,
                } = step;
                let entry = by_command.entry(command).or_default();
                entry.total_runs = entry
                    .total_runs
                    .checked_add(1)
                    .ok_or(DbError::InvalidTransition)?;
                if exit_code == 0 {
                    entry.pass_count = entry
                        .pass_count
                        .checked_add(1)
                        .ok_or(DbError::InvalidTransition)?;
                } else {
                    entry.fail_count = entry
                        .fail_count
                        .checked_add(1)
                        .ok_or(DbError::InvalidTransition)?;
                }

                if let Some(finished_at) = finished_at.as_deref() {
                    let should_update = entry
                        .last_run_at
                        .as_deref()
                        .map(|current| finished_at > current)
                        .unwrap_or(true);
                    if should_update {
                        entry.last_run_at = Some(finished_at.to_owned());
                    }
                }

                if let (Some(started_at), Some(finished_at)) =
                    (started_at.as_deref(), finished_at.as_deref())
                {
                    if let Some(duration_ms) = parse_duration_ms(started_at, finished_at) {
                        entry.duration_ms.push(duration_ms);
                    }
                }
            }
        }

        by_command
            .into_iter()
            .map(|(command, mut aggregate)| -> Result<CiStepStats> {
                aggregate.duration_ms.sort_unstable();
                let len = aggregate.duration_ms.len();
                let avg_duration_ms = if len > 0 {
                    let total = aggregate
                        .duration_ms
                        .iter()
                        .try_fold(0_i64, |total, value| total.checked_add(*value))
                        .ok_or(DbError::InvalidTransition)?;
                    let len = i64::try_from(len).map_err(|_| DbError::InvalidTransition)?;
                    Some(total / len)
                } else {
                    None
                };
                let p50_duration_ms = if len > 0 {
                    Some(aggregate.duration_ms[len / 2])
                } else {
                    None
                };
                let p95_duration_ms = if len > 0 {
                    let p95_index = len
                        .checked_mul(95)
                        .and_then(|value| value.checked_div(100))
                        .ok_or(DbError::InvalidTransition)?;
                    Some(aggregate.duration_ms[p95_index])
                } else {
                    None
                };

                Ok(CiStepStats {
                    command,
                    total_runs: aggregate.total_runs,
                    pass_count: aggregate.pass_count,
                    fail_count: aggregate.fail_count,
                    avg_duration_ms,
                    p50_duration_ms,
                    p95_duration_ms,
                    last_run_at: aggregate.last_run_at,
                })
            })
            .collect()
    }

    async fn get_project_review_summary(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<ProjectReviewSummary> {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT \
                COUNT(*) AS total_reviews, \
                COALESCE(SUM(CASE WHEN r.status = 'passed' THEN 1 ELSE 0 END), 0) AS passed, \
                COALESCE(SUM(CASE WHEN r.status = 'failed' THEN 1 ELSE 0 END), 0) AS failed, \
                COALESCE(SUM(CASE WHEN r.status = 'cancelled' THEN 1 ELSE 0 END), 0) AS cancelled, \
                AVG(CASE \
                    WHEN r.started_at IS NOT NULL AND r.finished_at IS NOT NULL \
                    THEN CAST((JULIANDAY(r.finished_at) - JULIANDAY(r.started_at)) * 86400000 AS INTEGER) \
                    ELSE NULL \
                END) AS avg_duration_ms \
             FROM review r \
             JOIN execution e ON r.execution_id = e.id \
             JOIN task t ON e.task_id = t.id \
             WHERE t.project_id = ",
        );
        query.push_bind(project_id);
        if let Some(from) = from {
            query
                .push(" AND julianday(r.started_at) >= julianday(")
                .push_bind(from)
                .push(")");
        }
        if let Some(to) = to {
            query
                .push(" AND julianday(r.started_at) < julianday(")
                .push_bind(to)
                .push(")");
        }

        let row = query.build().fetch_one(self.pool()).await?;
        let total_reviews: i64 = row.try_get("total_reviews")?;
        let passed: i64 = row.try_get("passed")?;
        let failed: i64 = row.try_get("failed")?;
        let cancelled: i64 = row.try_get("cancelled")?;
        let avg_duration_ms: Option<i64> = row
            .try_get::<Option<f64>, _>("avg_duration_ms")?
            .map(|value| {
                if value.is_finite() && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
                    Ok(value as i64)
                } else {
                    Err(DbError::InvalidTransition)
                }
            })
            .transpose()?;
        let pass_rate = if total_reviews > 0 {
            passed as f64 / total_reviews as f64
        } else {
            0.0
        };

        Ok(ProjectReviewSummary {
            total_reviews,
            passed,
            failed,
            cancelled,
            avg_duration_ms,
            pass_rate,
        })
    }
}
