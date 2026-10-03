use super::*;
use crate::{DomainEventConsumerLag, HealthErrorKind};

pub struct WorkerDiagnostic {
    pub worker_name: String,
    pub errors: Vec<(HealthErrorKind, String, String)>,
    pub deferred_reason: Option<String>,
    pub deferred_since: Option<String>,
}
pub struct WorkerDeadLetterIssue {
    pub worker_name: String,
    pub source_key: String,
    pub item_type: String,
    pub reason: String,
    pub occurred_at: String,
}

impl SqliteDb {
    pub async fn worker_operator_diagnostics(
        &self,
        consumers: &[&str],
    ) -> Result<Vec<WorkerDiagnostic>> {
        let mut query =
            sqlx::QueryBuilder::<Sqlite>::new("SELECT * FROM worker_health WHERE worker_name IN (");
        let mut separated = query.separated(", ");
        for name in consumers {
            separated.push_bind(*name);
        }
        separated.push_unseparated(") ORDER BY worker_name");
        query
            .build()
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                let mut errors = Vec::new();
                for (kind, message, time) in [
                    (
                        HealthErrorKind::Runtime,
                        "runtime_error",
                        "runtime_error_at",
                    ),
                    (HealthErrorKind::Item, "item_error", "item_error_at"),
                    (HealthErrorKind::Tick, "tick_error", "tick_error_at"),
                    (
                        HealthErrorKind::AfterCommit,
                        "after_commit_error",
                        "after_commit_error_at",
                    ),
                ] {
                    if let Some(message) = row.try_get::<Option<String>, _>(message)? {
                        errors.push((
                            kind,
                            message,
                            row.try_get::<Option<String>, _>(time)?
                                .unwrap_or_else(crate::now_rfc3339),
                        ));
                    }
                }
                Ok(WorkerDiagnostic {
                    worker_name: row.try_get("worker_name")?,
                    errors,
                    deferred_reason: row.try_get("deferred_reason")?,
                    deferred_since: row.try_get("deferred_since")?,
                })
            })
            .collect()
    }
    pub async fn worker_dead_letter_issues(
        &self,
        consumers: &[&str],
        cutoff: &str,
    ) -> Result<Vec<WorkerDeadLetterIssue>> {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT * FROM worker_dead_letter WHERE resolved_at IS NULL AND dead_lettered_at >= ",
        );
        query.push_bind(cutoff).push(" AND worker_name IN (");
        let mut separated = query.separated(", ");
        for name in consumers {
            separated.push_bind(*name);
        }
        separated.push_unseparated(") ORDER BY dead_lettered_at DESC, worker_name, source_key");
        query
            .build()
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                Ok(WorkerDeadLetterIssue {
                    worker_name: row.try_get("worker_name")?,
                    source_key: row.try_get("source_key")?,
                    item_type: row.try_get("item_type")?,
                    reason: row.try_get("last_error")?,
                    occurred_at: row.try_get("dead_lettered_at")?,
                })
            })
            .collect()
    }
    pub async fn worker_dead_letter_history(
        &self,
        name: &str,
    ) -> Result<(i64, Vec<super::dead_letter::DeadLetter>)> {
        let count = sqlx::query_scalar(
            "SELECT COUNT(*) FROM worker_dead_letter WHERE worker_name = ? AND resolved_at IS NULL",
        )
        .bind(name)
        .fetch_one(&self.pool)
        .await?;
        let rows = sqlx::query("SELECT * FROM worker_dead_letter WHERE worker_name = ? AND resolved_at IS NULL ORDER BY dead_lettered_at DESC, id DESC LIMIT 5")
            .bind(name).fetch_all(&self.pool).await?;
        let records = rows
            .into_iter()
            .map(super::dead_letter::map_dead_letter)
            .collect::<Result<Vec<_>>>()?;
        Ok((count, records))
    }
    pub async fn domain_event_consumer_lag(
        &self,
        expected_consumers: &[&str],
    ) -> Result<Vec<DomainEventConsumerLag>> {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "WITH consumers(consumer_name) AS (SELECT CAST(NULL AS TEXT) WHERE 0",
        );
        for name in expected_consumers {
            query.push(" UNION SELECT ").push_bind(*name);
        }
        query.push(") SELECT consumers.consumer_name, COALESCE(cursor.last_sequence, 0) AS last_sequence,
            cursor.updated_at, health.subscription_json, health.created_at AS initialized_at FROM consumers
            LEFT JOIN event_consumer_cursor AS cursor USING (consumer_name)
            LEFT JOIN worker_health AS health ON health.worker_name = consumers.consumer_name ORDER BY consumer_name");
        let rows = query.build().fetch_all(&self.pool).await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let cursor: i64 = row.try_get("last_sequence")?;
            let subscription: Option<String> = row.try_get("subscription_json")?;
            let subscription = subscription
                .map(|value| {
                    serde_json::from_str(&value)
                        .map_err(|_| DbError::Check("invalid worker subscription".into()))
                })
                .transpose()?
                .unwrap_or(EventSubscription::All);
            let (lag, oldest_unprocessed_at) =
                self.subscribed_backlog(cursor, &subscription).await?;
            result.push(DomainEventConsumerLag {
                consumer_name: row.try_get("consumer_name")?,
                last_sequence: cursor,
                initialized_at: row.try_get("initialized_at")?,
                lag,
                last_advanced_at: row.try_get("updated_at")?,
                oldest_unprocessed_at,
            });
        }
        Ok(result)
    }
}
