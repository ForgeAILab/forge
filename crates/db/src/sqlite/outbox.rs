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
            "SELECT * FROM worker_dead_letter WHERE dead_lettered_at >= ",
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
            cursor.updated_at, health.subscription_json FROM consumers
            LEFT JOIN event_consumer_cursor AS cursor USING (consumer_name)
            LEFT JOIN worker_health AS health ON health.worker_name = consumers.consumer_name ORDER BY consumer_name");
        let rows = query.build().fetch_all(&self.pool).await?;
        let mut result = Vec::with_capacity(rows.len());
        // Legacy consumers retain their global sequence-distance metric.
        let head = self.domain_event_head().await?;
        for row in rows {
            let cursor: i64 = row.try_get("last_sequence")?;
            let subscription: Option<String> = row.try_get("subscription_json")?;
            let (lag, oldest_unprocessed_at) = if let Some(subscription) = subscription {
                let subscription = serde_json::from_str(&subscription)
                    .map_err(|_| DbError::Check("invalid worker subscription".into()))?;
                self.subscribed_backlog(cursor, &subscription).await?
            } else {
                let oldest = sqlx::query_scalar("SELECT created_at FROM domain_event WHERE sequence > ? ORDER BY julianday(created_at), sequence LIMIT 1")
                    .bind(cursor).fetch_optional(&self.pool).await?;
                ((head - cursor).max(0), oldest)
            };
            result.push(DomainEventConsumerLag {
                consumer_name: row.try_get("consumer_name")?,
                last_sequence: cursor,
                lag,
                last_advanced_at: row.try_get("updated_at")?,
                oldest_unprocessed_at,
            });
        }
        Ok(result)
    }
}
