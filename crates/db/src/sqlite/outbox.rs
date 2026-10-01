use super::*;
use crate::DomainEventConsumerLag;

impl SqliteDb {
    pub async fn domain_event_consumer_lag(
        &self,
        expected_consumers: &[&str],
    ) -> Result<Vec<DomainEventConsumerLag>> {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "WITH consumers(consumer_name) AS (SELECT CAST(NULL AS TEXT) WHERE 0",
        );
        for consumer in expected_consumers {
            query.push(" UNION SELECT ").push_bind(*consumer);
        }
        query.push(
            ") SELECT consumers.consumer_name,
                CASE WHEN health.worker_name IS NOT NULL
                     THEN health.cursor_sequence
                     ELSE COALESCE(cursor.last_sequence, 0)
                END AS last_sequence,
                CASE WHEN health.worker_name IS NOT NULL
                     THEN health.cursor_updated_at
                     ELSE cursor.updated_at
                END AS updated_at,
                CASE WHEN health.worker_name IS NOT NULL
                     THEN health.lag
                     ELSE MAX(0, COALESCE((SELECT MAX(sequence) FROM domain_event), 0)
                                      - COALESCE(cursor.last_sequence, 0))
                END AS lag,
                CASE WHEN health.worker_name IS NOT NULL
                     THEN health.oldest_pending_at
                     ELSE (SELECT created_at FROM domain_event
                           WHERE sequence > COALESCE(cursor.last_sequence, 0)
                           ORDER BY julianday(created_at), sequence LIMIT 1)
                END AS oldest_unprocessed_at
             FROM consumers
             LEFT JOIN event_consumer_cursor AS cursor USING (consumer_name)
             LEFT JOIN worker_health AS health
               ON health.worker_name = consumers.consumer_name
             ORDER BY consumers.consumer_name",
        );
        query
            .build()
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                Ok(DomainEventConsumerLag {
                    consumer_name: row.try_get("consumer_name")?,
                    last_sequence: row.try_get("last_sequence")?,
                    lag: row.try_get("lag")?,
                    last_advanced_at: row.try_get("updated_at")?,
                    oldest_unprocessed_at: row.try_get("oldest_unprocessed_at")?,
                })
            })
            .collect()
    }
}
