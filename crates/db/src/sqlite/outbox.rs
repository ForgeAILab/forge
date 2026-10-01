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
            ") SELECT consumers.consumer_name, COALESCE(cursor.last_sequence, 0) AS last_sequence,
                cursor.updated_at,
                MAX(0, COALESCE((SELECT MAX(sequence) FROM domain_event), 0) - COALESCE(cursor.last_sequence, 0)) AS lag,
                (SELECT created_at FROM domain_event WHERE sequence > COALESCE(cursor.last_sequence, 0)
                 ORDER BY julianday(created_at), sequence LIMIT 1) AS oldest_unprocessed_at
             FROM consumers LEFT JOIN event_consumer_cursor AS cursor USING (consumer_name)
             ORDER BY consumers.consumer_name"
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
