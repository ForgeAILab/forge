use super::*;
use crate::DomainEventConsumerLag;

impl SqliteDb {
    /// Persistent runtime errors for expected migrated consumers, mapped by
    /// services onto the existing operator issue list.
    pub async fn domain_event_worker_errors(
        &self,
        consumers: &[&str],
    ) -> Result<Vec<(String, String, String)>> {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT worker_name, last_error, COALESCE(last_error_at, updated_at) FROM worker_health
             WHERE last_error IS NOT NULL AND worker_name IN (",
        );
        let mut separated = query.separated(", ");
        for name in consumers {
            separated.push_bind(*name);
        }
        separated.push_unseparated(") ORDER BY worker_name");
        Ok(query.build_query_as().fetch_all(&self.pool).await?)
    }
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
                CASE WHEN health.subscription_json IS NOT NULL
                     THEN CAST(health.cursor_key AS INTEGER)
                     ELSE COALESCE(cursor.last_sequence, 0)
                END AS last_sequence,
                CASE WHEN health.subscription_json IS NOT NULL
                     THEN health.cursor_updated_at
                     ELSE cursor.updated_at
                END AS updated_at,
                CASE WHEN health.subscription_json IS NOT NULL
                     THEN (SELECT COUNT(*) FROM domain_event AS event
                           WHERE event.sequence > CAST(health.cursor_key AS INTEGER)
                             AND (json_extract(health.subscription_json, '$.kind') = 'all'
                               OR EXISTS (SELECT 1 FROM json_each(health.subscription_json, '$.values') AS sub
                                  WHERE (json_extract(health.subscription_json, '$.kind') = 'exact'
                                         AND event.event_type = sub.value)
                                     OR (json_extract(health.subscription_json, '$.kind') = 'prefix'
                                         AND substr(event.event_type, 1, length(sub.value)) = sub.value))))
                     ELSE MAX(0, COALESCE((SELECT MAX(sequence) FROM domain_event), 0)
                                      - COALESCE(cursor.last_sequence, 0))
                END AS lag,
                CASE WHEN health.subscription_json IS NOT NULL
                     THEN (SELECT event.created_at FROM domain_event AS event
                           WHERE event.sequence > CAST(health.cursor_key AS INTEGER)
                             AND (json_extract(health.subscription_json, '$.kind') = 'all'
                               OR EXISTS (SELECT 1 FROM json_each(health.subscription_json, '$.values') AS sub
                                  WHERE (json_extract(health.subscription_json, '$.kind') = 'exact'
                                         AND event.event_type = sub.value)
                                     OR (json_extract(health.subscription_json, '$.kind') = 'prefix'
                                         AND substr(event.event_type, 1, length(sub.value)) = sub.value)))
                           ORDER BY julianday(event.created_at), event.sequence LIMIT 1)
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
