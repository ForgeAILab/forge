//! Narrow reads for CI triage; failed comparison evidence is never a pass.
use super::*;
impl SqliteDb {
    pub async fn ci_triage_consumer(&self, request_key: &str) -> Result<Option<CheckConsumer>> {
        sqlx::query("SELECT * FROM check_consumer WHERE request_key=?")
            .bind(request_key)
            .fetch_optional(self.pool())
            .await?
            .map(map_consumer)
            .transpose()
    }
    pub async fn ci_triage_child_consumers(
        &self,
        consumer: &CheckConsumer,
    ) -> Result<Vec<CheckConsumer>> {
        let Some((prefix, authority)) = consumer.request_key.rsplit_once('|') else {
            return Ok(Vec::new());
        };
        let keys = [
            format!("{prefix}|ci-triage:rerun:{authority}"),
            format!("{prefix}|ci-triage:base:{authority}"),
        ];
        sqlx::query("SELECT * FROM check_consumer WHERE request_key IN (?,?) AND cancelled_at IS NULL AND result_id IS NULL")
            .bind(&keys[0]).bind(&keys[1]).fetch_all(self.pool()).await?.into_iter().map(map_consumer).collect()
    }
    /// An unchanged queue head must still check a Review that passed by triage.
    pub async fn review_ci_passed_by_triage(&self, task: &str) -> Result<bool> {
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM json_each((SELECT step_results_json FROM review WHERE task_id=? AND status='passed' ORDER BY attempt_number DESC,id DESC LIMIT 1),'$.ci_steps') s WHERE json_extract(s.value,'$.exit_code')<>0 AND json_extract(s.value,'$.triage.mode')='full' AND json_extract(s.value,'$.triage.blocking')=0)")
            .bind(task).fetch_one(self.pool()).await?)
    }
}
