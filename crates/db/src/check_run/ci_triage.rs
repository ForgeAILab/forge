//! Narrow reads for CI triage; failed comparison evidence is never a pass.
use super::*;
impl SqliteDb {
    /// Only this head's outstanding triage work extends its check wait. The
    /// check worker owns execution deadlines; expired runs cannot hold it.
    pub async fn integration_head_triage_pending(
        &self,
        task: &str,
        epoch: i64,
        authority: &str,
        now: &str,
    ) -> Result<bool> {
        let prefix = format!("task-step/1|integration|{task}|{epoch}|ci-triage:");
        let keys = [
            format!("{prefix}rerun:{authority}"),
            format!("{prefix}base:{authority}"),
        ];
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM check_consumer c JOIN check_run r ON r.id=c.run_id WHERE c.request_key IN (?,?) AND c.cancelled_at IS NULL AND c.result_id IS NULL AND r.state IN ('queued','running','cancelling','cleaning','uncertain') AND (r.deadline_at IS NULL OR julianday(r.deadline_at)>julianday(?)))")
            .bind(&keys[0]).bind(&keys[1]).bind(now).fetch_one(self.pool()).await?)
    }

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
