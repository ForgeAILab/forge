//! Review progress, repair, and result comment publication.

use super::*;

impl TaskService {
    pub(crate) async fn publish_finished_review_comment(
        &self,
        review: &Review,
        execution: Option<&Execution>,
    ) -> Result<()> {
        if !matches!(
            review.status,
            ReviewStatus::Passed | ReviewStatus::Failed | ReviewStatus::AwaitingHuman
        ) {
            return Ok(());
        }
        let details = strict_review_details(review)?;
        let mut conformance: api_types::ReviewConformance = details
            .get("conformance")
            .filter(|value| !value.is_null())
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
            .unwrap_or_default();
        if conformance.reason.is_none() {
            conformance.reason = details
                .pointer("/auditor/reason")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    details["ci_steps"].as_array()?.iter().find_map(|step| {
                        (step["exit_code"].as_i64()? != 0)
                            .then(|| format!("CI step {} failed", step["index"]))
                    })
                });
        }
        let ledger = self.review_comment_ledger(review).await?;
        let content = reviewer_comment(
            review.status.clone(),
            review.attempt_number,
            &conformance,
            &ledger,
            details.get("verify"),
        );
        self.create_review_comment(
            review,
            "result",
            execution.and_then(|execution| execution.agent_id.as_deref()),
            content,
        )
        .await
    }

    pub(super) async fn review_comment_ledger(
        &self,
        review: &Review,
    ) -> Result<Vec<api_types::ReviewFindingEntry>> {
        let window = self.db.review_window(&review.task_id).await?;
        let reviews: Vec<_> = window
            .reviews
            .into_iter()
            .filter(|row| row.attempt_number <= review.attempt_number)
            .collect();
        // A comment never decides routing: a Review in the window the ledger
        // cannot read (an older details shape) leaves the list out of the
        // comment instead of stopping the cascade that posts it.
        let result = (|| {
            let ledger = db::review_findings::finding_ledger(&reviews)?;
            let prior: Vec<_> = reviews
                .iter()
                .filter(|row| row.attempt_number < review.attempt_number)
                .cloned()
                .collect();
            let previous = db::review_findings::finding_ledger(&prior)?;
            Ok::<_, String>(current_comment_findings(ledger, &previous))
        })();
        Ok(result.unwrap_or_else(|error| {
            tracing::warn!(task_id = %review.task_id, review_id = %review.id, %error,
                "review comment posted without its finding ledger");
            Vec::new()
        }))
    }

    pub(crate) async fn publish_review_repair_comment(
        &self,
        review: &Review,
        spent: i64,
        limit: i32,
    ) -> Result<()> {
        let ledger = self.review_comment_ledger(review).await?;
        let content = repair_round_comment(
            review.attempt_number,
            spent,
            i64::from(limit),
            &ledger,
            "review",
        );
        self.create_review_comment(review, "repair", None, content)
            .await
    }

    /// `entry_step` is the review entry that sent the Task back: a replay of
    /// that entry posts nothing new, a later entry judged against the same
    /// failed Review posts its own comment.
    pub(crate) async fn publish_review_no_change_comment(
        &self,
        review: &Review,
        entry_step: &str,
    ) -> Result<()> {
        let kind = db::budget::Kind::RepairNoChange;
        let spent = db::budget::spent(self.db.pool(), &review.task_id, kind.key()).await?;
        let limit = i64::from(kind.default_limit());
        self.create_review_comment(
            review,
            &format!("{}:{entry_step}", kind.key()),
            None,
            format!(
                "No change since review {}: HEAD still matches the failed candidate and the worktree is clean. Sent back to the coder (repair_no_change budget {spent} of {limit} spent; {} remaining). See the Review tab.",
                review.attempt_number,
                (limit - spent).max(0),
            ),
        )
        .await
    }

    pub(super) async fn publish_review_progress_comment(
        &self,
        review: &Review,
        task: &Task,
        config: Option<&Value>,
    ) -> Result<()> {
        let kind = db::budget::Kind::ReviewProgress;
        let spent = db::budget::spent(self.db.pool(), &task.id, kind.key()).await?;
        let limit = db::budget::limit(task, kind, config, None)?;
        let ledger = self.review_comment_ledger(review).await?;
        let content = repair_round_comment(
            review.attempt_number,
            spent,
            i64::from(limit),
            &ledger,
            kind.key(),
        );
        self.create_review_comment(review, "repair", None, content)
            .await
    }

    pub(crate) async fn publish_review_park_comment(
        &self,
        review: &Review,
        rule: &str,
    ) -> Result<()> {
        self.publish_review_park_comment_as(review, rule, "park")
            .await
    }

    /// A park comment under its own idempotency `kind`. The no-change park
    /// keys it by the review entry that parked, so parking the same failed
    /// Review again (after an owner send-back) is reported once more.
    pub(crate) async fn publish_review_park_comment_as(
        &self,
        review: &Review,
        rule: &str,
        kind: &str,
    ) -> Result<()> {
        let ledger = self.review_comment_ledger(review).await?;
        let ids = finding_ids(&ledger, |entry| entry.status.is_open());
        self.create_review_comment(review, kind, None, format!(
            "Task parked after review attempt {}: {rule}. Open findings: {ids}. Owner actions: re-run review, mark reviewed with a reason, or cancel the Task. See the Review tab.", review.attempt_number
        )).await
    }

    pub(super) async fn publish_reviewer_comment(
        &self,
        execution: &Execution,
        task_id: &str,
        content: String,
    ) -> Result<()> {
        if let Some(agent_id) = execution.agent_id.as_deref() {
            self.create_agent_comment(task_id, agent_id, content).await
        } else {
            self.create_system_comment(task_id, content).await
        }
    }

    pub(super) async fn derived_review_rules_apply(&self, task: &Task) -> Result<bool> {
        let project = ProjectRepo::get_by_id(&*self.db, &task.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        Ok(
            crate::task_hierarchy::coordination_root_lead(&self.db, task, &workflow)
                .await?
                .is_none(),
        )
    }
}
