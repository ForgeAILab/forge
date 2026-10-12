//! Re-plan of a coordination root that has a lead (root lead, stage 2).
//!
//! Two triggers send such a root back to its planning state, where its lead
//! runs again: a rejected aggregate review (`review_failure_target`), and a
//! current subtask that parked needing a decision (the dispatcher's
//! `Step::ReplanRoot`). Each trigger charges the `replan` budget once, under
//! a receipt that names it, and enters planning with a `replan` bridge that
//! carries the same receipt. When the budget is spent the root is blocked
//! with a typed `budget_exhausted{replan}` park instead.
use super::*;

/// The blocker text of a root whose re-plan budget is spent.
pub(crate) const REPLAN_EXHAUSTED_REASON: &str = "re-plan budget exhausted";
/// The interruption source the condition mapper reads the budget kind from.
pub(crate) const REPLAN_BLOCK_SOURCE: &str = "replan";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplanCharge {
    /// This call spent one re-plan.
    Charged,
    /// An earlier attempt at the same trigger already spent it.
    Recorded,
    /// Nothing is left and this trigger holds no receipt.
    Exhausted,
}

impl TaskService {
    /// Spend one re-plan of `root` for `receipt`, exactly once per receipt:
    /// a retry after a crash between this charge and the planning entry
    /// finds the receipt and spends nothing.
    pub(crate) async fn charge_replan(&self, root: &Task, receipt: &str) -> Result<ReplanCharge> {
        let key = db::budget::Kind::Replan.key();
        if db::budget::receipt_recorded(self.db.pool(), &root.id, key, receipt).await? {
            return Ok(ReplanCharge::Recorded);
        }
        let limit = i64::from(db::budget::limit(
            root,
            db::budget::Kind::Replan,
            None,
            None,
        )?);
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let charge = db::budget::charge(&mut tx, &root.id, key, limit, receipt).await?;
        tx.commit().await?;
        if charge.charged {
            return Ok(ReplanCharge::Charged);
        }
        // Lost to a concurrent attempt at the same trigger, or spent.
        Ok(
            if db::budget::receipt_recorded(self.db.pool(), &root.id, key, receipt).await? {
                ReplanCharge::Recorded
            } else {
                ReplanCharge::Exhausted
            },
        )
    }

    /// Park `root` where it is: its lead may not plan again in this budget
    /// window. The retry with a fresh budget on the root is the way on.
    /// Acting on the subtask alone does not release the root: a blocked
    /// root holds its subtasks (`parent { cause: blocked }`).
    pub(crate) async fn block_replan_exhausted(&self, root: &Task, trigger: &str) -> Result<Task> {
        let message = format!(
            "{REPLAN_EXHAUSTED_REASON}: the lead of this root already re-planned {} times; {trigger}",
            db::budget::Kind::Replan.default_limit()
        );
        let blocked_meta = json!({
            "reason": REPLAN_EXHAUSTED_REASON,
            "created_at": now_rfc3339(),
            "kind": api_types::FailureKind::RetryExhausted,
            "source": REPLAN_BLOCK_SOURCE,
            "execution_id": null,
        });
        let annotation = json!({
            "type": api_types::FailureKind::RetryExhausted,
            "blocking_reason": REPLAN_EXHAUSTED_REASON,
            "message": message,
            "detected_at": now_rfc3339(),
        });
        let task = TaskRepo::update(
            &*self.db,
            db::UpdateTask {
                id: root.id.clone(),
                expected_version: root.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: Some(Some(annotation.to_string())),
                blocked_json: Some(Some(blocked_meta.to_string())),
                failed_json: Some(None),
                task_state_config: None,
                parent_task_id: None,
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        tracing::info!(task_id = %task.id, %trigger, "re-plan budget exhausted, blocking coordination root");
        self.publish(ForgeEvent {
            event_type: "task.blocked".to_owned(),
            entity_id: task.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::TaskBlocked {
                project_id: task.project_id.clone(),
                reason: REPLAN_EXHAUSTED_REASON.to_owned(),
                kind: Some(api_types::FailureKind::RetryExhausted),
                source: Some(REPLAN_BLOCK_SOURCE.to_owned()),
                execution_id: None,
            },
        });
        Ok(task)
    }

    /// The receipt of the re-plan a rejected aggregate review of `root`
    /// spends: the failed Review, or the review entry when the entry failed
    /// before any Review existed.
    pub(crate) async fn review_replan_receipt(&self, root: &Task) -> Result<(String, String)> {
        let review = ReviewRepo::list_latest_reviews_for_tasks(&*self.db, &[root.id.as_str()])
            .await?
            .into_iter()
            .next()
            .filter(|review| review.status == ReviewStatus::Failed);
        let subject = match review {
            Some(review) => review.id,
            None => TransitionLogRepo::list_by_task(&*self.db, &root.id)
                .await?
                .into_iter()
                .rev()
                .find(|entry| entry.to_state == root.status)
                .map(|entry| format!("entry:{}", entry.id))
                .unwrap_or_else(|| format!("entry:{}", root.status)),
        };
        Ok((
            crate::task_hierarchy::replan_receipt_for_review(&subject),
            subject,
        ))
    }

    /// The bridge of an automatic review rejection of `task` into `target`:
    /// the `replan` bridge when `target` is where only a coordination root
    /// with a lead is sent (`review_failure_target` decided that), and none
    /// otherwise.
    pub(crate) async fn review_rejection_bridge(
        &self,
        task: &Task,
        target: &str,
        reason: &str,
    ) -> Result<api_types::TransitionBridge> {
        if task.parent_task_id.is_some() {
            return Ok(Default::default());
        }
        let Some(project) = ProjectRepo::get_by_id(&*self.db, &task.project_id).await? else {
            return Ok(Default::default());
        };
        let workflow = WorkflowEngine::resolve_workflow_for_task(
            task,
            &project.workflow_definition,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
        );
        // The lead was read once, by `review_failure_target`, when it chose
        // `target` and charged the re-plan. A target that only a lead root
        // gets is that decision; reading the assignment again here could
        // disagree with it (the lead removed in between) and leave a charged
        // re-plan with an entry the engine refuses. A root that lost its lead
        // still enters planning and leaves it at once through the gate's own
        // unassigned-role cascade.
        if crate::task_hierarchy::effective_reject_target(&workflow, &task.status, false) == target
            || crate::task_hierarchy::effective_reject_target(&workflow, &task.status, true)
                != target
        {
            return Ok(Default::default());
        }
        let (receipt, subject) = self.review_replan_receipt(task).await?;
        Ok(crate::task_hierarchy::replan_bridge(
            "aggregate_review",
            &subject,
            &receipt,
            reason,
        ))
    }

    /// Send coordination root `root_id` back to its planning state because
    /// subtask `child_id` parked needing a decision (`park` identifies that
    /// park). Re-derives the fact from current rows: a root that was held,
    /// blocked, moved, lost its lead, or already re-planned for this park is
    /// left alone. Returns whether anything was written.
    pub(crate) async fn replan_coordination_root(
        &self,
        root_id: &str,
        child_id: &str,
        park: &str,
    ) -> Result<bool> {
        let arguments = || json!([root_id, child_id, park]);
        if db::task_writer::current_task_step().is_some_and(|step| step.task_id != root_id) {
            self.enqueue_task_command(root_id, "replan_coordination_root", arguments(), false)
                .await?;
            return Ok(false);
        }
        if !db::task_writer::owns_task(root_id) {
            return self
                .request_task_command(root_id, "replan_coordination_root", arguments(), false)
                .await;
        }
        let root = TaskRepo::get_by_id(&*self.db, root_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", root_id.to_owned()))?;
        let project = ProjectRepo::get_by_id(&*self.db, &root.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", root.project_id.clone()))?;
        if project.paused_at.is_some() {
            return Ok(false);
        }
        let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
        let policy = crate::task_hierarchy::RootRolePolicy::for_workflow(&workflow);
        let children = crate::task_hierarchy::ordered_children(&self.db, root_id).await?;
        let assignments = TaskRoleAssignmentRepo::list_by_task(&*self.db, root_id).await?;
        let transitions = TransitionLogRepo::list_by_task(&*self.db, root_id).await?;
        let due = policy.replan_due(&root, &children, &assignments, &transitions);
        if due
            .as_ref()
            .map(|(child, park)| (child.as_str(), park.as_str()))
            != Some((child_id, park))
        {
            return Ok(false);
        }
        let Some(planning) = policy.planning_state().map(str::to_owned) else {
            return Ok(false);
        };
        let receipt = crate::task_hierarchy::replan_receipt_for_subtask(child_id, park);
        let child = children.iter().find(|child| child.id == child_id);
        let title = child.map(|child| child.title.as_str()).unwrap_or(child_id);
        if self.charge_replan(&root, &receipt).await? == ReplanCharge::Exhausted {
            let root = TaskRepo::get_by_id(&*self.db, root_id, false)
                .await?
                .ok_or_else(|| ServiceError::not_found("task", root_id.to_owned()))?;
            self.block_replan_exhausted(
                &root,
                &format!("subtask \"{title}\" is parked and needs a decision"),
            )
            .await?;
            return Ok(true);
        }
        // The charge may have restated the root's condition.
        let root = TaskRepo::get_by_id(&*self.db, root_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", root_id.to_owned()))?;
        let reason = format!("subtask \"{title}\" parked needing a decision; the lead re-plans");
        Box::pin(self.transition(
            root.id.clone(),
            planning,
            TransitionOptions {
                bridge: crate::task_hierarchy::replan_bridge(
                    "subtask_parked",
                    child_id,
                    &receipt,
                    &reason,
                ),
                version: root.version,
                reason: Some(reason.clone()),
                triggered_by: api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        ))
        .await?;
        Ok(true)
    }
}
