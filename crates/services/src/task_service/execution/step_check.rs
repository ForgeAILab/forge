//! Settlement of a step check: the lead's run on a subtask in the `lead_check`
//! gate of the inherited subtask workflow.
//!
//! The run owes one thing, a result block. Only the block's parser is shared
//! with reviews: there is no `Review` row, no check run, no Review budget and
//! no integration evidence here. `pass` takes the gate's Accept trigger;
//! `fail` takes its Reject trigger as a rejection (which charges the gate's
//! own rejection budget) with the lead's findings as the reason the worker is
//! resumed with; a reply without a usable verdict goes through the bounded
//! completion-guard path (the same session is asked again, and the Task is
//! blocked with a visible failure when that budget is spent).
use super::*;

/// The guard name a step check without a usable verdict is recorded under.
pub(crate) const STEP_CHECK_RESULT_GUARD: &str = "step_check_result";

/// Bound on the findings kept in the rejection reason and the comment.
const FINDINGS_LIMIT: usize = 6_000;

fn bounded_findings(text: &str) -> &str {
    if text.len() <= FINDINGS_LIMIT {
        return text;
    }
    let mut end = FINDINGS_LIMIT;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// What the worker is told when a step is returned: the one-sentence reason,
/// then the lead's findings.
fn returned_step_reason(reason: &str, report: &str) -> String {
    let reason = reason.trim();
    let report = bounded_findings(report.trim());
    let mut text = if reason.is_empty() {
        "The lead returned this step.".to_owned()
    } else {
        format!("The lead returned this step: {reason}")
    };
    if !report.is_empty() {
        text.push_str("\n\n");
        text.push_str(report);
    }
    text
}

impl TaskService {
    /// Whether `execution` on `task` is a step check: the step-check role of
    /// a subtask governed by the inherited workflow, resting in a gate.
    pub(super) fn is_step_check(
        task: &Task,
        execution: &Execution,
        current_state: &api_types::StateDefinition,
    ) -> bool {
        current_state.kind == api_types::StateKind::Gate
            && crate::task_hierarchy::step_check_root(task, &execution.role).is_some()
    }

    /// Settle a completed step check that still owns the current role attempt
    /// (the caller checked the role, the attempt, the state entry and the
    /// Project revision).
    pub(super) async fn settle_step_check(
        &self,
        task: &Task,
        execution: &Execution,
        workflow: &api_types::WorkflowDefinition,
        current_state: &api_types::StateDefinition,
    ) -> Result<()> {
        // The lead writes no plan. A private stage a brokered run left behind
        // is dropped, never published.
        self.discard_brokered_execution_plan_stage(execution)
            .await?;
        let message = super::cascade::reviewer_final_message(execution).await?;
        let (accept, reason, report) = match ::review::contract::parse_assessment(&message) {
            Ok(assessment) => match assessment.result {
                api_types::ReviewResult::Pass => (true, assessment.reason, assessment.report),
                api_types::ReviewResult::Fail => (false, assessment.reason, assessment.report),
                api_types::ReviewResult::Blocked => {
                    return self
                        .handle_executor_completion_guard_rejection(
                            execution,
                            task,
                            current_state,
                            STEP_CHECK_RESULT_GUARD,
                            &format!(
                                "the step check must end in a pass or fail verdict, and reported it could not reach one: {}",
                                assessment.reason
                            ),
                        )
                        .await;
                }
            },
            Err(problem) => {
                return self
                    .handle_executor_completion_guard_rejection(
                        execution,
                        task,
                        current_state,
                        STEP_CHECK_RESULT_GUARD,
                        &format!("the step check has no usable result block: {problem}"),
                    )
                    .await;
            }
        };
        let target = if accept {
            workflow
                .auto_transition_target(&task.status)
                .map(str::to_owned)
        } else {
            workflow.gate_reject_target(&task.status).map(str::to_owned)
        };
        let Some(target) = target else {
            return self
                .handle_executor_completion_guard_rejection(
                    execution,
                    task,
                    current_state,
                    STEP_CHECK_RESULT_GUARD,
                    "the step-check gate has no target for this verdict",
                )
                .await;
        };
        let reason = if accept {
            let reason = reason.trim();
            if reason.is_empty() {
                "The lead accepted this step.".to_owned()
            } else {
                format!("The lead accepted this step: {reason}")
            }
        } else {
            returned_step_reason(&reason, &report)
        };
        // The verdict on the subtask's record, once per run: a replay after a
        // crash finds the same comment.
        let author = match execution.agent_id.as_deref() {
            Some(agent_id) => AgentRepo::get_by_id(&*self.db, agent_id).await?,
            None => None,
        };
        let now = now_rfc3339();
        let comment = TaskCommentRepo::create_comment(
            &*self.db,
            CreateTaskComment {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                author_type: if author.is_some() {
                    CommentAuthorType::Agent
                } else {
                    CommentAuthorType::System
                },
                author_id: author.as_ref().map(|agent| agent.id.clone()),
                author_name: author
                    .as_ref()
                    .map(|agent| agent.name.clone())
                    .unwrap_or_else(|| "Forge".to_owned()),
                content: reason.clone(),
                execution_id: None,
                role: None,
                worklog_kind: None,
                idempotency_key: Some(format!("step-check:{}", execution.id)),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await?;
        self.publish(ForgeEvent {
            event_type: "comment.created".to_owned(),
            entity_id: comment.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::CommentCreated {
                task_id: task.id.clone(),
                comment_id: comment.id.clone(),
                author_type: comment.author_type.to_string(),
                author_name: comment.author_name.clone(),
            },
        });

        let from = task.status.clone();
        let options = TransitionOptions {
            bridge: if accept {
                Default::default()
            } else {
                api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateRejected)
            },
            version: task.version,
            reason: Some(reason),
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Workflow),
            rejection: !accept,
            defer_dispatch_seconds: None,
        };
        match self
            .transition(task.id.clone(), target.clone(), options)
            .await
        {
            Ok(_) => {
                if let Err(error) = self.clear_workflow_guard_retry_metadata(task).await {
                    tracing::warn!(task_id = %task.id, %error, "failed to clear workflow guard retry metadata");
                }
                self.publish(ForgeEvent {
                    event_type: "task.auto_transitioned".to_owned(),
                    entity_id: task.id.clone(),
                    timestamp: event_timestamp(),
                    context: EventContext::TaskAutoTransitioned {
                        task_id: task.id.clone(),
                        from,
                        to: target,
                        reason: if accept {
                            "step_check_passed".to_owned()
                        } else {
                            "step_check_failed".to_owned()
                        },
                    },
                });
                Ok(())
            }
            // Something else moved the subtask; the next pass settles from
            // what is stored (the run no longer owns a moved entry).
            Err(ServiceError::Db(DbError::VersionConflict)) => {
                tracing::warn!(task_id = %task.id, "step-check settlement version conflict");
                Ok(())
            }
            Err(ServiceError::GuardRejection { guard, reason }) => {
                self.handle_executor_completion_guard_rejection(
                    execution,
                    task,
                    current_state,
                    &guard,
                    &reason,
                )
                .await
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_returned_step_carries_the_reason_and_bounded_findings() {
        assert_eq!(
            returned_step_reason(" the last row is dropped ", "  Details.  "),
            "The lead returned this step: the last row is dropped\n\nDetails."
        );
        assert_eq!(returned_step_reason("", ""), "The lead returned this step.");
        let long = "é".repeat(FINDINGS_LIMIT);
        let text = returned_step_reason("r", &long);
        assert!(text.len() <= FINDINGS_LIMIT + 64, "{}", text.len());
    }
}
