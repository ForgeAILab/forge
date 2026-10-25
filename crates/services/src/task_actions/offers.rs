//! Task action offer derivation and audience filtering.

use super::*;

pub(super) fn derive_offers(snapshot: &TaskSnapshot) -> Vec<Offer> {
    use ActionAuthority::{Owner, ProjectAgent};
    let task = &snapshot.task;
    let condition_read = task.condition.read();
    let workflow = &snapshot.workflow;
    let Some(state) = workflow
        .states
        .iter()
        .find(|state| state.name == task.status)
    else {
        return Vec::new();
    };
    let in_review = db::budget::is_review_state(state);
    let mut offers = Vec::new();
    let running = snapshot.executions.iter().any(|execution| {
        execution.status == ExecutionStatus::Running && execution.role != "interactive"
    });
    let role = crate::workflow::effective_role(state);
    let retry_role = crate::workflow::action_role(workflow, &task.status);
    let retry_role_assigned = retry_role.is_some_and(|role| {
        let role = if role == "executor" { "coder" } else { role };
        snapshot
            .role_assignments
            .iter()
            .find(|assignment| assignment.role_name == role)
            .map(|assignment| {
                assignment.assignee_type == Some(db::AssigneeKind::Agent)
                    && assignment.assignee_id.is_some()
            })
            .unwrap_or(task.assignee_type.as_deref() == Some("agent") && task.assignee_id.is_some())
    });
    let current_executions: Vec<_> = snapshot
        .executions
        .iter()
        .filter(|execution| {
            role.is_some_and(|role| {
                role == execution.role || (role == "coder" && execution.role == "executor")
            })
        })
        .collect();
    let latest = current_executions
        .iter()
        .max_by_key(|execution| (&execution.created_at, &execution.id));
    let logical_thread = state.dispatch.as_ref().is_some_and(|dispatch| {
        dispatch.execution_policy
            == Some(api_types::WorkflowExecutionPolicy::ResumeLatestTargetRoleThread)
    });
    let eligible_resume = |execution: &&Execution| {
        let terminal_review = snapshot.latest_review.as_ref().is_some_and(|review| {
            matches!(
                review.status,
                ReviewStatus::Passed | ReviewStatus::Failed | ReviewStatus::Cancelled
            ) && (review.reviewer_execution_id.as_deref() == Some(execution.id.as_str())
                || review.auditor_execution_id.as_deref() == Some(execution.id.as_str())
                || (matches!(execution.role.as_str(), "reviewer" | "auditor")
                    && review.execution_id == execution.id))
        });
        let assigned = snapshot
            .role_assignments
            .iter()
            .find(|assignment| role == Some(assignment.role_name.as_str()))
            .and_then(|assignment| assignment.assignee_id.as_deref())
            .or(snapshot.task.assignee_id.as_deref());
        snapshot.owner_supports_resume
            && (execution.agent_session_id.is_some() || logical_thread)
            && execution.status != ExecutionStatus::Running
            && !terminal_review
            && execution.agent_id.as_deref() == assigned
    };
    let resumable = current_executions
        .iter()
        .copied()
        .any(|execution| eligible_resume(&execution));
    let condition = snapshot.condition();
    let barrier = condition_read.entry.clone();
    let barrier_blocked = barrier
        .as_ref()
        .is_some_and(|barrier| barrier["status"] == "blocked");
    let held = condition == Some(FailureKind::ManualStop);
    let queued = condition_read.queued_command;
    let implementation_candidate = snapshot.executions.iter().any(|execution| {
        execution.status == ExecutionStatus::Completed
            && (execution.task_id == task.id || snapshot.coordination_root)
            && matches!(execution.role.as_str(), "executor" | "coder" | "worker")
    });
    let dependency_cancelled = condition == Some(FailureKind::DependencyCancelled)
        || (condition == Some(FailureKind::WorkflowGuardRejected)
            && snapshot
                .annotation()
                .is_some_and(|annotation| annotation.blocking_reason == "dependency_cancelled"));
    let manual_review_candidate = snapshot.latest_review.as_ref().is_some_and(|review| {
        review.status == ReviewStatus::Failed
            && snapshot.executions.iter().any(|execution| {
                execution.id == review.execution_id
                    && (execution.task_id == task.id || snapshot.coordination_root)
                    && execution.status == ExecutionStatus::Completed
                    && !matches!(
                        execution.role.as_str(),
                        "interactive" | "reviewer" | "auditor"
                    )
            })
    });
    let failed_review = snapshot
        .latest_review
        .as_ref()
        .is_some_and(|review| review.status == ReviewStatus::Failed);
    // While the entry's hook step runs, the latest Review may belong to an
    // earlier entry; nothing waits for a human until its checks settle.
    let entry_hooks_running = snapshot.entry_hooks_running;
    let awaiting_human = !entry_hooks_running
        && (snapshot
            .latest_review
            .as_ref()
            .is_some_and(|review| review.status == ReviewStatus::AwaitingHuman)
            || (condition_read.explicit_human_wait
                && (!in_review || snapshot.latest_review.is_none()))
            || (in_review
                && snapshot.latest_review.is_none()
                && !running
                && state.gate_config.as_ref().is_some_and(|gate| {
                    gate.requires_user_approval()
                        && (!gate.optional_when_unassigned()
                            || role.is_some_and(|role| {
                                snapshot.role_assignments.iter().any(|assignment| {
                                    assignment.role_name == role && assignment.assignee_id.is_some()
                                })
                            }))
                })));
    let target = |trigger| {
        workflow
            .outgoing_trigger_targets(&task.status)
            .find(|(candidate, _)| *candidate == trigger)
            .map(|(_, target)| target)
    };
    let retry_gate = workflow.states.iter().find(|gate| {
        gate.kind == StateKind::Gate
            && (gate.name == task.status
                || gate
                    .gate_config
                    .as_ref()
                    .and_then(|config| config.reject_target.as_deref())
                    == Some(task.status.as_str()))
            && (db::budget::is_review_state(gate)
                || gate
                    .gate_config
                    .as_ref()
                    .and_then(|config| config.max_rejections)
                    .is_some())
    });
    let gate_budget = |gate: &api_types::StateDefinition| {
        let kind = if db::budget::is_review_state(gate) {
            db::budget::Kind::Review
        } else {
            db::budget::Kind::GateRejection
        };
        let mut origin = task.clone();
        origin.status = gate.name.clone();
        db::budget::limit(&origin, kind, Some(&gate.config), gate.gate_config.as_ref()).ok()
    };
    let gate_exhausted = retry_gate.is_some_and(|gate| {
        gate_budget(gate).is_some_and(|budget| {
            db::budget::gate_entry_exhausted(
                db::budget::is_review_state(gate),
                i64::from(budget),
                *snapshot
                    .budget_spent
                    .get(&db::budget::gate_budget_key(gate))
                    .unwrap_or(&0),
            )
        })
    });
    let exhausted = gate_exhausted
        || condition.is_some_and(|kind| {
            kind.is_budget_exhausted_annotation() || kind.is_retry_exhausted_metadata()
        });
    // A subtask resting in its step check with that gate's budget spent is
    // parked by the gate's own entry (`check_retry_budget` writes
    // `retry_exhausted` there): its recovery is a new budget window and a
    // new entry, not another run under the spent one. Every other gate keeps
    // the recovery its condition names.
    let step_gate_exhausted = gate_exhausted
        && task.parent_task_id.is_some()
        && task.status == crate::workflow::default_states::LEAD_CHECK;
    let blocked_execution_id = snapshot
        .annotation()
        .and_then(|annotation| annotation.blocked_execution_id);
    let pinned_resume = current_executions
        .iter()
        .copied()
        .filter(eligible_resume)
        .max_by_key(|execution| {
            (
                blocked_execution_id.as_deref() == Some(execution.id.as_str()),
                &execution.created_at,
                &execution.id,
            )
        });
    let one_shot_allowed = snapshot.caller.owner
        && snapshot.wait_cause.is_none()
        && retry_gate.is_some_and(|gate| {
            db::budget::is_review_state(gate)
                && (gate_budget(gate).is_some_and(|budget| {
                    !db::budget::allows_retry(
                        i64::from(budget),
                        *snapshot
                            .budget_spent
                            .get(&db::budget::gate_budget_key(gate))
                            .unwrap_or(&0),
                    )
                }) || snapshot.annotation().is_some_and(|annotation| {
                    crate::task_diagnostics::is_retry_budget_exhausted(&annotation)
                }))
        });
    let mut offer = |action: TaskAction,
                     parameters: &[&str],
                     authority: &[ActionAuthority],
                     reason: &str,
                     label: &str| {
        if snapshot.caller.permits(authority)
            && (snapshot.wait_cause.is_none()
                || action.verb() != "retry"
                || matches!(
                    reason,
                    "retry_budget_exhausted"
                        | "execution_retry_exhausted"
                        | "check_infrastructure_exhausted"
                ))
            && (!in_review
                || action.verb() != "retry"
                || implementation_candidate
                || matches!(
                    reason,
                    "entry_barrier_blocked"
                        | "state_hooks_failed"
                        | "placement_retry"
                        | "owner_reconcile"
                        | "retry_budget_exhausted"
                        | "execution_retry_exhausted"
                        | "check_infrastructure_exhausted"
                ))
        {
            offers.push(Offer {
                action: action.clone(),
                parameters: parameters
                    .iter()
                    .copied()
                    .chain(
                        matches!(
                            action,
                            TaskAction::Approve { .. }
                                | TaskAction::Cancel { .. }
                                | TaskAction::Retry { .. }
                                | TaskAction::Hold { .. }
                                | TaskAction::Release { .. }
                                | TaskAction::Restart { .. }
                        )
                        .then_some("reason"),
                    )
                    .collect::<Vec<_>>()
                    .iter()
                    .map(|name| api_types::ActionParameter {
                        name: (*name).to_owned(),
                        required: (*name == "guidance"
                            && matches!(action, TaskAction::SendBack { .. }))
                            || (*name == "reason"
                                && (matches!(
                                    action,
                                    TaskAction::Approve {
                                        override_checks: Some(true),
                                        ..
                                    }
                                ) || reason == "review_needs_owner"
                                    || (snapshot.caller.project_agent
                                        && matches!(
                                            action,
                                            TaskAction::Cancel { .. }
                                                | TaskAction::Retry { .. }
                                                | TaskAction::Restart { .. }
                                        )))),
                        required_when: (*name == "reason"
                            && reason == "retry_budget_exhausted"
                            && one_shot_allowed
                            && !awaiting_human)
                            .then(|| api_types::ActionParameterRequirement {
                                parameter: "reset_budget".to_owned(),
                                value: false,
                            }),
                        boolean_values: match *name {
                            "guidance" | "reason" => None,
                            "reset_budget" => Some(
                                if reason == "retry_budget_exhausted"
                                    && one_shot_allowed
                                    && !awaiting_human
                                {
                                    vec![false, true]
                                } else {
                                    vec![true]
                                },
                            ),
                            "override" if reason != "review_needs_owner" => Some(vec![true]),
                            _ => Some(vec![false, true]),
                        },
                    })
                    .collect(),
                authority: authority.to_vec(),
                reason: reason.to_owned(),
                label: label.to_owned(),
                propagates: matches!(action, TaskAction::Cancel { .. })
                    && snapshot.coordination_root,
                target_execution_id: if matches!(
                    action,
                    TaskAction::Retry { .. } | TaskAction::Release { .. }
                ) && resumable
                {
                    pinned_resume.map(|execution| execution.id.clone())
                } else {
                    None
                },
            });
        }
    };
    if state.kind == StateKind::Terminal || task.archived_at.is_some() {
        return offers;
    }
    if workflow.cancellation_state.is_some()
        || workflow
            .states
            .iter()
            .any(|state| state.name == "cancelled" && state.kind == StateKind::Terminal)
    {
        offer(
            TaskAction::Cancel { reason: None },
            &[],
            &[Owner, ProjectAgent],
            "cancellable",
            "Cancel Task",
        );
    }
    // A coordination root has no implementation execution to repair an
    // ejected aggregate branch. Its owner can reopen the working state,
    // held, and add a corrective subtask through the existing send-back path.
    if snapshot.coordination_root
        && !held
        && !running
        && state.kind == StateKind::Gate
        && target(WorkflowTrigger::Reject)
            .or_else(|| workflow.gate_reject_target(&task.status).map(str::to_owned))
            .is_some_and(|target| workflow.state_kind(&target) == Some(StateKind::Active))
        && !(exhausted && in_review)
        && matches!(
            task.condition.integration_wait(),
            Some(api_types::IntegrationReason::Repair { .. })
        )
    {
        offer(
            TaskAction::SendBack {
                guidance: String::new(),
            },
            &["guidance"],
            &[Owner],
            "root_review_reopen",
            "Reopen for a Corrective Subtask",
        );
    }
    if let db::TaskCondition::Parked {
        primary: db::ParkReason::Integration { reason },
        ..
    } = &task.condition
    {
        // A queue/phase wait is owned work, never an agent retry or a manual
        // decision.
        offer(
            TaskAction::Hold { reason: None },
            &[],
            &[Owner],
            "integration_wait",
            "Hold Task",
        );
        // A merge that stopped (`deferred`) always offers the action that
        // puts the Task back in line: the queue worker retries what it can
        // by itself, and this is the exit of every wait it cannot end.
        if integration_retry_offered(task, reason.attempt_id().as_str()) {
            offer(
                TaskAction::Retry {
                    reason: None,
                    fresh_session: None,
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: None,
                },
                &[],
                &[Owner, ProjectAgent],
                "integration_retry",
                "Retry Merge",
            );
        }
        return offers;
    }
    if let db::TaskCondition::Parked {
        primary: db::ParkReason::Check { wait },
        ..
    } = &task.condition
    {
        // Waiting for a result or a slot is the check runner's own work:
        // nothing to retry, approve or relaunch (Cancel is offered above).
        // Exhausted infrastructure retries are no verdict on the candidate:
        // the owner asks again, with a fresh infrastructure budget.
        if wait.requires_intervention() {
            offer(
                TaskAction::Retry {
                    reason: None,
                    fresh_session: None,
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: None,
                },
                &[],
                &[Owner, ProjectAgent],
                "check_infrastructure_exhausted",
                "Retry Check",
            );
        }
        return offers;
    }
    // A running Task is held through its execution (below); a dispatch-wait
    // Hold only applies to work that is waiting to be dispatched. A Task
    // whose entry hooks have not finished is not that: its hooks step waits
    // (for a check, or for free disk to make its worktree again), a hold
    // would supersede that step, and the release has no entry to resume: the
    // Task would sit in its state with its entry never run (a review with no
    // check and no attempt, offered for approval). It offers `cancel` only.
    let running_live = running && !snapshot.owner_disconnected;
    if !held
        && !running_live
        && (queued
            || (condition_read.explicit_human_wait
                && !snapshot.entry_hooks_running
                && task.condition.reasons().any(|reason| {
                    matches!(
                        reason,
                        db::ParkReason::HumanDecision {
                            boundary: db::HumanBoundary::LeadInput,
                            ..
                        }
                    )
                }))
            || (snapshot.wait_cause.is_some()
                && condition.is_none()
                && !condition_read.hard_failure
                && !snapshot.entry_hooks_running))
    {
        offer(
            TaskAction::Hold { reason: None },
            &[],
            &[Owner],
            "dispatch_wait",
            "Hold Task",
        );
    }
    if dependency_cancelled {
        return offers;
    }
    // Nothing was delivered for this review, so the reviewer has no
    // candidate and a reviewer retry would park again. The decision is the
    // owner's, exactly as before the park existed: approve the gate, send
    // the Task back to add work, or cancel it (offered above).
    if in_review
        && condition == Some(FailureKind::ReviewBlocked)
        && snapshot.annotation().is_some_and(|annotation| {
            annotation.blocking_reason == crate::task_service::MISSING_REVIEW_CANDIDATE
        })
    {
        if target(WorkflowTrigger::Accept).is_some() {
            offer(
                TaskAction::Approve {
                    reason: None,
                    override_checks: Some(false),
                },
                &[],
                &[Owner],
                "gate_waiting_for_decision",
                state
                    .gate_config
                    .as_ref()
                    .and_then(|gate| gate.approve_label.as_deref())
                    .unwrap_or("Approve"),
            );
        }
        if target(WorkflowTrigger::Reject).is_some() {
            offer(
                TaskAction::SendBack {
                    guidance: String::new(),
                },
                &["guidance"],
                &[Owner],
                if snapshot.coordination_root {
                    "root_review_reopen"
                } else {
                    "gate_can_reject"
                },
                "Send Back to Add Work",
            );
        }
        append_owner_advance(snapshot, &mut offers);
        return offers;
    }
    if in_review
        && condition == Some(FailureKind::ReviewBlocked)
        && task
            .blocked_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .is_some_and(|value| value["source"] == "verify")
    {
        offer(
            TaskAction::retry(),
            &[],
            &[Owner],
            "review_verify_retry",
            "Retry Verify",
        );
        offer(
            TaskAction::Approve {
                reason: None,
                override_checks: Some(true),
            },
            &["reason"],
            &[Owner],
            "reviewer_failed_manual_pass",
            "Mark Reviewed",
        );
        return offers;
    }
    if condition_read.hard_failure {
        if workflow
            .states
            .iter()
            .any(|state| state.kind == StateKind::Initial)
        {
            offer(
                TaskAction::Restart { reason: None },
                &[],
                &[Owner, ProjectAgent],
                "hard_failure",
                "Restart Task",
            );
        }
        return offers;
    }
    if running_live {
        if !held {
            offer(
                TaskAction::Hold { reason: None },
                &[],
                &[Owner],
                "execution_running",
                "Hold Task",
            );
        }
        append_owner_advance(snapshot, &mut offers);
        return offers;
    }
    if snapshot.owner_disconnected && condition != Some(FailureKind::WorkspaceResetRequired) {
        offer(
            TaskAction::Retry {
                reason: None,
                fresh_session: Some(true),
                refresh_workspace: None,
                reset_budget: None,
                guidance: None,
            },
            &["guidance"],
            &[Owner, ProjectAgent],
            "owner_reconcile",
            "Retry on Workspace Owner",
        );
        return offers;
    }
    let decision_ready = condition.is_none()
        && (awaiting_human
            || (state.kind == StateKind::Active
                && latest.is_some_and(|execution| execution.status == ExecutionStatus::Completed))
            || (in_review
                && (manual_review_candidate || target(WorkflowTrigger::Accept).is_some()))
            || (task.status == "planning" && snapshot.planning_approval_ready));
    let placement_wait = snapshot.wait_cause.is_some() && !decision_ready
        || condition.is_none()
            && ((snapshot.project_paused && !decision_ready)
                || condition_read.environment_recorded
                || condition_read
                    .retry_kind
                    .as_deref()
                    .is_some_and(|kind| kind.starts_with("environment_"))
                || condition_read
                    .current_refusal(task.version)
                    .is_some_and(|disposition| {
                        matches!(
                            disposition.capability.as_str(),
                            "machine_capacity" | "project_capacity"
                        )
                    }));
    if placement_wait {
        if !queued && snapshot.wait_cause.is_none() && !snapshot.entry_hooks_running {
            offer(
                TaskAction::Hold { reason: None },
                &[],
                &[Owner],
                "dispatch_wait",
                "Hold Task",
            );
        }
        if condition.is_none() {
            return offers;
        }
    }
    if entry_hooks_running || queued {
        return offers;
    }
    if matches!(
        condition,
        Some(FailureKind::DispatchFailed | FailureKind::WorkflowLoop | FailureKind::CascadeFailed)
    ) && condition_read.placement_matches_diagnostic
    {
        offer(
            TaskAction::retry(),
            &[],
            &[Owner],
            "placement_retry",
            "Retry Placement",
        );
        if workflow
            .states
            .iter()
            .any(|state| state.kind == StateKind::Initial)
        {
            offer(
                TaskAction::Restart { reason: None },
                &[],
                &[Owner, ProjectAgent],
                "interrupted_restart",
                "Restart Task",
            );
        }
        append_owner_advance(snapshot, &mut offers);
        return offers;
    }
    // A coordination root runs only its aggregate review role. Held in any
    // other state, its release launches nothing: it lets its subtasks run.
    let root_role_refused = snapshot.coordination_root
        && role.is_some_and(|role| {
            !crate::task_hierarchy::RootRolePolicy::for_workflow(workflow).runs_role(
                &task.id,
                &task.status,
                role,
                &snapshot.role_assignments,
            )
        });
    if held {
        if (snapshot.has_agent && !snapshot.project_paused) && role.is_some() && !root_role_refused
        {
            offer(
                TaskAction::Release { reason: None },
                &[],
                &[Owner],
                "manually_held",
                "Release Task",
            );
            offer(
                TaskAction::Retry {
                    reason: None,
                    fresh_session: Some(!resumable),
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: None,
                },
                if resumable {
                    &["fresh_session", "guidance"]
                } else {
                    &["guidance"]
                },
                &[Owner, ProjectAgent],
                "role_retry",
                "Retry Held Task",
            );
        } else {
            // No Agent can take the role right now: releasing returns the Task
            // to the dispatch queue instead of launching it.
            offer(
                TaskAction::Release { reason: None },
                &[],
                &[Owner],
                "held_waiting",
                "Release Task",
            );
        }
        offer(
            TaskAction::Restart { reason: None },
            &[],
            &[Owner, ProjectAgent],
            "interrupted_restart",
            "Restart Task",
        );
        append_owner_advance(snapshot, &mut offers);
        return offers;
    }
    let retry = |fresh_session, refresh_workspace, reset_budget| TaskAction::Retry {
        reason: None,
        fresh_session,
        refresh_workspace,
        reset_budget,
        guidance: None,
    };
    if state.kind != StateKind::Backlog
        && (barrier_blocked
            || matches!(
                condition,
                Some(FailureKind::BeforeWorkHookFailed | FailureKind::BeforeWorkHookTimeout)
            ))
    {
        offer(
            retry(None, Some(false), None),
            &["refresh_workspace", "guidance"],
            &[Owner],
            if barrier_blocked {
                "entry_barrier_blocked"
            } else {
                "state_hooks_failed"
            },
            "Retry Entry Checks",
        );
        offer(
            TaskAction::Approve {
                reason: None,
                override_checks: Some(true),
            },
            &["override"],
            &[Owner],
            if barrier_blocked {
                "entry_barrier_override"
            } else {
                "state_hooks_override"
            },
            "Skip Entry Checks Once",
        );
        if restart_condition(condition) {
            offer(
                TaskAction::Restart { reason: None },
                &[],
                &[Owner, ProjectAgent],
                "interrupted_restart",
                "Restart Task",
            );
        }
        return offers;
    }
    if exhausted {
        if retry_gate.is_none()
            && !(condition == Some(FailureKind::RetryExhausted)
                && role.is_some()
                && (snapshot.has_agent && !snapshot.project_paused))
        {
            if restart_condition(condition)
                && workflow
                    .states
                    .iter()
                    .any(|state| state.kind == StateKind::Initial)
            {
                offer(
                    TaskAction::Restart { reason: None },
                    &[],
                    &[Owner, ProjectAgent],
                    "interrupted_restart",
                    "Restart Task",
                );
            }
            append_owner_advance(snapshot, &mut offers);
            return offers;
        }
        offer(
            retry(None, None, Some(true)),
            &["reset_budget", "guidance"],
            &[Owner, ProjectAgent],
            // A coordination root parked on its spent re-plan budget has no
            // run to retry: the fresh budget is the whole recovery, and the
            // root's own next step (re-plan, or its aggregate review) follows.
            if !step_gate_exhausted
                && condition == Some(FailureKind::RetryExhausted)
                && role.is_some()
                && (snapshot.has_agent && !snapshot.project_paused)
                && !condition_read
                    .interruption
                    .as_ref()
                    .is_some_and(|interruption| {
                        interruption.source.as_deref()
                            == Some(crate::task_service::replan::REPLAN_BLOCK_SOURCE)
                    })
            {
                "execution_retry_exhausted"
            } else {
                "retry_budget_exhausted"
            },
            "Reset Budget and Retry",
        );
        if restart_condition(condition) {
            offer(
                TaskAction::Restart { reason: None },
                &[],
                &[Owner, ProjectAgent],
                "interrupted_restart",
                "Restart Task",
            );
        }
        if in_review && failed_review && manual_review_candidate {
            offer(
                TaskAction::Approve {
                    reason: None,
                    override_checks: Some(true),
                },
                &["override"],
                &[Owner],
                "failed_review_override",
                "Override Failed Review",
            );
        }
        // A parent Task whose aggregate review cannot pass needs corrective
        // work, and a parent in review accepts no new subtask. Sending it
        // back reopens it, held, so a corrective subtask can be added; its
        // release runs that subtask and the aggregate review after it.
        if in_review && snapshot.coordination_root && target(WorkflowTrigger::Reject).is_some() {
            offer(
                TaskAction::SendBack {
                    guidance: String::new(),
                },
                &["guidance"],
                &[Owner],
                "root_review_reopen",
                "Reopen for a Corrective Subtask",
            );
        }
        if !(in_review
            && state
                .gate_config
                .as_ref()
                .is_some_and(|gate| gate.requires_user_approval())
            && !failed_review
            && condition.is_none()
            && awaiting_human)
        {
            append_owner_advance(snapshot, &mut offers);
            return offers;
        }
    }
    if condition_read.external_merge {
        offer(
            retry(None, None, None),
            &[],
            &[Owner],
            "pull_request_merge_wait",
            "Retry Merge",
        );
        append_owner_advance(snapshot, &mut offers);
        return offers;
    }
    let review_wait = state.kind == StateKind::Gate
        && awaiting_human
        && (task.status != "planning" || snapshot.planning_approval_ready);
    let owner_gate = state
        .gate_config
        .as_ref()
        .is_some_and(|gate| gate.requires_user_approval());
    if in_review
        && matches!(
            condition,
            Some(FailureKind::ReviewNeedsOwner | FailureKind::ReviewBlocked)
        )
    {
        if manual_review_candidate && condition == Some(FailureKind::ReviewNeedsOwner) {
            offer(
                TaskAction::Approve {
                    reason: None,
                    override_checks: Some(false),
                },
                &["override"],
                &[Owner],
                "review_needs_owner",
                "Defer Finding to Follow-up",
            );
        } else if manual_review_candidate {
            offer(
                TaskAction::Approve {
                    reason: None,
                    override_checks: Some(true),
                },
                &["override"],
                &[Owner],
                "reviewer_failed_manual_pass",
                "Pass Review Manually",
            );
        }
        if role.is_some() && (snapshot.has_agent && !snapshot.project_paused) {
            offer(
                retry(Some(!resumable), None, None),
                if resumable {
                    &["fresh_session", "guidance"]
                } else {
                    &["guidance"]
                },
                &[Owner, ProjectAgent],
                "role_retry",
                "Retry Review With Guidance",
            );
        } else {
            offer(
                retry(None, None, None),
                &["guidance"],
                &[Owner],
                "review_owner_retry",
                "Retry With Guidance",
            );
        }
    } else if (!in_review || (awaiting_human && condition.is_none()))
        && (review_wait
            || ((in_review || (task.status == "planning" && snapshot.planning_approval_ready))
                && !failed_review
                && target(WorkflowTrigger::Accept).is_some())
            || (state.kind == StateKind::Gate
                && owner_gate
                && (task.status != "planning" || snapshot.planning_approval_ready)
                && target(WorkflowTrigger::Accept).is_some()))
    {
        let authorities = if in_review && owner_gate {
            vec![Owner, ProjectAgent]
        } else {
            vec![Owner]
        };
        offer(
            TaskAction::Approve {
                reason: None,
                override_checks: Some(false),
            },
            &[],
            &authorities,
            if in_review
                && snapshot
                    .latest_review
                    .as_ref()
                    .is_some_and(|review| review.status == ReviewStatus::AwaitingHuman)
                && target(WorkflowTrigger::Accept).as_deref() == Some("merging")
            {
                "human_review_decision"
            } else {
                "gate_waiting_for_decision"
            },
            state
                .gate_config
                .as_ref()
                .and_then(|gate| gate.approve_label.as_deref())
                .unwrap_or("Approve"),
        );
        if one_shot_allowed && !exhausted {
            offer(
                retry(None, None, Some(true)),
                &["reset_budget"],
                &[Owner],
                "retry_budget_exhausted",
                "Reset Budget and Retry",
            );
        }
        if in_review
            && !exhausted
            && !one_shot_allowed
            && implementation_candidate
            && snapshot
                .latest_review
                .as_ref()
                .is_some_and(|review| review.status == ReviewStatus::AwaitingHuman)
        {
            offer(
                retry(None, None, None),
                &[],
                &[Owner],
                "review_checks_retry",
                "Recheck Review",
            );
        }
    } else if in_review
        && state.kind == StateKind::Gate
        && failed_review
        && implementation_candidate
    {
        if condition.is_some() && role.is_some() && (snapshot.has_agent && !snapshot.project_paused)
        {
            offer(
                retry(Some(!resumable), None, None),
                if resumable {
                    &["fresh_session", "guidance"]
                } else {
                    &["guidance"]
                },
                &[Owner, ProjectAgent],
                "role_retry",
                "Retry Review",
            );
        } else {
            offer(
                retry(None, None, None),
                &["guidance"],
                &[Owner],
                "review_failed",
                "Retry Review",
            );
        }
        if in_review && manual_review_candidate {
            offer(
                TaskAction::Approve {
                    reason: None,
                    override_checks: Some(true),
                },
                &["override"],
                &[Owner],
                "failed_review_override",
                "Override Failed Review",
            );
        }
    } else if condition.is_some() {
        let annotation = snapshot.annotation();
        let reason = if task.status == "merging"
            && annotation.as_ref().is_some_and(|annotation| {
                matches!(
                    annotation.blocked_by.as_deref(),
                    Some("coordination_root" | "manual_workspace_repair")
                )
            }) {
            "manual_merge_repair"
        } else if task.status == "merging"
            && (condition == Some(FailureKind::TargetRepoDirty)
                || !condition.is_some_and(FailureKind::is_merge_recoverable))
        {
            "merge_gate_retry"
        } else if task.status == "merging"
            && condition.is_some_and(FailureKind::is_merge_recoverable)
        {
            "merge_retry_remediation"
        } else if task.status == "merge_failed" {
            "merge_fix_retry"
        } else {
            if role.is_some() {
                "role_retry"
            } else {
                "initial_retry"
            }
        };
        if state.kind != StateKind::Backlog
            && (!matches!(reason, "role_retry" | "merge_fix_retry") || retry_role_assigned)
            && !matches!(
                condition,
                Some(
                    FailureKind::BeforeWorkHookFailed
                        | FailureKind::BeforeWorkHookTimeout
                        | FailureKind::MaxTurnsExceeded
                        | FailureKind::WorkspaceResetRequired
                        | FailureKind::DependencyCancelled
                )
            )
            && (!snapshot.coordination_root || state.kind == StateKind::Gate)
            && ((role.is_some() && (snapshot.has_agent && !snapshot.project_paused))
                // A merge fix needs its Agent; a Project pause only delays it.
                || (reason == "merge_fix_retry" && snapshot.has_agent)
                || !matches!(reason, "role_retry" | "initial_retry" | "merge_fix_retry")
                || (state.kind == StateKind::Initial
                    && (snapshot.has_agent && !snapshot.project_paused)
                    && task.status != "backlog"))
        {
            let authorities =
                if matches!(reason, "role_retry" | "merge_fix_retry" | "initial_retry") {
                    vec![Owner, ProjectAgent]
                } else {
                    vec![Owner]
                };
            offer(
                retry(Some(!resumable), None, None),
                if resumable {
                    &["fresh_session", "guidance"]
                } else {
                    &["guidance"]
                },
                &authorities,
                reason,
                "Retry Task",
            );
        }
        if restart_condition(condition)
            && workflow
                .states
                .iter()
                .any(|state| state.kind == StateKind::Initial)
        {
            offer(
                TaskAction::Restart { reason: None },
                &[],
                &[Owner, ProjectAgent],
                "interrupted_restart",
                "Restart Task",
            );
        }
    } else if task.status == "merging" {
        offer(
            retry(None, None, None),
            &[],
            &[Owner],
            "merge_gate_retry",
            "Retry Merge",
        );
    } else if state.kind == StateKind::Initial
        && !snapshot.coordination_root
        && (snapshot.has_agent && !snapshot.project_paused)
        && snapshot.dependencies_satisfied
        // A subtask its parent does not let run cannot be started: the
        // exit is on the parent, which the condition names.
        && !task
            .condition
            .reasons()
            .any(|reason| matches!(reason, db::ParkReason::Parent { .. }))
        && workflow
            .outgoing_trigger_targets(&task.status)
            .any(|(_, target)| {
                matches!(
                    workflow.state_kind(&target),
                    Some(StateKind::Active | StateKind::Gate)
                )
            })
    {
        offer(
            TaskAction::Start,
            &[],
            &[Owner],
            "ready_to_start",
            "Start Task",
        );
    } else if state.kind == StateKind::Active && !snapshot.coordination_root {
        if latest.is_some_and(|execution| {
            execution.status == ExecutionStatus::Completed
                && snapshot
                    .transition_logs
                    .iter()
                    .rfind(|entry| entry.to_state == task.status)
                    .is_none_or(|entry| execution.created_at > entry.created_at)
        }) && target(WorkflowTrigger::Accept).is_some()
        {
            offer(
                TaskAction::Approve {
                    reason: None,
                    override_checks: Some(false),
                },
                &[],
                &[Owner],
                "work_ready_to_submit",
                "Submit Work",
            );
        }
        if snapshot.has_agent && !snapshot.project_paused {
            offer(
                retry(Some(!resumable), None, None),
                if resumable {
                    &["fresh_session", "guidance"]
                } else {
                    &["guidance"]
                },
                &[Owner],
                "role_retry",
                "Retry Task",
            );
        }
    }
    if task.status == "planning"
        && snapshot.has_agent
        && !snapshot.project_paused
        && role.is_some()
        && !offers.iter().any(|offer| offer.action.verb() == "retry")
        && snapshot.caller.owner
    {
        let mut parameters = vec![
            api_types::ActionParameter {
                name: "guidance".to_owned(),
                required: false,
                required_when: None,
                boolean_values: None,
            },
            api_types::ActionParameter {
                name: "reason".to_owned(),
                required: false,
                required_when: None,
                boolean_values: None,
            },
        ];
        if resumable {
            parameters.push(api_types::ActionParameter {
                name: "fresh_session".to_owned(),
                required: false,
                required_when: None,
                boolean_values: Some(vec![false, true]),
            });
        }
        offers.push(Offer {
            action: retry(Some(!resumable), None, None),
            parameters,
            authority: vec![Owner],
            reason: "role_retry".to_owned(),
            label: "Retry Planning".to_owned(),
            target_execution_id: pinned_resume.map(|execution| execution.id.clone()),
            propagates: false,
        });
    }
    if in_review
        && implementation_candidate
        && condition.is_none()
        && !offers.iter().any(|offer| offer.action.verb() == "retry")
    {
        let mut parameters = vec![api_types::ActionParameter {
            name: "reason".to_owned(),
            required: false,
            required_when: None,
            boolean_values: None,
        }];
        parameters.push(api_types::ActionParameter {
            name: "guidance".to_owned(),
            required: false,
            required_when: None,
            boolean_values: None,
        });
        if snapshot.caller.owner {
            offers.push(Offer {
                action: TaskAction::retry(),
                parameters,
                authority: vec![Owner],
                reason: "review_checks_retry".to_owned(),
                label: "Re-run Review".to_owned(),
                target_execution_id: None,
                propagates: false,
            });
        }
    }
    let mut offer = |action: TaskAction,
                     parameters: &[&str],
                     authority: &[ActionAuthority],
                     reason: &str,
                     label: &str| {
        if snapshot.caller.permits(authority) {
            offers.push(Offer {
                action,
                parameters: parameters
                    .iter()
                    .map(|name| api_types::ActionParameter {
                        name: (*name).to_owned(),
                        required: *name == "guidance",
                        required_when: None,
                        boolean_values: None,
                    })
                    .collect(),
                authority: authority.to_vec(),
                reason: reason.to_owned(),
                label: label.to_owned(),
                target_execution_id: None,
                propagates: false,
            });
        }
    };
    if target(WorkflowTrigger::Reject).is_some() && state.kind == StateKind::Gate {
        let authorities = if owner_gate && in_review {
            vec![Owner, ProjectAgent]
        } else {
            vec![Owner]
        };
        offer(
            TaskAction::SendBack {
                guidance: String::new(),
            },
            &["guidance"],
            &authorities,
            if in_review
                && snapshot
                    .latest_review
                    .as_ref()
                    .is_some_and(|review| review.status == ReviewStatus::AwaitingHuman)
                && target(WorkflowTrigger::Reject).as_deref() == Some("in_progress")
            {
                "human_review_send_back"
            } else {
                "gate_can_reject"
            },
            state
                .gate_config
                .as_ref()
                .and_then(|gate| gate.reject_label.as_deref())
                .unwrap_or("Send Back"),
        );
    }
    append_owner_advance(snapshot, &mut offers);
    offers
}

/// The lead's part of a subtask's offers: the verbs the lead may apply now
/// (`ActionCaller::lead_verbs`) name the lead wherever the Project Agent is
/// offered them, and a caller that is only the lead is offered nothing else.
pub(super) fn lead_audience(caller: &ActionCaller, offers: &mut Vec<Offer>) {
    if caller.lead_verbs.is_empty() {
        return;
    }
    for offer in offers.iter_mut() {
        if caller.lead_verbs.contains(&offer.action.verb())
            && offer.authority.contains(&ActionAuthority::ProjectAgent)
            && !offer.authority.contains(&ActionAuthority::Lead)
        {
            offer.authority.push(ActionAuthority::Lead);
        }
    }
    if caller.lead && !caller.owner && !caller.project_agent {
        offers.retain(|offer| offer.authority.contains(&ActionAuthority::Lead));
    }
}

fn append_owner_advance(snapshot: &TaskSnapshot, offers: &mut Vec<Offer>) {
    if !snapshot.caller.owner || snapshot.advance_target.is_none() {
        return;
    }
    if let Some(approval) = offers
        .iter_mut()
        .find(|offer| offer.action.verb() == "approve")
    {
        if matches!(
            approval.action,
            TaskAction::Approve {
                override_checks: Some(false),
                ..
            }
        ) && approval.reason != "review_needs_owner"
        {
            approval.parameters.push(api_types::ActionParameter {
                name: "override".to_owned(),
                required: false,
                boolean_values: Some(vec![false, true]),
                required_when: None,
            });
            if let Some(reason) = approval
                .parameters
                .iter_mut()
                .find(|parameter| parameter.name == "reason")
            {
                reason.required_when = Some(api_types::ActionParameterRequirement {
                    parameter: "override".to_owned(),
                    value: true,
                });
            }
        }
        return;
    }
    offers.push(Offer {
        action: TaskAction::Approve {
            override_checks: Some(true),
            reason: None,
        },
        parameters: vec![
            api_types::ActionParameter {
                name: "override".to_owned(),
                required: false,
                boolean_values: Some(vec![true]),
                required_when: None,
            },
            api_types::ActionParameter {
                name: "reason".to_owned(),
                required: true,
                boolean_values: None,
                required_when: None,
            },
        ],
        authority: vec![ActionAuthority::Owner],
        reason: "state_advance_override".to_owned(),
        // In a merge state the override ends the Task without its merge:
        // the label says so before the owner confirms.
        label: if snapshot.workflow.states.iter().any(|state| {
            state.name == snapshot.task.status
                && state
                    .hooks
                    .on_enter
                    .iter()
                    .any(|hook| hook.action == "run_merge")
        }) {
            "Mark done without merging".to_owned()
        } else {
            "Advance Task".to_owned()
        },
        target_execution_id: None,
        propagates: false,
    });
}

fn restart_condition(condition: Option<FailureKind>) -> bool {
    matches!(
        condition,
        Some(
            FailureKind::DispatchFailed
                | FailureKind::WorkflowLoop
                | FailureKind::CascadeFailed
                | FailureKind::RecoveryRequired
                | FailureKind::ExecutorFailed
                | FailureKind::ExecutorUnavailable
                | FailureKind::WorkspaceFailed
                | FailureKind::WorkspaceError
                | FailureKind::WorkspaceResetRequired
                | FailureKind::ManualStop
                | FailureKind::Unknown
                | FailureKind::RetryExhausted
                | FailureKind::MaxTurnsExceeded
        )
    )
}
