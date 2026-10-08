//! Read once, resolve without I/O. This adapter builds the pure scheduling facts
//! from batched Task, role, execution, Review, hierarchy and entry records.
use super::{helpers, next_step::Facts, TaskDispatcher};
use crate::{
    deferred_dispatch,
    workflow::{effective_role, engine::WorkflowEngine},
};
use api_types::{Actor, StateKind, SystemComponent, WorkflowDefinition};
use chrono::{DateTime, Utc};
use db::{
    AssigneeKind, ExecutionStatus, ParkReason, Project, ScheduleRead, TaskCondition,
    TaskRoleAssignment,
};
use serde_json::Value;

pub(super) struct Prepared {
    pub read: ScheduleRead,
    pub workflow: WorkflowDefinition,
    pub facts: Facts,
    pub deadline: Option<String>,
    /// The Task was refused a machine run slot on this pass.
    pub machine_wait: bool,
    /// The stored condition did not match the legacy fields it shadows.
    pub condition_stale: bool,
}
fn assignment<'a>(r: &'a ScheduleRead, role: &str) -> Option<&'a TaskRoleAssignment> {
    let role = if role == "executor" { "coder" } else { role };
    r.assignments
        .iter()
        .find(|a| a.task_id == r.task.id && a.role_name == role)
        .or_else(|| {
            r.assignments
                .iter()
                .find(|a| role == "coder" && a.role_name == role)
        })
}
fn initial_target(w: &WorkflowDefinition, r: &ScheduleRead) -> Option<(String, String, String)> {
    let mut cursor = r.task.status.as_str();
    let mut seen = std::collections::HashSet::new();
    let mut kinds = vec![StateKind::Active, StateKind::Gate];
    let mut first = None;
    loop {
        if !seen.insert(cursor) {
            return None;
        }
        let state = helpers::first_transition_to_kind(w, cursor, &kinds)?;
        let first = first.get_or_insert_with(|| state.name.clone());
        let role = effective_role(state)?;
        match assignment(r, role) {
            Some(a) if a.assignee_type == Some(AssigneeKind::Agent) && a.assignee_id.is_some() => {
                return Some((first.clone(), role.into(), a.assignee_id.clone()?))
            }
            Some(a) if a.assignee_type == Some(AssigneeKind::User) => return None,
            a if helpers::role_assignment_unassigned(a)
                && helpers::auto_cascades_on_unassigned_role(state) =>
            {
                cursor = &state.name;
                kinds = vec![StateKind::Active];
            }
            _ => return None,
        }
    }
}
/// Whether a reason was read from the entry barrier column.
fn from_barrier(reason: &ParkReason) -> bool {
    let source = match reason {
        ParkReason::EntryBlocked { source, .. }
        | ParkReason::BudgetExhausted { source, .. }
        | ParkReason::UnknownCondition { source, .. } => source,
        _ => return false,
    };
    source.field == db::LegacyConditionField::EntryBarrierJson
}
/// `barrier_holds` is false where the base scheduler never read the entry
/// barrier: admission from an initial state, and a gate that cascades past an
/// unassigned role.
fn blocking(c: &TaskCondition, barrier_holds: bool) -> bool {
    let evidence = match c {
        TaskCondition::Clear { evidence }
        | TaskCondition::Entering { evidence, .. }
        | TaskCondition::Running { evidence, .. }
        | TaskCondition::Deferred { evidence, .. }
        | TaskCondition::Parked { evidence, .. }
        | TaskCondition::Failed { evidence, .. }
        | TaskCondition::Settled { evidence, .. } => evidence,
    };
    if evidence.blocked_json.is_some()
        || evidence.failed_json.is_some()
        || (barrier_holds && evidence.entry_barrier_json.is_some())
    {
        return true;
    }
    // A condition its writer stated carries no legacy copy: the same three
    // holds are its typed presentation.
    if evidence.stated
        && evidence.presentation.as_ref().is_some_and(|read| {
            read.interruption_present || read.hard_failure || (barrier_holds && read.entry_recorded)
        })
    {
        return true;
    }
    match c {
        TaskCondition::Failed { .. } => true,
        TaskCondition::Parked {
            primary,
            additional,
            ..
        } => std::iter::once(primary).chain(additional).any(|p| {
            !matches!(
                p,
                ParkReason::UnknownCondition {
                    source: db::ConditionSource {
                        field: db::LegacyConditionField::SchedulePark,
                        ..
                    },
                    ..
                }
            ) && (barrier_holds || !from_barrier(p))
                && matches!(
                    p,
                    ParkReason::Held { .. }
                        | ParkReason::Failure { .. }
                        | ParkReason::AgentTimeout { .. }
                        | ParkReason::BudgetExhausted { .. }
                        | ParkReason::EntryBlocked { .. }
                        | ParkReason::UnknownCondition { .. }
                )
        }),
        _ => false,
    }
}
fn at(raw: Option<&str>) -> Option<DateTime<Utc>> {
    raw.and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&Utc))
}
fn earlier(deadline: &mut Option<String>, time: Option<DateTime<Utc>>, now: DateTime<Utc>) {
    if let Some(time) = time.filter(|t| *t > now) {
        if deadline
            .as_deref()
            .and_then(|t| at(Some(t)))
            .is_none_or(|old| time < old)
        {
            *deadline = Some(time.to_rfc3339());
        }
    }
}
impl TaskDispatcher {
    pub(super) fn prepare_schedule(
        &self,
        mut r: ScheduleRead,
        project: &Project,
        now: DateTime<Utc>,
    ) -> Prepared {
        // The stored condition shadows the legacy fields, which stay
        // authoritative. When a writer missed its sync the copy is stale but
        // the row read in this snapshot is not: hold exactly where the legacy
        // fields hold, and leave the stored copy to be repaired.
        let stale = |stored: &TaskCondition, task: &db::Task| {
            // A writer stated this condition in the transaction of its write:
            // it is not a copy of the legacy fields and cannot lag them.
            let unreadable = stored.reasons().find_map(|reason| match reason {
                ParkReason::UnknownCondition {
                    source:
                        db::ConditionSource {
                            field: db::LegacyConditionField::ConditionJson,
                            ..
                        },
                    problem,
                } => Some(problem),
                _ => None,
            });
            match unreadable {
                // A newer build's encoding is quarantined: this build neither
                // resolves around it nor repairs it.
                Some(db::UnknownConditionProblem::UnknownKind) => return None,
                // Corrupt or empty: the legacy fields are intact, so resolve
                // from them and let the check restate the stored copy.
                Some(_) => {
                    return Some(db::map_legacy_condition(&db::LegacyConditionInput::from(
                        task,
                    )))
                }
                None => {}
            }
            if stored.evidence().stated {
                return None;
            }
            let legacy = stored.restate_legacy(&db::LegacyConditionInput::from(task));
            let (stored, fresh) = (stored.evidence(), legacy.evidence());
            (stored.error_annotation != fresh.error_annotation
                || stored.blocked_json != fresh.blocked_json
                || stored.failed_json != fresh.failed_json
                || stored.entry_barrier_json != fresh.entry_barrier_json
                || stored.metadata != fresh.metadata
                || stored.unparsed_metadata != fresh.unparsed_metadata)
                .then_some(legacy)
        };
        let legacy = stale(&r.condition, &r.task);
        let condition_stale = legacy.is_some();
        if let Some(legacy) = legacy {
            // Every callee handed this Task reads the same condition this
            // pass resolved from.
            r.task.condition = legacy.clone();
            r.condition = legacy;
        }
        // The coordination root gates its children by its condition: a root
        // whose stored copy is stale holds exactly where its fields hold.
        if let Some(parent) = &mut r.parent {
            if let Some(legacy) = stale(&parent.condition, parent) {
                parent.condition = legacy;
            }
        }
        let t = &r.task;
        let w = WorkflowEngine::resolve_workflow_for_task(
            t,
            &project.workflow_definition,
            &Actor::system(SystemComponent::TaskDispatcher),
        );
        let pw = WorkflowEngine::resolve_workflow(&project.workflow_definition);
        let state = w.states.iter().find(|s| s.name == t.status);
        let role = state.and_then(effective_role);
        let metadata = t
            .metadata_json
            .as_deref()
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .unwrap_or(Value::Null);
        let entry = r.transitions.iter().rev().find(|e| e.to_state == t.status);
        let own = |e: &db::Execution| {
            let config = e
                .executor_config_snapshot_json
                .as_deref()
                .and_then(|s| serde_json::from_str::<Value>(s).ok());
            if let Some(c) = &config {
                if c["task_state"].as_str().is_some_and(|s| s != t.status) {
                    return false;
                }
                if let Some(token) = c.get("state_entry_token") {
                    return match token {
                        Value::Null => entry.is_none(),
                        Value::String(token) => entry.is_some_and(|e| e.id == *token),
                        _ => false,
                    };
                }
            }
            entry.is_none_or(|entry| e.created_at >= entry.created_at)
        };
        let is_initial = w.state_kind(&t.status) == Some(StateKind::Initial);
        // The admission path: the base scheduler transitions these Tasks
        // without reading the entry barrier.
        let admits = is_initial
            || state.is_some_and(|s| {
                s.kind == StateKind::Gate
                    && helpers::auto_cascades_on_unassigned_role(s)
                    && helpers::role_assignment_unassigned(
                        role.and_then(|role| assignment(&r, role)),
                    )
            });
        let mut f = Facts {
            blocking: blocking(&r.condition, !admits),
            paused: project.paused_at.is_some(),
            queue_owned: r.queue_owned,
            child_ready: true,
            root_role_allowed: true,
            reviewer_ready: true,
            ..Facts::default()
        };
        f.refresh_placement = metadata
            .get("placement_refusal")
            .is_some_and(Value::is_object);
        f.publication = metadata.get("plan_publication_claim").is_some()
            || metadata.get("plan_publication_cleanup").is_some();
        f.publication_owned = ["plan_publication_claim", "plan_publication_cleanup"]
            .iter()
            .all(|key| {
                metadata.get(*key).is_none_or(|c| {
                    c["execution_id"].as_str().is_some_and(|id| !id.is_empty())
                        && c["state"].as_str().is_some_and(|s| !s.is_empty())
                        && c["project_version"].is_i64()
                        && (c["state_entry_token"].is_null() || c["state_entry_token"].is_string())
                })
            });
        f.placement_unavailable = r
            .placement_state
            .as_deref()
            .is_some_and(|s| matches!(s, "disconnected" | "cleaning" | "reserved" | "preparing"));
        f.environment_manual = matches!(
            metadata["environment_wait"]["kind"].as_str(),
            Some("environment_unverified" | "provision_failed")
        );
        f.integrate = deferred_dispatch::paused_integration(t).is_some();
        // The key is the fact, as the base scan listed it. An intent that no
        // longer parses still reaches the replay, which blocks the Task with
        // `recovery_required` instead of dispatching past it.
        f.queued_recovery = metadata
            .get(deferred_dispatch::QUEUED_RECOVERY_KEY)
            .is_some_and(|intent| !intent.is_null());
        let mut deadline = None;
        for execution in r
            .executions
            .iter()
            .filter(|e| e.status == ExecutionStatus::Running)
        {
            earlier(
                &mut deadline,
                at(execution.lease_expires_at.as_deref()),
                now,
            );
            earlier(
                &mut deadline,
                at(execution.hard_deadline_at.as_deref()),
                now,
            );
        }
        let retry = deferred_dispatch::pending_until(t).and_then(|d| at(Some(&d.not_before)));
        let environment_ready =
            serde_json::from_str::<api_types::ProjectSettings>(&project.settings).is_ok_and(|s| {
                r.ready_environment_digest.as_deref()
                    == Some(db::environment_checks_digest(&s.environment).as_str())
            });
        f.retry_pending = w.state_kind(&t.status) != Some(StateKind::Initial)
            && !environment_ready
            && retry.is_some_and(|at| at > now)
            && !(r.external
                && (metadata.get("environment_wait").is_some()
                    || metadata.get("owner_wait").is_some()));
        earlier(&mut deadline, retry, now);
        let owner = at(metadata["owner_wait"]["started_at"].as_str()).and_then(|started| {
            chrono::Duration::from_std(self.task_service.owner_wait_timeout())
                .ok()
                .map(|timeout| started + timeout)
        });
        f.owner_expired = matches!(
            w.state_kind(&t.status),
            Some(StateKind::Active | StateKind::Gate)
        ) && metadata["owner_wait"]["daemon_id"].is_string()
            && owner.is_some_and(|at| at <= now)
            && t.blocked_json.is_none();
        earlier(&mut deadline, owner, now);
        f.review_ci_retry = t.entry_barrier_json.is_some()
            && t.error_annotation
                .as_deref()
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
                .is_some_and(|a| a["blocking_reason"] == "review_ci_infrastructure");
        f.root = t.parent_task_id.is_none() && !r.children.is_empty();
        let complete = f.root && crate::task_hierarchy::child_sequence_complete(&r.children, &pw);
        f.root_advance = f.root
            && matches!(
                w.state_kind(&t.status),
                Some(StateKind::Initial | StateKind::Active | StateKind::Gate)
            )
            && (crate::task_service::coordination_review_pending(t)
                || (complete
                    && pw.canonical_phase_for_state(&t.status)
                        != api_types::CanonicalPhase::Review
                    && pw.state_kind(&t.status) != Some(StateKind::Terminal)));
        if let Some(parent) = &r.parent {
            f.child_ready =
                crate::task_hierarchy::coordination_root_allows_child_dispatch(parent, &pw)
                    && crate::task_hierarchy::next_incomplete_child(&r.siblings, &pw)
                        .is_some_and(|c| c.id == t.id);
        }
        if let Some(role) = role {
            f.root_role_allowed = !f.root
                || crate::task_hierarchy::RootRolePolicy::for_workflow(&w)
                    .allows_execution(&t.status, role);
            f.in_flight = r.executions.iter().any(|e| {
                e.status == ExecutionStatus::Running
                    && helpers::execution_guard_roles(role).contains(&e.role.as_str())
            });
            let a = assignment(&r, role);
            f.role_target = a
                .filter(|a| a.assignee_type == Some(AssigneeKind::Agent))
                .and_then(|a| a.assignee_id.as_ref())
                .map(|id| (role.into(), id.clone()));
            if state.is_none_or(|s| {
                !s.hooks
                    .on_enter
                    .iter()
                    .any(|h| h.action == "dispatch_role_agent")
            }) {
                f.role_target = None;
            }
            let latest = r
                .executions
                .iter()
                .rev()
                .find(|e| helpers::execution_guard_roles(role).contains(&e.role.as_str()));
            let assignment_newer = |e: &db::Execution| {
                a.and_then(|a| at(Some(&a.updated_at)))
                    .zip(at(Some(&e.updated_at)))
                    .is_some_and(|(a, e)| a > e)
            };
            let mut no_exact_review = false;
            if let Some(e) = latest.filter(|e| e.status != ExecutionStatus::Running) {
                if role == "reviewer"
                    && crate::task_service::execution_dispatch_project_version(e)
                        == Some(project.version)
                {
                    let exact =
                        crate::task_service::execution::exact_review_for_execution(e, &r.reviews);
                    no_exact_review=exact.is_none_or(|v|crate::task_service::execution::reviewer_execution_lacks_exact_review_binding(e,v)) || exact.zip(r.reviews.last()).is_none_or(|(bound,last)|bound.id!=last.id);
                    if !no_exact_review
                        && matches!(e.resume_policy, None | Some(db::ResumePolicy::Manual))
                        && !assignment_newer(e)
                    {
                        f.terminal_execution = Some(e.id.clone());
                    }
                } else if role != "reviewer" && own(e) && !assignment_newer(e) {
                    let config_version = crate::task_service::execution_dispatch_project_version(e);
                    let receipt = &metadata["terminal_execution_settlement"];
                    let settled = receipt["execution_id"] == e.id
                        && receipt["state"] == t.status
                        && receipt["state_entry_token"].as_str() == entry.map(|e| e.id.as_str());
                    if e.status == ExecutionStatus::Completed
                        && !settled
                        && config_version == Some(project.version)
                    {
                        f.terminal_execution = Some(e.id.clone());
                    }
                    if e.status == ExecutionStatus::Failed
                        && config_version == Some(project.version)
                        && !f.retry_pending
                        && metadata["last_execution_failure_execution_id"] != e.id
                        && !crate::project_environment::is_environment_pre_dispatch_failure(e)
                        && crate::task_service::execution::should_block_task_for_failed_execution(e)
                    {
                        f.terminal_execution = Some(e.id.clone());
                    }
                    if settled && config_version == Some(project.version) {
                        f.human_wait = true;
                    }
                }
                f.stopped_execution = !no_exact_review
                    && matches!(
                        e.status,
                        ExecutionStatus::Failed | ExecutionStatus::Cancelled
                    )
                    && matches!(e.resume_policy, None | Some(db::ResumePolicy::Manual))
                    && !assignment_newer(e)
                    && !crate::project_environment::is_environment_pre_dispatch_failure(e);
            }
            if role != "reviewer" && helpers::awaiting_human(t) {
                let authoritative =
                    state.is_some_and(|s| helpers::awaiting_human_is_authoritative(t, s, role));
                let matches = if metadata["awaiting_human_reason"] != "plan_review" {
                    true
                } else if let Some(token) = metadata.get("planning_state_entry_token") {
                    token.as_str() == entry.map(|e| e.id.as_str())
                        && (token.is_null() || token.is_string())
                } else {
                    metadata["planning_execution_id"]
                        .as_str()
                        .and_then(|id| r.executions.iter().find(|e| e.id == id))
                        .is_some_and(own)
                };
                f.human_wait |= authoritative && matches;
                f.stale_human_wait = !f.human_wait;
            }
            if role == "reviewer" {
                let config = state
                    .map(|s| {
                        helpers::merged_state_config(s, project, t.task_state_config.as_deref())
                    })
                    .unwrap_or(Value::Null);
                let ci = config
                    .get("review")
                    .unwrap_or(&config)
                    .get("ci_steps")
                    .and_then(Value::as_array)
                    .is_some_and(|s| !s.is_empty());
                f.reviewer_ready = crate::execution_setup::classify_task_execution(
                    &t.task_type,
                    r.capability_class.as_deref(),
                )
                .is_ok_and(|c| c.is_read_only())
                    || !ci
                    || r.reviews.last().is_some_and(|v| {
                        v.status == db::ReviewStatus::Running
                            && serde_json::from_str::<Value>(&v.step_results_json)
                                .ok()
                                .is_some_and(|v| {
                                    v["ci_steps"].as_array().is_some_and(|steps| {
                                        !steps.is_empty()
                                            && steps
                                                .iter()
                                                .all(|s| s["exit_code"].as_i64().is_some())
                                    })
                                })
                    });
                if let Some(review) = r
                    .reviews
                    .last()
                    .filter(|v| v.status == db::ReviewStatus::Failed)
                {
                    let grace =
                        at(Some(&review.updated_at)).map(|at| at + chrono::Duration::minutes(2));
                    let eligible = !f.blocking
                        && t.error_annotation.is_none()
                        && !helpers::awaiting_human(t)
                        && t.entry_barrier_json.is_none()
                        && !r
                            .executions
                            .iter()
                            .any(|e| e.status == ExecutionStatus::Running)
                        && r.transitions.last().is_some_and(|e| {
                            e.to_state == t.status
                                && !e.triggered_by.starts_with("user:")
                                && at(Some(&review.started_at))
                                    .zip(at(Some(&e.created_at)))
                                    .is_some_and(|(start, entry)| start >= entry)
                        });
                    f.failed_review = eligible && grace.is_some_and(|at| at <= now);
                    f.review_grace =
                        eligible && !f.reviewer_ready && grace.is_some_and(|at| at > now);
                    earlier(&mut deadline, grace, now);
                }
            }
        }
        if role.is_none() && helpers::awaiting_human(t) {
            f.human_wait = true;
        }
        if admits {
            f.initial_target = initial_target(&w, &r);
        }
        if let Some(held) = role.and_then(|role| assignment(&r, role)) {
            f.role_user = held.assignee_type == Some(AssigneeKind::User);
        }
        f.role_open = role.is_some() && f.role_target.is_none() && !f.role_user;
        f.apply_defaults = serde_json::from_str::<api_types::ProjectSettings>(&project.settings)
            .is_ok_and(|s| !s.default_role_assignments.is_empty())
            && is_initial
            && !r.assignments.iter().any(|a| a.task_id == t.id)
            && assignment(&r, "coder").is_none();
        let capability = if f.root_advance {
            super::initial_scheduling::COORDINATION_ROOT_CAPABILITY
        } else if is_initial {
            f.initial_target
                .as_ref()
                .map(|(_, role, _)| role.as_str())
                .unwrap_or("")
        } else {
            &t.status
        };
        // A recorded refusal holds until the Task's version changes or a wake
        // clears it, exactly as before: no other fact re-opens it.
        f.disposition_current = deferred_dispatch::dispatch_disposition_is_current(t, capability);
        if let Some(last) = r.transitions.last() {
            if last.from_state == "merging"
                && last.to_state == t.status
                && last.bridge.is_review_refresh()
                && last.triggered_by == Actor::system(SystemComponent::Workflow).display()
            {
                f.review_refresh = crate::workflow::review_refresh_target(&w, &t.status);
            }
        }
        f.missing_merge_entry = t.parent_task_id.is_none()
            && !r.entry_hooks_seen
            && state.is_some_and(|s| s.hooks.on_enter.iter().any(|h| h.action == "run_merge"));
        f.unsafe_merge_entry = state.is_some_and(|s| {
            s.hooks
                .on_enter
                .iter()
                .chain(&s.hooks.after_enter)
                .any(|h| {
                    !matches!(
                        h.action.as_str(),
                        "run_merge" | "auto_cascade_on_merge_result"
                    )
                })
        });
        f.merge_witness = r
            .transitions
            .iter()
            .any(|e| e.to_state == t.status && e.from_state != e.to_state);
        Prepared {
            read: r,
            workflow: w,
            facts: f,
            deadline,
            machine_wait: false,
            condition_stale,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn integration_restatement_preserves_lineage_and_new_legacy_holds() {
        for condition in crate::task_actions::tests::integration_conditions() {
            let input = db::LegacyConditionInput { error_annotation: Some(serde_json::json!({"type":"manual_stop","blocking_reason":"new hold","blocked_by":"user"}).to_string()), ..Default::default() };
            let restated = condition.restate_legacy(&input);
            assert_eq!(
                restated.integration_reason(),
                condition.integration_reason()
            );
            assert!(blocking(&restated, true));
            assert!(matches!(
                restated,
                TaskCondition::Parked {
                    primary: ParkReason::Held { .. },
                    ..
                }
            ));
        }
        let unknown = db::task_condition::decode_or_unknown(r#"{"kind":"future"}"#);
        assert!(blocking(&unknown, true));
    }

    /// A condition its writer stated carries no legacy copy; the dispatcher
    /// holds on its typed presentation exactly where it held on the copies.
    #[test]
    fn a_stated_condition_holds_on_its_typed_presentation() {
        for field in ["blocked", "failed", "entry"] {
            let mut read = db::ConditionRead::default();
            match field {
                "blocked" => read.interruption_present = true,
                "failed" => read.hard_failure = true,
                _ => read.entry_recorded = true,
            }
            let mut evidence = db::ConditionEvidence {
                presentation: Some(read),
                stated: true,
                ..Default::default()
            };
            let condition = |evidence: db::ConditionEvidence| TaskCondition::Clear { evidence };
            assert!(blocking(&condition(evidence.clone()), true), "{field}");
            assert_eq!(
                blocking(&condition(evidence.clone()), false),
                field != "entry",
                "{field}: an unread entry record does not hold"
            );
            // The same presentation on a mapped condition is not a hold by
            // itself: there the legacy copies decide, as before.
            evidence.stated = false;
            assert!(!blocking(&condition(evidence), true), "{field}");
        }
    }
    #[test]
    fn legacy_interruption_and_barrier_holds_survive_an_owned_condition() {
        for field in ["blocked", "failed", "entry"] {
            let mut evidence = db::ConditionEvidence::default();
            match field {
                "blocked" => evidence.blocked_json = Some("{}".into()),
                "failed" => evidence.failed_json = Some("{}".into()),
                _ => evidence.entry_barrier_json = Some("{}".into()),
            }
            let condition = TaskCondition::Entering {
                state: "review".into(),
                epoch: 1,
                step_id: "orphaned-step".into(),
                phase: "entry".into(),
                since: "2026-10-06T00:00:00Z".into(),
                evidence,
            };
            assert!(
                blocking(&condition, true),
                "legacy {field} still holds even when the entry owner is missing"
            );
        }
    }
}
