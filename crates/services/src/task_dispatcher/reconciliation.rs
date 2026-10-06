//! One reconciliation loop: dirty generations, exact deadlines and a 120s
//! bounded repair sweep all feed the existing Task command/cascade adapter.
use super::{
    next_step::{self, Action, Next, Owner, Park, Reason, Snapshot, Step},
    snapshot::Prepared,
    TaskDispatcher,
};
use crate::{deferred_dispatch, Result, ServiceError};
use api_types::{Actor, SystemComponent};
use db::{Project, ProjectRepo};
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

#[derive(Debug)]
pub(super) struct ScheduleState {
    pub started: bool,
    pub generation: u64,
    pub sweep_at: Instant,
    pub deadline: Option<Instant>,
    pub repo_retry: bool,
    pub server_cap: Option<Option<i64>>,
}
impl Default for ScheduleState {
    fn default() -> Self {
        Self {
            started: false,
            generation: 0,
            sweep_at: Instant::now(),
            deadline: None,
            repo_retry: false,
            server_cap: None,
        }
    }
}
impl TaskDispatcher {
    pub async fn startup_reconcile(&self) -> Result<u64> {
        self.reconcile_once().await
    }
    pub async fn sweep_and_assert(&self) -> Result<()> {
        self.schedule_state.lock().expect("schedule state").sweep_at = Instant::now();
        self.reconcile_once().await?;
        let violations = self.db.task_schedule_violations().await?;
        if !violations.is_empty() {
            return Err(ServiceError::invalid_operation(format!(
                "unowned Tasks after reconciliation: {violations:?}"
            )));
        }
        Ok(())
    }
    pub(super) async fn reconcile_once(&self) -> Result<u64> {
        let _reconciliation = self.reconcile_lock.lock().await;
        let generation = self.db.schedule_generation();
        let now = Instant::now();
        let jobs_ready = self
            .environment_rechecks
            .lock()
            .expect("environment jobs")
            .values()
            .any(|j| j.is_finished());
        let cap = self.db.server_run_cap.effective();
        let (sweep, first, capacity_changed) = {
            let mut state = self.schedule_state.lock().expect("schedule state");
            let capacity_changed = state.server_cap.is_some_and(|old| old != cap);
            if state.started
                && !capacity_changed
                && state.generation == generation
                && !self.db.schedule_commit_pending()
                && state.sweep_at > now
                && state.deadline.is_none_or(|d| d > now)
                && !jobs_ready
                && !state.repo_retry
            {
                return Ok(0);
            }
            let first = !state.started;
            let sweep = first || state.sweep_at <= now;
            state.started = true;
            state.generation = generation;
            state.server_cap = Some(cap);
            if sweep {
                state.sweep_at = now + Duration::from_secs(120);
            }
            (sweep, first, capacity_changed)
        };
        self.observe_environment_settings();
        let mut ids: HashSet<String> = self
            .db
            .dirty_schedule_tasks(i64::MAX)
            .await?
            .into_iter()
            .collect();
        let capacity_ids: HashSet<String> = if capacity_changed {
            sqlx::query_scalar(
                "SELECT task_id FROM task_schedule_wait WHERE daemon_id IS NULL OR daemon_id=?",
            )
            .bind(self.db.server_run_cap.embedded_machine_id())
            .fetch_all(self.db.pool())
            .await?
            .into_iter()
            .collect()
        } else {
            HashSet::new()
        };
        ids.extend(capacity_ids.iter().cloned());
        ids.extend(self.db.due_schedule_tasks(&db::now_rfc3339()).await?);
        if sweep || !ids.is_empty() {
            crate::placement::admission::sweep_expired_reservations(&self.db, &db::now_rfc3339())
                .await?;
        }
        let environment_changed = self.sync_due_environment_checks().await?;
        if !environment_changed.is_empty() {
            ids.extend(self.db.dirty_schedule_tasks(i64::MAX).await?);
        }
        if sweep {
            self.db.begin_condition_sweep(first).await?;
            // Discover candidates cheaply before admission. Full condition
            // checking remains after the dispatch pass, as on the base.
            ids.extend(
                sqlx::query_scalar::<_, String>("SELECT id FROM task")
                    .fetch_all(self.db.pool())
                    .await?,
            );
        }
        let mut reads = Vec::new();
        let ids: Vec<_> = ids.into_iter().collect();
        for page in ids.chunks(50) {
            reads.extend(self.db.schedule_reads(page).await?);
        }
        for read in &mut reads {
            read.external |= capacity_ids.contains(&read.task.id);
            read.verify = sweep;
        }
        let project_reads = self.db.schedule_projects(sweep).await?;
        let mut projects: HashMap<_, _> = project_reads
            .iter()
            .map(|(p, _)| (p.id.clone(), p.clone()))
            .collect();
        for read in &reads {
            if !projects.contains_key(&read.task.project_id) {
                if let Some(p) = ProjectRepo::get_by_id(&*self.db, &read.task.project_id).await? {
                    projects.insert(p.id.clone(), p);
                }
            }
        }
        let mut skipped = HashSet::new();
        // Repo negative readiness still needs a filesystem probe on the sweep.
        // Positive readiness is memoized and only relevant Projects are loaded.
        for p in projects.values() {
            if self.sync_repository_pause(p).await? {
                skipped.insert(p.id.clone());
            }
        }
        for (p, generation) in &project_reads {
            self.db
                .acknowledge_schedule_project(&p.id, *generation)
                .await?;
        }
        self.schedule_state
            .lock()
            .expect("schedule state")
            .repo_retry = projects
            .values()
            .any(|p| p.system_pause_reason.as_deref() == Some("repository_not_ready"));
        let now = chrono::Utc::now();
        let mut prepared: Vec<_> = reads
            .into_iter()
            .filter_map(|r| {
                projects
                    .get(&r.task.project_id)
                    .map(|p| self.prepare_schedule(r, p, now))
            })
            .collect();
        self.enrich_schedule_agents(&mut prepared).await?;
        prepared.sort_by(|a, b| {
            let (pa, pb) = (
                &projects[&a.read.task.project_id],
                &projects[&b.read.task.project_id],
            );
            let phase = |p: &Prepared| {
                if p.facts.publication {
                    0
                } else if p.facts.queued_recovery {
                    1
                } else if p.workflow.state_kind(&p.read.task.status)
                    == Some(api_types::StateKind::Initial)
                {
                    3
                } else {
                    2
                }
            };
            (pa.created_at.as_str(), pa.id.as_str(), phase(a))
                .cmp(&(pb.created_at.as_str(), pb.id.as_str(), phase(b)))
                .then_with(|| {
                    if phase(a) == 3 {
                        b.read.task.priority.cmp(&a.read.task.priority)
                    } else {
                        std::cmp::Ordering::Equal
                    }
                })
                .then_with(|| {
                    (&a.read.task.created_at, &a.read.task.id)
                        .cmp(&(&b.read.task.created_at, &b.read.task.id))
                })
        });
        let mut count = 0;
        let mut slots = HashMap::new();
        let mut repairs = 0;
        for mut p in prepared {
            if self.is_stopped() {
                break;
            }
            let project = &projects[&p.read.task.project_id];
            if p.read.task.deleted_at.is_some() || p.read.task.archived_at.is_some() {
                self.db
                    .acknowledge_schedule(
                        &p.read.task.id,
                        if p.read.dirty { p.read.generation } else { 0 },
                    )
                    .await?;
                continue;
            }
            if skipped.contains(&project.id) && !p.facts.publication {
                let park = Park {
                    reason: Reason::ProjectPaused,
                    owner: Owner::User,
                    recovery: Action::ResumeProject,
                };
                self.db
                    .record_schedule_park(
                        &p.read.task.id,
                        p.read.epoch,
                        &serde_json::to_string(&park).expect("park serializes"),
                    )
                    .await?;
                continue;
            }
            let unowned = sweep
                && !self
                    .db
                    .schedule_has_owner(&p.read.task.id, p.read.epoch)
                    .await?
                && p.workflow.state_kind(&p.read.task.status)
                    != Some(api_types::StateKind::Terminal);
            if unowned {
                repairs += 1;
                tracing::warn!(task_id=%p.read.task.id, startup=first,"scheduler invariant repaired by reconciliation");
            }
            // Default assignment and stale approval cleanup can expose the same
            // admission candidate in this pass, preserving the old fairness.
            for _ in 0..3 {
                let next = next_step::next_step(&Snapshot {
                    state: &p.read.task.status,
                    condition: &p.read.condition,
                    workflow: &p.workflow,
                    facts: &p.facts,
                });
                let mut result_park = None;
                match &next {
                    Next::Park(park) => result_park = Some(park.clone()),
                    Next::Step(step) => {
                        tracing::debug!(task_id=%p.read.task.id, queue_identity=%step.identity(), "resolved Task step");
                        if matches!(step, Step::Initial { .. }) {
                            if !slots.contains_key(&project.id) {
                                slots.insert(
                                    project.id.clone(),
                                    match super::slots::load_project_slots(&self.db, project).await
                                    {
                                        Ok(slots) => slots,
                                        Err(error) => {
                                            self.handle_schedule_error(&p, step, &error).await?;
                                            break;
                                        }
                                    },
                                );
                            }
                            let slot = slots.get_mut(&project.id).expect("project slots");
                            if slot.limit > 0
                                && (slot.active >= slot.limit || slot.parked >= 2 * slot.limit)
                            {
                                let message = if slot.active >= slot.limit {
                                    format!(
                                        "project_at_capacity: waiting for a slot ({}/{} active)",
                                        slot.active, slot.limit
                                    )
                                } else {
                                    format!("project_waiting_on_owner: {} parked tasks waiting on the owner",slot.parked)
                                };
                                deferred_dispatch::record_dispatch_disposition(
                                    &self.db,
                                    &p.read.task,
                                    "project_capacity",
                                    &message,
                                )
                                .await?;
                                result_park = Some(Park {
                                    reason: Reason::Capacity,
                                    owner: Owner::Scheduler,
                                    recovery: Action::FreeCapacity,
                                });
                            }
                        }
                        if result_park.is_none() {
                            match self.apply_schedule_step(&p, project, step).await {
                                Ok(n) => {
                                    count += n;
                                    if n > 0 && matches!(step, Step::Initial { .. }) {
                                        slots.get_mut(&project.id).expect("slots").active += 1;
                                    }
                                    if matches!(
                                        step,
                                        Step::ApplyDefaults
                                            | Step::ClearPlanningWait
                                            | Step::RefreshPlacement
                                    ) {
                                        let id = p.read.task.id.clone();
                                        if let Some(read) =
                                            self.db.schedule_reads(&[id]).await?.pop()
                                        {
                                            p = self.prepare_schedule(
                                                read,
                                                project,
                                                chrono::Utc::now(),
                                            );
                                            p.facts.refresh_placement = false;
                                            continue;
                                        }
                                    }
                                    if !self
                                        .db
                                        .schedule_has_owner(&p.read.task.id, p.read.epoch)
                                        .await?
                                    {
                                        result_park = Some(match step {
                                            Step::Publication => {
                                                p.deadline = Some(
                                                    (chrono::Utc::now()
                                                        + chrono::Duration::seconds(10))
                                                    .to_rfc3339(),
                                                );
                                                Park {
                                                    reason: Reason::RetryDeadline,
                                                    owner: Owner::Worker,
                                                    recovery: Action::WaitForDeadline,
                                                }
                                            }
                                            Step::Initial { .. }
                                            | Step::Role { .. }
                                            | Step::QueuedRecovery => Park {
                                                reason: Reason::Capacity,
                                                owner: Owner::Scheduler,
                                                recovery: Action::FreeCapacity,
                                            },
                                            _ => Park {
                                                reason: Reason::UnknownCondition {
                                                    owner: step.identity(),
                                                },
                                                owner: Owner::Workflow,
                                                recovery: Action::ReconcileEntry,
                                            },
                                        });
                                    }
                                }
                                Err(error) => {
                                    self.handle_schedule_error(&p, step, &error).await?;
                                    result_park = Some(Park {
                                        reason: Reason::UnknownCondition {
                                            owner: "dispatch admission".into(),
                                        },
                                        owner: Owner::Workflow,
                                        recovery: Action::RepairAndRetry,
                                    });
                                    if !super::helpers::is_deterministic_dispatch_refusal(&error) {
                                        p.deadline = Some(
                                            (chrono::Utc::now() + chrono::Duration::seconds(10))
                                                .to_rfc3339(),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                if let Some(park) = result_park {
                    let encoded = serde_json::to_string(&park).expect("park serializes");
                    if p.read.park_json.as_deref() != Some(&encoded) {
                        self.db
                            .record_schedule_park(&p.read.task.id, p.read.epoch, &encoded)
                            .await?;
                        if unowned {
                            tracing::warn!(task_id=%p.read.task.id,reason=?park.reason,owner=?park.owner,recovery=?park.recovery,"installed Task owner park");
                        }
                    }
                }
                let wait_scope = matches!(
                    next,
                    Next::Step(
                        Step::Initial { .. }
                            | Step::Role { .. }
                            | Step::QueuedRecovery
                            | Step::FailedReview
                    )
                ) || matches!(
                    next,
                    Next::Park(Park {
                        reason: Reason::RetryDeadline | Reason::AgentUnavailable | Reason::Capacity,
                        ..
                    })
                );
                let recovery_agent = if matches!(next, Next::Step(Step::FailedReview)) {
                    serde_json::from_str::<api_types::ProjectSettings>(&project.settings)
                        .ok()
                        .filter(|s| s.automatic_recovery.enabled)
                        .and_then(|s| s.automatic_recovery.agent_id)
                } else {
                    None
                };
                let agent = wait_scope
                    .then(|| {
                        p.facts
                            .role_target
                            .as_ref()
                            .map(|(_, id)| id.as_str())
                            .or_else(|| {
                                p.facts
                                    .initial_target
                                    .as_ref()
                                    .map(|(_, _, id)| id.as_str())
                            })
                    })
                    .flatten();
                let agent = recovery_agent.as_deref().or(agent);
                let agent_daemon = match agent {
                    Some(id) => db::AgentRepo::get_by_id(&*self.db, id)
                        .await?
                        .and_then(|a| a.daemon_id),
                    None => None,
                };
                let metadata = p.read.task.metadata().ok();
                let daemon = p
                    .read
                    .placement_daemon_id
                    .clone()
                    .or_else(|| {
                        metadata
                            .as_ref()
                            .and_then(|m| m.extra.get("environment_wait"))
                            .and_then(|v| v["machine"]["daemon_id"].as_str())
                            .map(str::to_owned)
                    })
                    .or_else(|| {
                        metadata
                            .as_ref()
                            .and_then(|m| m.extra.get("owner_wait"))
                            .and_then(|v| v["daemon_id"].as_str())
                            .map(str::to_owned)
                    })
                    .or(agent_daemon);
                if agent.is_some() || p.deadline.is_some() || p.read.has_wait {
                    self.db
                        .schedule_wait(
                            &p.read.task.id,
                            (
                                &project.id,
                                p.workflow.state_kind(&p.read.task.status)
                                    == Some(api_types::StateKind::Initial),
                            ),
                            agent,
                            daemon.as_deref(),
                            p.deadline.as_deref(),
                        )
                        .await?;
                }
                self.db
                    .acknowledge_schedule(
                        &p.read.task.id,
                        if p.read.dirty { p.read.generation } else { 0 },
                    )
                    .await?;
                break;
            }
        }
        if sweep {
            // One bounded checker and persisted cursor, after dispatch work.
            loop {
                self.db
                    .check_task_conditions(db::CONDITION_CHECK_PAGE)
                    .await?;
                self.db.persist_condition_cursor().await?;
                if self.db.condition_check_page_ids().1.is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        if repairs > 0 {
            self.db.count_schedule_repairs(repairs);
        }
        // The owning timers (Task retries, leases, reservation expiry, readiness
        // and failed Review grace) wake independently of the slow sweep.
        let deadline:Option<String>=sqlx::query_scalar("SELECT MIN(deadline) FROM (SELECT deadline FROM task_schedule_wait WHERE deadline IS NOT NULL UNION ALL SELECT next_check_at FROM project_machine_readiness WHERE status='not_ready' UNION ALL SELECT reserved_until FROM workspace_placement WHERE state IN ('reserved','preparing') UNION ALL SELECT lease_until FROM task_step WHERE status='claimed' UNION ALL SELECT lease_expires_at FROM execution WHERE status='running') WHERE julianday(deadline)>julianday('now')").fetch_one(self.db.pool()).await?;
        let deadline = deadline
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|d| {
                Instant::now()
                    + (d.with_timezone(&chrono::Utc) - chrono::Utc::now())
                        .to_std()
                        .unwrap_or(Duration::ZERO)
            });
        self.schedule_state.lock().expect("schedule state").deadline = deadline;
        Ok(count)
    }
    pub(super) fn schedule_sleep(&self) -> Duration {
        let state = self.schedule_state.lock().expect("schedule state");
        if !state.started {
            return self.check_interval;
        }
        let due = state
            .deadline
            .map_or(state.sweep_at, |d| d.min(state.sweep_at));
        due.saturating_duration_since(Instant::now())
            .min(self.check_interval)
    }
    async fn apply_schedule_step(
        &self,
        p: &Prepared,
        project: &Project,
        step: &Step,
    ) -> Result<u64> {
        let mut current = p.read.task.clone();
        if p.read.external || p.read.verify {
            // A dependency revision invalidates the observation, without a
            // bookkeeping Task write or cancelling an owned retry deadline.
            if let Ok(mut metadata) = db::TaskMetadata::parse(current.metadata_json.as_deref()) {
                metadata.extra.remove("dispatch_disposition");
                current.metadata_json = metadata.to_json();
            }
        }
        let task = &current;
        match step {
            Step::RefreshPlacement => {
                self.task_service
                    .refresh_placement_dispatch_refusal(task.clone())
                    .await?;
                Ok(0)
            }
            Step::Publication => self.reconcile_publication_task(task).await,
            Step::QueuedRecovery => self
                .task_service
                .dispatch_queued_recovery(task)
                .await
                .map(u64::from),
            Step::Integrate => self
                .task_service
                .retry_paused_integration(task)
                .await
                .map(u64::from),
            Step::ExpireOwnerWait => {
                self.task_service.expire_owner_wait(task).await?;
                Ok(0)
            }
            Step::RetryReviewCi => {
                self.task_service
                    .retry_entry_checks(task.clone(), Some("retry review CI infrastructure".into()))
                    .await?;
                Ok(1)
            }
            Step::ReviewRefresh { target } => {
                self.task_service
                    .transition(
                        task.id.clone(),
                        target.clone(),
                        crate::task_service::TransitionOptions {
                            bridge: api_types::TransitionBridge::new(
                                api_types::TransitionBridgeKind::ReviewRefresh,
                            ),
                            version: task.version,
                            reason: Some("Recover interrupted review refresh".into()),
                            triggered_by: Actor::system(SystemComponent::TaskDispatcher),
                            rejection: false,
                            defer_dispatch_seconds: None,
                        },
                    )
                    .await?;
                Ok(1)
            }
            Step::SettleExecution { .. } => {
                let role = p
                    .facts
                    .role_target
                    .as_ref()
                    .map(|(r, _)| r.as_str())
                    .or_else(|| {
                        p.workflow
                            .states
                            .iter()
                            .find(|s| s.name == task.status)
                            .and_then(crate::workflow::effective_role)
                    })
                    .unwrap_or("");
                if role == "reviewer" {
                    self.reconcile_terminal_reviewer_execution(&task.id, project.version)
                        .await?;
                } else {
                    self.reconcile_terminal_role_execution(task, role, project.version)
                        .await?;
                }
                Ok(0)
            }
            Step::FailedReview => Ok(u64::from(
                self.recover_failed_review(task).await?.unwrap_or(false),
            )),
            Step::ClearPlanningWait => {
                crate::task_service::execution::clear_stale_planning_review_metadata(
                    &self.db, task,
                )
                .await?;
                Ok(0)
            }
            Step::AdvanceRoot => self.advance_coordination_root_once(task).await,
            Step::ApplyDefaults => {
                self.task_service.assign_project_default_roles(task).await?;
                Ok(0)
            }
            Step::Initial {
                target,
                role,
                agent_id,
            } => self
                .dispatch_initial_task(
                    task,
                    &super::initial_scheduling::InitialScheduleTarget {
                        transition_to: target.clone(),
                        role: role.clone(),
                        agent_id: agent_id.clone(),
                    },
                )
                .await
                .map(u64::from),
            Step::Role { role, agent_id } => {
                self.enqueue_resolved_role(p, project, role, agent_id).await
            }
            Step::MergeEntry => self
                .task_service
                .workflow_execution()
                .enqueue_recovered_entry_hooks(task, p.read.epoch, project, &p.workflow)
                .await
                .map(|id| u64::from(id.is_some())),
        }
    }
    async fn enrich_schedule_agents(&self, prepared: &mut [Prepared]) -> Result<()> {
        let ids: std::collections::HashSet<String> = prepared
            .iter()
            .filter_map(|p| {
                match next_step::next_step(&Snapshot {
                    state: &p.read.task.status,
                    condition: &p.read.condition,
                    workflow: &p.workflow,
                    facts: &p.facts,
                }) {
                    Next::Step(Step::Role { agent_id, .. } | Step::Initial { agent_id, .. }) => {
                        Some(agent_id)
                    }
                    _ => None,
                }
            })
            .collect();
        let ids: Vec<_> = ids.into_iter().collect();
        let agents = self.db.schedule_agents(&ids).await?;
        let mut running = HashMap::new();
        for agent in &agents {
            running.insert(
                agent.id.clone(),
                crate::agent_capacity::count_running_executions(&self.db, &agent.id).await?,
            );
        }
        let registry = crate::daemon_transport::DaemonConnectionRegistry::without_handlers();
        let statuses = crate::agent_service::compute_effective_status_for_agents(
            &self.db, &agents, &running, &registry,
        )
        .await?;
        for p in prepared {
            let candidate = next_step::next_step(&Snapshot {
                state: &p.read.task.status,
                condition: &p.read.condition,
                workflow: &p.workflow,
                facts: &p.facts,
            });
            if let Next::Step(step @ (Step::Role { .. } | Step::Initial { .. })) = &candidate {
                let (role, agent) = match step {
                    Step::Role { role, agent_id } | Step::Initial { role, agent_id, .. } => {
                        (role, agent_id)
                    }
                    _ => unreachable!(),
                };
                if matches!(step, Step::Role { .. }) {
                    let gate = if role == "reviewer" {
                        self.task_service.ensure_task_reviewable(&p.read.task).await
                    } else {
                        self.task_service.ensure_task_runnable(&p.read.task).await
                    };
                    if let Err(error) = gate {
                        self.handle_schedule_error(p, step, &error).await?;
                        p.read.condition = self.db.task_condition(&p.read.task.id).await?;
                        p.facts.disposition_current = true;
                        continue;
                    }
                }
                if let Some(identity) = agents.iter().find(|a| a.id == *agent) {
                    match crate::placement::machine_precheck::wait_before_dispatch(
                        &self.db,
                        &self.task_service,
                        &p.read.task,
                        identity,
                        Some(role),
                    )
                    .await
                    {
                        Ok(true) => {
                            p.facts.agent_full = true;
                            continue;
                        }
                        Ok(false) => {}
                        Err(error) => {
                            self.handle_schedule_error(p, step, &error).await?;
                            p.read.condition = self.db.task_condition(&p.read.task.id).await?;
                            p.facts.disposition_current = true;
                            continue;
                        }
                    }
                }
            }
            if let Some(agent) = p
                .facts
                .initial_target
                .as_ref()
                .map(|(_, _, id)| id)
                .or_else(|| p.facts.role_target.as_ref().map(|(_, id)| id))
            {
                use crate::agent_service::EffectiveStatus;
                let status = statuses.get(agent);
                p.facts.agent_unavailable = !matches!(
                    status,
                    Some(EffectiveStatus::Active | EffectiveStatus::Busy)
                );
                p.facts.agent_full = status == Some(&EffectiveStatus::Busy);
            }
        }
        Ok(())
    }
    async fn enqueue_resolved_role(
        &self,
        p: &Prepared,
        project: &Project,
        role: &str,
        agent: &str,
    ) -> Result<u64> {
        use db::TaskStepRepo;
        if super::helpers::has_running_execution_for_roles(
            &self.db,
            &p.read.task.id,
            &super::helpers::execution_guard_roles(role),
        )
        .await?
        {
            return Ok(0);
        }
        let (queued, _) = self.db.queued_admissions(agent, &p.read.task.id).await?;
        let Some(identity) = db::AgentRepo::get_by_id(&*self.db, agent).await? else {
            return Ok(0);
        };
        if queued + crate::agent_capacity::count_occupied_agent_slots(&self.db, agent).await?
            >= identity.max_concurrent_tasks
        {
            return Ok(0);
        }
        let id = db::new_uuid_v4();
        let command = crate::task_service::commands::TaskCommand {
            operation: "reconcile_role".into(),
            arguments: serde_json::json!([
                p.read.task.id,
                role,
                agent,
                (p.read.external || p.read.verify)
                    .then(
                        || db::TaskMetadata::parse(p.read.task.metadata_json.as_deref())
                            .ok()
                            .and_then(|m| m.extra.get("dispatch_disposition").cloned())
                    )
                    .flatten(),
                p.read.external.then(|| {
                    let metadata = p.read.task.metadata().ok();
                    serde_json::json!({
                        "environment_wait": metadata.as_ref().and_then(|m| m.extra.get("environment_wait")),
                        "owner_wait": metadata.as_ref().and_then(|m| m.extra.get("owner_wait")),
                        "deferred_dispatch": metadata.as_ref().and_then(|m| m.extra.get("deferred_dispatch"))
                    })
                })
            ]),
            preempt: false,
        };
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        self.db
            .enqueue_step_in_tx(
                &mut tx,
                &db::EnqueueTaskStep {
                    id: id.clone(),
                    task_id: p.read.task.id.clone(),
                    kind: "command".into(),
                    payload_json: command.payload_json()?,
                    causation_step_id: None,
                    causation_key: format!(
                        "schedule:{}:{}:{}:{}:{}",
                        p.read.epoch,
                        p.read.task.version,
                        project.version,
                        p.read.generation,
                        Step::Role {
                            role: role.into(),
                            agent_id: agent.into()
                        }
                        .identity()
                    ),
                    chain_id: id,
                    chain_position: 1,
                    expected_status: p.read.task.status.clone(),
                    expected_version: p.read.task.version,
                    expected_epoch: Some(p.read.epoch),
                    lane: if serde_json::from_str::<api_types::ProjectSettings>(&project.settings)
                        .is_ok_and(|s| !s.environment.checks.is_empty())
                    {
                        "long"
                    } else {
                        "fast"
                    }
                    .into(),
                    available_at: db::now_rfc3339(),
                },
            )
            .await?;
        tx.commit().await?;
        self.db.domain_event_notify().notify_waiters();
        Ok(1)
    }
    pub(crate) async fn execute_resolved_role(
        &self,
        id: &str,
        role: &str,
        agent: &str,
        invalidated_disposition: Option<&serde_json::Value>,
        invalidated_wait: Option<&serde_json::Value>,
    ) -> Result<bool> {
        let Some(mut read) = self.db.schedule_reads(&[id.into()]).await?.pop() else {
            return Ok(false);
        };
        if read.task.deleted_at.is_some() || read.task.archived_at.is_some() {
            return Ok(false);
        }
        if let Some(expected) = invalidated_disposition {
            if let Ok(mut metadata) = db::TaskMetadata::parse(read.task.metadata_json.as_deref()) {
                if metadata.extra.get("dispatch_disposition") == Some(expected) {
                    metadata.extra.remove("dispatch_disposition");
                    read.verify = true;
                    read.task.metadata_json = metadata.to_json();
                }
            }
        }
        if let Some(wait) = invalidated_wait {
            let metadata = read.task.metadata().ok();
            // An external fact revision permits immediate re-admission only
            // for the exact wait observed by the resolver. A newer timer wins.
            read.external |= ["environment_wait", "owner_wait", "deferred_dispatch"]
                .iter()
                .all(|key| {
                    metadata
                        .as_ref()
                        .and_then(|m| m.extra.get(*key))
                        .unwrap_or(&serde_json::Value::Null)
                        == &wait[*key]
                });
        }
        // The command itself owns the Task lease; no other head can execute.
        read.queue_owned = false;
        // This durable command already authorized a reconciliation attempt.
        // Refusal metadata is a cache, including late writes from the old
        // dependency observation; fresh admission gates remain authoritative.
        read.verify = true;
        let Some(project) = ProjectRepo::get_by_id(&*self.db, &read.task.project_id).await? else {
            return Ok(false);
        };
        let p = self.prepare_schedule(read, &project, chrono::Utc::now());
        let expected = Step::Role {
            role: role.into(),
            agent_id: agent.into(),
        };
        let resolved = next_step::next_step(&Snapshot {
            state: &p.read.task.status,
            condition: &p.read.condition,
            workflow: &p.workflow,
            facts: &p.facts,
        });
        if resolved != Next::Step(expected.clone()) {
            return Ok(false);
        }
        match self
            .dispatch_resolved_role(&project, &p.workflow, &p.read.task, role, agent)
            .await
        {
            Ok(done) => {
                if done {
                    if let Some(current) = db::TaskRepo::get_by_id(&*self.db, id, false).await? {
                        self.clear_dispatch_disposition(&current).await?;
                    }
                }
                Ok(done)
            }
            Err(error) => {
                self.handle_schedule_error(&p, &expected, &error).await?;
                if !super::helpers::is_deterministic_dispatch_refusal(&error)
                    && !matches!(
                        error,
                        ServiceError::Db(
                            db::DbError::VersionConflict | db::DbError::TaskVersionConflict { .. }
                        )
                    )
                {
                    if let Some(current) = db::TaskRepo::get_by_id(&*self.db, id, false).await? {
                        db::TaskRepo::mutate_metadata(&*self.db,id,Some(current.version),vec![db::TaskMetadataMutation::SetIfAbsent {key:"deferred_dispatch".into(),value:serde_json::json!({"not_before":(chrono::Utc::now()+chrono::Duration::seconds(10)).to_rfc3339(),"target_state":current.status,"reason":"transient dispatcher admission"})}],&db::now_rfc3339()).await?;
                    }
                }
                Ok(false)
            }
        }
    }
    async fn handle_schedule_error(
        &self,
        p: &Prepared,
        step: &Step,
        error: &ServiceError,
    ) -> Result<()> {
        let task = &p.read.task;
        if matches!(
            error,
            ServiceError::Db(
                db::DbError::VersionConflict | db::DbError::TaskVersionConflict { .. }
            )
        ) {
            return Ok(());
        }
        if matches!(error, ServiceError::WorkspaceResetRequired { .. }) {
            self.block_task_for_workspace_reset(task, error).await?;
        } else if super::helpers::is_io_or_workspace_error(error) {
            self.block_task_on_workspace_error(task, error).await?;
        } else if matches!(step, Step::Role { .. })
            && crate::placement::admission_refusal_is_retryable(&self.db, &task.id, error).await?
        {
            let current = db::TaskRepo::get_by_id(&*self.db, &task.id, false)
                .await?
                .ok_or(db::DbError::NotFound)?;
            self.task_service
                .defer_placement_refusal(&current, error)
                .await?;
        } else if super::helpers::is_deterministic_dispatch_refusal(error)
            && !self
                .task_service
                .record_placement_dispatch_refusal(task, error)
                .await?
        {
            let capability = match step {
                Step::Initial { role, .. } => role.as_str(),
                _ => task.status.as_str(),
            };
            deferred_dispatch::record_dispatch_disposition(
                &self.db,
                task,
                capability,
                &error.to_string(),
            )
            .await?;
            crate::workflow::engine::annotate_upgrade_dispatch_refusal(
                &self.db,
                &task.id,
                &task.status,
                error,
            )
            .await?;
        }
        tracing::warn!(task_id=%task.id,%error,"Task reconciliation deferred");
        Ok(())
    }
}
