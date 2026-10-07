//! One reconciliation loop. A commit that changes a scheduling fact marks the
//! Tasks it can affect; exact deadlines mark their own; a paged sweep marks
//! whatever both missed. Everything marked is resolved by the pure
//! `next_step` and applied through the existing Task command and cascade
//! adapters, with the legacy fields written exactly as the scanning
//! dispatcher wrote them.
use super::{
    next_step::{self, Action, Next, Owner, Park, Reason, Snapshot, Step},
    snapshot::Prepared,
    TaskDispatcher,
};
use crate::{deferred_dispatch, Result, ServiceError};
use api_types::{Actor, StateKind, SystemComponent};
use db::{Project, ProjectRepo};
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

/// Every Task that is not settled is re-examined once per period.
pub(super) const SWEEP_PERIOD: Duration = Duration::from_secs(120);
/// What one tick spends on the sweep before it yields to dispatch.
pub(super) const SWEEP_SLICE: Duration = Duration::from_millis(100);
const SWEEP_PAGE: i64 = 100;

#[derive(Debug)]
pub(super) struct ScheduleState {
    pub started: bool,
    /// The commit generation of the last pass that completed.
    pub generation: u64,
    pub deadline: Option<Instant>,
    pub repo_retry: bool,
    pub server_cap: Option<Option<i64>>,
    /// Tasks re-read at the base scan cadence without a commit: a hold on a
    /// fact no write announces (credential, provider health and backoff,
    /// connection health, CLI policy), a step that declined, a failed pass.
    pub recheck: HashSet<String>,
    pub recheck_at: Option<Instant>,
    pub sweep: Sweep,
}
#[derive(Debug)]
pub(super) struct Sweep {
    /// When the next lap starts.
    pub due: Instant,
    active: bool,
    /// In memory only: a restart begins a fresh lap.
    cursor: Option<String>,
    /// Tasks the sweep found work for, handed to the next pass.
    pending: HashSet<String>,
    /// Every Project's repository is probed once per lap.
    projects: bool,
    repairs: u64,
    backfilling: bool,
}
impl Sweep {
    /// A lap is in progress.
    pub(super) fn active(&self) -> bool {
        self.active
    }
    /// The sweep found Tasks the next pass has not looked at yet.
    pub(super) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }
}
impl Default for ScheduleState {
    fn default() -> Self {
        Self {
            started: false,
            generation: 0,
            deadline: None,
            repo_retry: false,
            server_cap: None,
            recheck: HashSet::new(),
            recheck_at: None,
            sweep: Sweep {
                due: Instant::now(),
                active: false,
                cursor: None,
                pending: HashSet::new(),
                projects: true,
                repairs: 0,
                backfilling: false,
            },
        }
    }
}

/// What one pass was asked to look at besides the committed dirty set.
#[derive(Default)]
struct Marked {
    swept: HashSet<String>,
    recheck: HashSet<String>,
    capacity_changed: bool,
    all_projects: bool,
    /// Read every Task that is not settled in this pass.
    discover: bool,
}
/// Agent availability as one pass read it.
struct AgentStatuses {
    agents: Vec<db::Agent>,
    statuses: HashMap<String, crate::agent_service::EffectiveStatus>,
}
/// What one Task's reconciliation asks of later ticks.
#[derive(Default)]
struct Outcome {
    dispatched: u64,
    recheck: bool,
}

fn encode(park: &Park) -> String {
    serde_json::to_string(park).expect("park serializes")
}
fn waits(reason: Reason, owner: Owner, recovery: Action) -> Park {
    Park {
        reason,
        owner,
        recovery,
    }
}

impl TaskDispatcher {
    /// Dispatch starts from the durable dirty set at once. Everything else a
    /// restart has to rediscover is the first lap of the paged sweep, which
    /// runs behind it and never delays startup.
    pub async fn startup_reconcile(&self) -> Result<u64> {
        self.reconcile_once(false).await
    }

    /// Reconcile everything now: the dirty set, one complete lap of the sweep
    /// and whatever that lap found. The operator refresh asks for this, as it
    /// asked the scanning dispatcher for a full scan.
    pub async fn reconcile_all(&self) -> Result<u64> {
        let mut dispatched = self.reconcile_once(true).await?;
        {
            let mut state = self.schedule_state.lock().expect("schedule state");
            if !state.sweep.active {
                state.sweep.due = Instant::now();
            }
        }
        loop {
            self.sweep_slice(Duration::MAX).await?;
            if !self
                .schedule_state
                .lock()
                .expect("schedule state")
                .sweep
                .active()
            {
                break;
            }
        }
        dispatched += self.reconcile_once(true).await?;
        Ok(dispatched)
    }

    /// Reconcile what commits marked, then fail if any Task that is not
    /// settled is left with no queued step, no live execution and no park.
    /// The check reads the durable state directly, before the sweep could
    /// repair it, so a Task the kicks missed fails here.
    pub async fn sweep_and_assert(&self) -> Result<()> {
        self.reconcile_once(false).await?;
        let violations = self.db.task_schedule_violations().await?;
        if !violations.is_empty() {
            return Err(ServiceError::invalid_operation(format!(
                "unowned Tasks after reconciliation: {violations:?}"
            )));
        }
        Ok(())
    }

    /// `asked` is a direct request to look now (a caller of `check_once`,
    /// the operator refresh), as opposed to the supervised loop's own tick:
    /// it also re-reads the Tasks held on facts no commit announces, which
    /// the loop re-reads at the scan interval, and the first one an instance
    /// receives reads every Task that is not settled, as a first scan did.
    /// Startup and the loop never do that: the paged sweep does it for them.
    pub(super) async fn reconcile_once(&self, asked: bool) -> Result<u64> {
        self.register();
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
        let marked = {
            let mut state = self.schedule_state.lock().expect("schedule state");
            let capacity_changed = state.server_cap.is_some_and(|old| old != cap);
            let recheck_due = state.recheck_at.is_some_and(|at| at <= now)
                || (asked && !state.recheck.is_empty());
            if state.started
                && !capacity_changed
                && state.generation == generation
                && !self.db.schedule_commit_pending()
                && state.deadline.is_none_or(|d| d > now)
                && !jobs_ready
                && !state.repo_retry
                && state.sweep.pending.is_empty()
                && !state.sweep.projects
                && !recheck_due
            {
                return Ok(0);
            }
            let discover = asked && !state.started;
            state.started = true;
            state.server_cap = Some(cap);
            let recheck = if recheck_due {
                state.recheck_at = None;
                std::mem::take(&mut state.recheck)
            } else {
                HashSet::new()
            };
            Marked {
                swept: std::mem::take(&mut state.sweep.pending),
                recheck,
                capacity_changed,
                all_projects: std::mem::take(&mut state.sweep.projects),
                discover,
            }
        };
        let retry: Vec<String> = marked
            .swept
            .iter()
            .chain(&marked.recheck)
            .cloned()
            .collect();
        let all_projects = marked.all_projects;
        let result = Box::pin(self.reconcile_pass(marked)).await;
        let mut state = self.schedule_state.lock().expect("schedule state");
        match &result {
            // Only a pass that completed has seen this generation.
            Ok(_) => state.generation = generation,
            Err(error) => {
                tracing::warn!(%error, "Task reconciliation pass failed; retrying at the next tick");
                state.recheck.extend(retry);
                state.recheck_at = Some(Instant::now() + self.check_interval);
                state.sweep.projects |= all_projects;
            }
        }
        result
    }

    async fn reconcile_pass(&self, marked: Marked) -> Result<u64> {
        self.observe_environment_settings();
        let mut ids: HashSet<String> = self
            .db
            .dirty_schedule_tasks(i64::MAX)
            .await?
            .into_iter()
            .collect();
        let capacity_ids: HashSet<String> = if marked.capacity_changed {
            self.db
                .schedule_machine_waiters()
                .await?
                .into_iter()
                .collect()
        } else {
            HashSet::new()
        };
        ids.extend(capacity_ids.iter().cloned());
        ids.extend(self.db.due_schedule_tasks(&db::now_rfc3339()).await?);
        ids.extend(marked.swept);
        ids.extend(marked.recheck);
        if marked.discover {
            ids.extend(self.db.open_schedule_tasks(None, i64::MAX).await?);
        }
        if !ids.is_empty() {
            crate::placement::admission::sweep_expired_reservations(&self.db, &db::now_rfc3339())
                .await?;
        }
        match self.sync_due_environment_checks().await {
            Ok(changed) if !changed.is_empty() => {
                ids.extend(self.db.dirty_schedule_tasks(i64::MAX).await?)
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(%error, "environment readiness scan failed; continuing dispatch")
            }
        }
        // One Task that cannot be read or reconciled is logged, counted in the
        // invariant report and retried; every other Task still gets its pass.
        let mut failures = 0_u64;
        let mut retry: Vec<String> = Vec::new();
        let mut reads = Vec::new();
        let ids: Vec<_> = ids.into_iter().collect();
        for page in ids.chunks(50) {
            let (page_reads, unreadable) = self.db.schedule_reads_isolated(page).await?;
            reads.extend(page_reads);
            for (id, error) in unreadable {
                tracing::warn!(task_id = %id, %error, "Task scheduling snapshot unreadable; recomputing its condition");
                failures += 1;
                // The stored condition is what failed to decode; the legacy
                // fields it shadows are intact, so recompute it and go on.
                let one = std::slice::from_ref(&id);
                let repaired = match self.db.check_task_conditions_of(one).await {
                    Ok(_) => self.db.schedule_reads_isolated(one).await.ok(),
                    Err(error) => {
                        tracing::warn!(task_id = %id, %error, "Task condition repair failed");
                        None
                    }
                };
                match repaired {
                    Some((mut read, unreadable)) if unreadable.is_empty() => {
                        reads.append(&mut read)
                    }
                    _ => retry.push(id),
                }
            }
        }
        for read in &mut reads {
            read.external |= capacity_ids.contains(&read.task.id);
        }
        let project_reads = self.db.schedule_projects(marked.all_projects).await?;
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
        let mut unsynced = HashSet::new();
        // Repo negative readiness still needs a filesystem probe on the sweep.
        // Positive readiness is memoized and only relevant Projects are loaded.
        for p in projects.values() {
            match self.sync_repository_pause(p).await {
                Ok(true) => {
                    skipped.insert(p.id.clone());
                }
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(project_id = %p.id, %error, "repository pause synchronization failed; skipping Project");
                    unsynced.insert(p.id.clone());
                }
            }
        }
        for (p, generation) in &project_reads {
            if !unsynced.contains(&p.id) {
                self.db
                    .acknowledge_schedule_project(&p.id, *generation)
                    .await?;
            }
        }
        self.schedule_state
            .lock()
            .expect("schedule state")
            .repo_retry = !unsynced.is_empty()
            || projects
                .values()
                .any(|p| p.system_pause_reason.as_deref() == Some("repository_not_ready"));
        let now = chrono::Utc::now();
        let mut prepared: Vec<_> = reads
            .into_iter()
            .filter(|r| !unsynced.contains(&r.task.project_id))
            .filter_map(|r| {
                projects
                    .get(&r.task.project_id)
                    .map(|p| self.prepare_schedule(r, p, now))
            })
            .collect();
        let agents = self.schedule_agent_statuses(&prepared).await?;
        // The scanning dispatcher's order: Projects oldest first; within one,
        // publication cleanup, queued recoveries, work already in flight, then
        // new admissions by priority. Every tie breaks on creation order.
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
                } else if p.workflow.state_kind(&p.read.task.status) == Some(StateKind::Initial) {
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
        let mut recheck = Vec::new();
        for mut p in prepared {
            if self.is_stopped() {
                break;
            }
            let project = &projects[&p.read.task.project_id];
            let id = p.read.task.id.clone();
            let paused_by_sync = skipped.contains(&project.id);
            match Box::pin(self.reconcile_task(
                &mut p,
                project,
                paused_by_sync,
                &agents,
                &mut slots,
            ))
            .await
            {
                Ok(outcome) => {
                    count += outcome.dispatched;
                    if outcome.recheck {
                        recheck.push(id);
                    }
                }
                Err(ServiceError::Db(
                    db::DbError::VersionConflict | db::DbError::TaskVersionConflict { .. },
                )) => {
                    // The write that won dirtied the Task again.
                    tracing::debug!(task_id = %id, "Task reconciliation lost a version race");
                }
                Err(error) => {
                    tracing::warn!(task_id = %id, %error, "Task reconciliation failed; continuing with the next Task");
                    failures += 1;
                    retry.push(id);
                }
            }
        }
        let deadline = self
            .db
            .next_schedule_deadline()
            .await?
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|d| {
                Instant::now()
                    + (d.with_timezone(&chrono::Utc) - chrono::Utc::now())
                        .to_std()
                        .unwrap_or(Duration::ZERO)
            });
        let mut state = self.schedule_state.lock().expect("schedule state");
        state.deadline = deadline;
        state.sweep.repairs += failures;
        state.recheck.extend(retry);
        state.recheck.extend(recheck);
        if !state.recheck.is_empty() && state.recheck_at.is_none() {
            state.recheck_at = Some(Instant::now() + self.check_interval);
        }
        Ok(count)
    }

    /// Resolve one Task and apply what it resolved to. Default assignment and
    /// stale approval cleanup can expose an admission candidate in the same
    /// pass, preserving the old fairness.
    async fn reconcile_task(
        &self,
        p: &mut Prepared,
        project: &Project,
        paused_by_sync: bool,
        agents: &AgentStatuses,
        slots: &mut HashMap<String, api_types::ProjectSlots>,
    ) -> Result<Outcome> {
        let mut outcome = Outcome::default();
        if p.read.task.deleted_at.is_some() || p.read.task.archived_at.is_some() {
            self.acknowledge(p).await?;
            return Ok(outcome);
        }
        if paused_by_sync && !p.facts.publication {
            // The Project row this pass loaded is stale; the pause change
            // dirtied its Tasks, so the next pass resolves them.
            let park = encode(&waits(
                Reason::ProjectPaused,
                Owner::User,
                Action::ResumeProject,
            ));
            if p.read.park_json.as_deref() != Some(&park) {
                self.db
                    .record_schedule_park(&p.read.task.id, p.read.epoch, &park)
                    .await?;
            }
            return Ok(outcome);
        }
        // A placement refusal is re-evaluated against live facts no commit
        // announces (a machine's connection, a due provisioning retry). The
        // scanning dispatcher re-read such a Task on every scan; so does this.
        outcome.recheck |= p.facts.refresh_placement;
        if p.condition_stale {
            // A writer missed its condition sync. This pass already resolved
            // from the legacy fields; repair the stored copy and report it.
            let repaired = self
                .db
                .check_task_conditions_of(std::slice::from_ref(&p.read.task.id))
                .await?;
            self.schedule_state
                .lock()
                .expect("schedule state")
                .sweep
                .repairs += repaired;
        }
        let mut gate = true;
        let mut refused = false;
        for _ in 0..4 {
            let machine_wait = p.machine_wait;
            refused |= Box::pin(self.observe_admission(p, project, agents, gate)).await;
            p.machine_wait |= machine_wait;
            gate = false;
            let initial = p.workflow.state_kind(&p.read.task.status) == Some(StateKind::Initial);
            let next = next_step::next_step(&Snapshot {
                state: &p.read.task.status,
                condition: &p.read.condition,
                workflow: &p.workflow,
                facts: &p.facts,
            });
            // Ownerless diagnoses now live in the condition; never write a legacy annotation.
            let visible = matches!(&next, Next::Park(park) if matches!(&park.reason, Reason::WorkflowInvalid{..}) || matches!(&park.reason, Reason::UnknownCondition{owner} if owner == next_step::PUBLICATION_OWNER || owner == next_step::ENTRY_HOOKS_OWNER));
            let current_visible = p.read.condition.reasons().any(|r| {
                matches!(
                    r,
                    db::ParkReason::WorkflowInvalid { .. }
                        | db::ParkReason::UnknownCondition {
                            source: db::ConditionSource {
                                field: db::LegacyConditionField::SchedulePark,
                                ..
                            },
                            ..
                        }
                )
            });
            if !visible
                && current_visible
                && self.db.clear_visible_schedule_park(&p.read.task.id).await?
            {
                if self.reread(p, project).await? {
                    continue;
                }
                return Ok(outcome);
            }
            let mut parked = None;
            let mut project_wait = false;
            match &next {
                Next::Park(park) => {
                    parked = Some(park.clone());
                    // Availability also turns on facts no commit announces.
                    outcome.recheck |= park.reason == Reason::AgentUnavailable;
                    if initial
                        && p.facts.initial_target.is_none()
                        && park.reason == Reason::HumanWork
                    {
                        // No dispatch target: a capacity wait recorded for an
                        // earlier target no longer describes this Task.
                        deferred_dispatch::clear_capacity_wait(&self.db, &p.read.task).await?;
                    }
                }
                // The admission check for this role failed without leaving a
                // refusal: nothing is dispatched past it, and it is tried
                // again at the scan cadence, as the next scan tried it.
                Next::Step(Step::Role { .. }) if refused => {
                    outcome.recheck = true;
                    parked = Some(waits(
                        Reason::RetryDeadline,
                        Owner::Scheduler,
                        Action::WaitForDeadline,
                    ));
                }
                Next::Step(step) => {
                    tracing::debug!(task_id=%p.read.task.id, queue_identity=%step.identity(), "resolved Task step");
                    // Only an admission from an initial state takes a Project
                    // slot. A gate cascading past an unassigned role already
                    // holds one.
                    if initial && matches!(step, Step::Initial { .. }) {
                        if !slots.contains_key(&project.id) {
                            slots.insert(
                                project.id.clone(),
                                super::slots::load_project_slots(&self.db, project).await?,
                            );
                        }
                        let slot = slots.get(&project.id).expect("project slots");
                        let waiting = if slot.limit == 0 {
                            None
                        } else if slot.active >= slot.limit {
                            Some(format!(
                                "project_at_capacity: waiting for a slot ({}/{} active)",
                                slot.active, slot.limit
                            ))
                        } else if slot.parked >= 2 * slot.limit {
                            Some(format!(
                                "project_waiting_on_owner: {} parked tasks waiting on the owner",
                                slot.parked
                            ))
                        } else {
                            None
                        };
                        if let Some(message) = waiting {
                            // Written once: an unchanged reason is not
                            // recorded again, so a Project at its limit costs
                            // nothing while it waits.
                            if deferred_dispatch::record_dispatch_disposition(
                                &self.db,
                                &p.read.task,
                                "project_capacity",
                                &message,
                            )
                            .await?
                            {
                                self.publish_capacity_disposition_change(&p.read.task);
                            }
                            project_wait = true;
                            parked = Some(waits(
                                Reason::Capacity,
                                Owner::Scheduler,
                                Action::FreeCapacity,
                            ));
                        }
                    }
                    if parked.is_none() {
                        match Box::pin(self.apply_schedule_step(p, project, step)).await {
                            Ok(n) => {
                                outcome.dispatched += n;
                                if n > 0 && matches!(step, Step::Initial { .. }) {
                                    self.clear_dispatch_disposition(&p.read.task).await?;
                                    if initial {
                                        slots.get_mut(&project.id).expect("slots").active += 1;
                                    }
                                }
                                if matches!(
                                    step,
                                    Step::ApplyDefaults
                                        | Step::ClearPlanningWait
                                        | Step::RefreshPlacement
                                ) {
                                    if self.reread(p, project).await? {
                                        p.facts.refresh_placement = false;
                                        continue;
                                    }
                                    return Ok(outcome);
                                }
                                let owned = self
                                    .db
                                    .schedule_has_owner(&p.read.task.id, p.read.epoch)
                                    .await?;
                                match step {
                                    // Admission declined without a refusal:
                                    // a slot, a queued admission or the
                                    // machine is full. Freed capacity kicks.
                                    Step::Initial { .. }
                                    | Step::Role { .. }
                                    | Step::QueuedRecovery => {
                                        if !owned {
                                            p.machine_wait = true;
                                            parked = Some(waits(
                                                Reason::Capacity,
                                                Owner::Scheduler,
                                                Action::FreeCapacity,
                                            ));
                                        }
                                    }
                                    // The step had nothing to do yet. The
                                    // scanning dispatcher asked again on its
                                    // next scan; so does this one.
                                    _ => {
                                        outcome.recheck |= n == 0;
                                        if !owned {
                                            parked = Some(waits(
                                                Reason::RetryDeadline,
                                                Owner::Scheduler,
                                                Action::WaitForDeadline,
                                            ));
                                        }
                                    }
                                }
                            }
                            Err(error) => {
                                self.handle_schedule_error(p, step, &error).await?;
                                // A recorded refusal waits for the Task to
                                // change; anything else is retried at the
                                // scan cadence, as before.
                                outcome.recheck |=
                                    !super::helpers::is_deterministic_dispatch_refusal(&error);
                                parked = Some(waits(
                                    Reason::RetryDeadline,
                                    Owner::Scheduler,
                                    Action::WaitForDeadline,
                                ));
                            }
                        }
                    }
                }
            }
            if let Some(park) = &parked {
                let encoded = encode(park);
                if p.read.park_json.as_deref() != Some(&encoded) {
                    self.db
                        .record_schedule_park(&p.read.task.id, p.read.epoch, &encoded)
                        .await?;
                }
            }
            self.record_wait(p, project, &next, parked.as_ref(), project_wait)
                .await?;
            self.acknowledge(p).await?;
            return Ok(outcome);
        }
        // Each repeat follows a write that dirtied the Task again.
        Ok(outcome)
    }

    async fn acknowledge(&self, p: &Prepared) -> Result<()> {
        self.db
            .acknowledge_schedule(
                &p.read.task.id,
                if p.read.dirty { p.read.generation } else { 0 },
            )
            .await?;
        Ok(())
    }

    /// Replace the snapshot after a write of this pass. False when the Task
    /// is gone.
    async fn reread(&self, p: &mut Prepared, project: &Project) -> Result<bool> {
        let id = p.read.task.id.clone();
        let Some(read) = self.db.schedule_reads(&[id]).await?.pop() else {
            return Ok(false);
        };
        *p = self.prepare_schedule(read, project, chrono::Utc::now());
        Ok(true)
    }

    /// Which later commits must look at this Task again: the Agent it waits
    /// for, the machine it waits on, the Project limit, an exact deadline.
    /// Nothing else is recorded, so nothing else kicks it.
    async fn record_wait(
        &self,
        p: &Prepared,
        project: &Project,
        next: &Next,
        parked: Option<&Park>,
        project_wait: bool,
    ) -> Result<()> {
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
        let metadata = p.read.task.metadata().ok();
        let machine = |key: &str, path: &[&str]| {
            let mut value = metadata.as_ref()?.extra.get(key)?;
            for part in path {
                value = &value[*part];
            }
            value
                .as_str()
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
        };
        // A readiness or reconnect wait names its machine. A wait for a run
        // slot is for whichever machine placement can use.
        let machine_wait = p.machine_wait
            && parked.is_some_and(|park| park.reason == Reason::Capacity)
            || deferred_dispatch::dispatch_disposition_is_current(&p.read.task, "machine_capacity");
        let daemon = machine("environment_wait", &["machine", "daemon_id"])
            .or_else(|| machine("owner_wait", &["daemon_id"]))
            .or_else(|| machine_wait.then(|| "*".to_owned()));
        if agent.is_some()
            || daemon.is_some()
            || p.deadline.is_some()
            || project_wait
            || p.read.has_wait
        {
            self.db
                .schedule_wait(
                    &p.read.task.id,
                    (&project.id, project_wait),
                    agent,
                    daemon.as_deref(),
                    p.deadline.as_deref(),
                )
                .await?;
        }
        Ok(())
    }

    /// Advance the sweep by at most `budget`. It runs outside the dispatch
    /// pass and takes no writer for a read: a page of Tasks that are not
    /// settled is condition-checked, resolved without I/O, and whatever the
    /// kicks missed is handed to the next pass. A settled Task is never read.
    pub(super) async fn sweep_slice(&self, budget: Duration) -> Result<()> {
        let _slice = self.sweep_lock.lock().await;
        let started = Instant::now();
        // After a mapping change the backfill covers every Task once, in its
        // own bounded slices, behind dispatch.
        let backfilling = self.db.backfill_task_conditions_if_stale().await?;
        {
            let mut state = self.schedule_state.lock().expect("schedule state");
            state.sweep.backfilling = backfilling;
            if !state.sweep.active {
                if state.sweep.due > started {
                    return Ok(());
                }
                state.sweep.active = true;
                state.sweep.cursor = None;
                state.sweep.due = started + SWEEP_PERIOD;
                state.sweep.projects = true;
            }
        }
        let mut projects: HashMap<String, Option<Project>> = HashMap::new();
        loop {
            let cursor = self
                .schedule_state
                .lock()
                .expect("schedule state")
                .sweep
                .cursor
                .clone();
            let ids = self
                .db
                .open_schedule_tasks(cursor.as_deref(), SWEEP_PAGE)
                .await?;
            let Some(last) = ids.last().cloned() else {
                let repairs = {
                    let mut state = self.schedule_state.lock().expect("schedule state");
                    state.sweep.active = false;
                    std::mem::take(&mut state.sweep.repairs)
                };
                self.db.complete_condition_pass(repairs);
                return Ok(());
            };
            self.db.check_task_conditions_of(&ids).await?;
            let (reads, unreadable) = self.db.schedule_reads_isolated(&ids).await?;
            let mut marked = Vec::new();
            let mut repairs = unreadable.len() as u64;
            for (id, error) in unreadable {
                tracing::warn!(task_id = %id, %error, "Task scheduling snapshot unreadable during the sweep");
                marked.push(id);
            }
            let now = chrono::Utc::now();
            for read in reads {
                if read.task.deleted_at.is_some() || read.task.archived_at.is_some() {
                    continue;
                }
                if !projects.contains_key(&read.task.project_id) {
                    let project = ProjectRepo::get_by_id(&*self.db, &read.task.project_id).await?;
                    projects.insert(read.task.project_id.clone(), project);
                }
                let Some(project) = &projects[&read.task.project_id] else {
                    continue;
                };
                let p = self.prepare_schedule(read, project, now);
                let next = next_step::next_step(&Snapshot {
                    state: &p.read.task.status,
                    condition: &p.read.condition,
                    workflow: &p.workflow,
                    facts: &p.facts,
                });
                let owned = p.read.queue_owned
                    || p.read.park_json.is_some()
                    || p.read.condition.is_blocked()
                    || p.read.executions.iter().any(|e| {
                        e.status == db::ExecutionStatus::Running && e.role != "interactive"
                    });
                if !owned && p.workflow.state_kind(&p.read.task.status) != Some(StateKind::Terminal)
                {
                    // Every Task that is not settled has a queued step, a
                    // live execution or a park. This one has none.
                    repairs += 1;
                    tracing::warn!(task_id = %p.read.task.id, "scheduler invariant repaired by reconciliation");
                    marked.push(p.read.task.id.clone());
                    continue;
                }
                // Work the kicks missed, or a park that no longer says why
                // the Task waits. A cached refusal resolves to its own park
                // and is left alone until a fact it depends on changes.
                let stale = match &next {
                    Next::Step(_) => true,
                    Next::Park(park) => {
                        p.read.park_json.as_deref() != Some(encode(park).as_str())
                            && !p.read.queue_owned
                    }
                };
                if stale {
                    marked.push(p.read.task.id.clone());
                }
            }
            {
                let mut state = self.schedule_state.lock().expect("schedule state");
                state.sweep.cursor = Some(last);
                state.sweep.repairs += repairs;
                state.sweep.pending.extend(marked);
            }
            if started.elapsed() >= budget {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    pub(super) fn schedule_sleep(&self) -> Duration {
        let state = self.schedule_state.lock().expect("schedule state");
        if !state.started {
            return self.check_interval;
        }
        if state.sweep.active() || state.sweep.has_pending() || state.sweep.backfilling {
            // A lap in progress continues in slices between dispatch passes.
            return Duration::from_millis(5);
        }
        let mut due = state.sweep.due;
        for other in [state.deadline, state.recheck_at].into_iter().flatten() {
            due = due.min(other);
        }
        due.saturating_duration_since(Instant::now())
            .min(self.check_interval)
    }
    async fn apply_schedule_step(
        &self,
        p: &Prepared,
        project: &Project,
        step: &Step,
    ) -> Result<u64> {
        let task = &p.read.task;
        match step {
            Step::RefreshPlacement => {
                self.task_service
                    .refresh_placement_dispatch_refusal(task.clone())
                    .await?;
                Ok(0)
            }
            Step::Publication => self.reconcile_publication_task(task).await,
            Step::QueuedRecovery => Box::pin(self.task_service.dispatch_queued_recovery(task))
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
            } => Box::pin(self.dispatch_initial_task(
                task,
                &super::initial_scheduling::InitialScheduleTarget {
                    transition_to: target.clone(),
                    role: role.clone(),
                    agent_id: agent_id.clone(),
                },
            ))
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
    /// Whether each candidate's target Agent can take work, read once per
    /// pass for every Agent the pass may dispatch to.
    async fn schedule_agent_statuses(&self, prepared: &[Prepared]) -> Result<AgentStatuses> {
        let ids: HashSet<String> = prepared
            .iter()
            .filter_map(|p| {
                p.facts
                    .initial_target
                    .as_ref()
                    .map(|(_, _, id)| id)
                    .or_else(|| p.facts.role_target.as_ref().map(|(_, id)| id))
                    .cloned()
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
        Ok(AgentStatuses { agents, statuses })
    }

    /// The facts a pure resolution cannot read, taken at the Task's own turn
    /// so that it sees what earlier Tasks of this pass did, as each Task of
    /// a scan did. For a role dispatch these are the gates and the machine
    /// precheck the scanning dispatcher ran before it looked for a running
    /// execution or finished Review checks: a Task in flight still has its
    /// capacity wait recorded and retired, and a refusal is written exactly
    /// as it was written there.
    ///
    /// Returns whether an admission check failed. What a refusal wrote is
    /// read back, so the Task resolves from it; a failure that wrote nothing
    /// is retried at the scan cadence and never mistaken for a park.
    async fn observe_admission(
        &self,
        p: &mut Prepared,
        project: &Project,
        agents: &AgentStatuses,
        gate: bool,
    ) -> bool {
        let mut failed = false;
        let probe = next_step::next_step(&Snapshot {
            state: &p.read.task.status,
            condition: &p.read.condition,
            workflow: &p.workflow,
            facts: &next_step::Facts {
                in_flight: false,
                reviewer_ready: true,
                ..p.facts.clone()
            },
        });
        if let (true, Next::Step(step @ Step::Role { .. })) = (gate, probe) {
            let Step::Role { role, agent_id } = step.clone() else {
                unreachable!("matched a role step")
            };
            // One Task's refusal or failure never ends the pass.
            let gated: Result<()> = async {
                let gate = if role == "reviewer" {
                    self.task_service.ensure_task_reviewable(&p.read.task).await
                } else {
                    self.task_service.ensure_task_runnable(&p.read.task).await
                };
                let waiting = match gate {
                    Err(error) => Err(error),
                    Ok(()) => match agents.agents.iter().find(|a| a.id == agent_id) {
                        Some(identity) => {
                            crate::placement::machine_precheck::wait_before_dispatch(
                                &self.db,
                                &self.task_service,
                                &p.read.task,
                                identity,
                                Some(&role),
                            )
                            .await
                        }
                        None => Ok(false),
                    },
                };
                match waiting {
                    Ok(true) => p.machine_wait = true,
                    Ok(false) => {}
                    Err(error) => {
                        failed = true;
                        self.handle_schedule_error(p, &step, &error).await?;
                    }
                }
                Ok(())
            }
            .await;
            if let Err(error) = gated {
                failed = true;
                tracing::warn!(task_id = %p.read.task.id, %error, "Task admission check failed; continuing with the next Task");
            }
            if failed {
                // A refusal may have recorded a disposition, a deferral or a
                // blocker. Resolve from what is stored now.
                match self.reread(p, project).await {
                    Ok(true) => {}
                    Ok(false) => return true,
                    Err(error) => {
                        tracing::warn!(task_id = %p.read.task.id, %error, "Task could not be re-read after a refused admission");
                    }
                }
            }
        }
        if p.machine_wait {
            p.facts.agent_full = true;
        } else if let Some(agent) = p
            .facts
            .initial_target
            .as_ref()
            .map(|(_, _, id)| id)
            .or_else(|| p.facts.role_target.as_ref().map(|(_, id)| id))
        {
            use crate::agent_service::EffectiveStatus;
            let status = agents.statuses.get(agent);
            p.facts.agent_unavailable = !matches!(
                status,
                Some(EffectiveStatus::Active | EffectiveStatus::Busy)
            );
            p.facts.agent_full = status == Some(&EffectiveStatus::Busy);
        }
        failed
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
        invalidated_wait: Option<&serde_json::Value>,
    ) -> Result<bool> {
        let Some(mut read) = self.db.schedule_reads(&[id.into()]).await?.pop() else {
            return Ok(false);
        };
        if read.task.deleted_at.is_some() || read.task.archived_at.is_some() {
            return Ok(false);
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
        if self.is_stopped() {
            return Ok(false);
        }
        match Box::pin(self.dispatch_resolved_role(
            &project,
            &p.workflow,
            &p.read.task,
            role,
            agent,
        ))
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
                    // Retried at the scan cadence, as the scanning dispatcher
                    // retried it. The Task's own fields are not touched.
                    self.db
                        .schedule_retry_at(
                            id,
                            &(chrono::Utc::now()
                                + chrono::Duration::from_std(self.check_interval)
                                    .unwrap_or_else(|_| chrono::Duration::seconds(10)))
                            .to_rfc3339(),
                        )
                        .await?;
                }
                Ok(false)
            }
        }
    }
    /// The scanning dispatcher's handling of a failed dispatch, branch for
    /// branch: the same refusal writes the same annotation, deferral or
    /// disposition, and a transient failure writes nothing.
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
            tracing::debug!(task_id = %task.id, "Task dispatch lost a version race");
            return Ok(());
        }
        let deterministic = super::helpers::is_deterministic_dispatch_refusal(error);
        if let Step::Initial { role, .. } = step {
            // Admission from an initial state recorded a deterministic
            // refusal and only logged anything else.
            if deterministic {
                let current = db::TaskRepo::get_by_id(&*self.db, &task.id, false)
                    .await?
                    .ok_or(db::DbError::NotFound)?;
                if current.status == task.status
                    && self
                        .task_service
                        .record_placement_dispatch_refusal(&current, error)
                        .await?
                {
                    return Ok(());
                }
                deferred_dispatch::record_dispatch_disposition(
                    &self.db,
                    task,
                    role,
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
                tracing::warn!(task_id = %task.id, %error, "task dispatch blocked; parked until Task/governance state changes or an explicit wake");
            } else {
                tracing::warn!(task_id = %task.id, %error, "task dispatcher initial dispatch failed");
            }
            return Ok(());
        }
        if matches!(error, ServiceError::WorkspaceResetRequired { .. }) {
            tracing::warn!(task_id = %task.id, %error, "task branch lost, blocking for user reset");
            self.block_task_for_workspace_reset(task, error).await?;
        } else if crate::placement::admission_refusal_is_retryable(&self.db, &task.id, error)
            .await?
        {
            let current = db::TaskRepo::get_by_id(&*self.db, &task.id, false)
                .await?
                .ok_or(db::DbError::NotFound)?;
            self.task_service
                .defer_placement_refusal(&current, error)
                .await?;
        } else if super::helpers::is_io_or_workspace_error(error) {
            tracing::error!(task_id = %task.id, %error, "task dispatcher recovery blocked task due to workspace error");
            self.block_task_on_workspace_error(task, error).await?;
        } else if deterministic {
            if self
                .task_service
                .record_placement_dispatch_refusal(task, error)
                .await?
            {
                return Ok(());
            }
            deferred_dispatch::record_dispatch_disposition(
                &self.db,
                task,
                &task.status,
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
            tracing::warn!(task_id = %task.id, %error, "task dispatch blocked; parked until Task/governance state changes or an explicit wake");
        } else {
            tracing::warn!(task_id = %task.id, %error, "task dispatcher recovery failed");
        }
        Ok(())
    }
}
