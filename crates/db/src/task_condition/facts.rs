//! Durable witness projection shared by producers and invariant repair.
//!
//! A stored condition carries every fact family it was built from as
//! witnesses, so a producer can carry the families its write cannot change
//! and load only the ones it can. [`ConditionFacts::load`] is the full
//! recompute: the mapping fallback, the backfill and the invariant check.
use super::*;
use serde_json::Value;
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConditionWitness {
    Integration {
        reason: IntegrationReason,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        handoff_ready: bool,
    },
    /// Always first. `since` is the entry's time, or the Task's creation.
    Entry {
        task_id: String,
        epoch: i64,
        transition_id: Option<String>,
        since: String,
        /// The state is an initial one: the scheduler admits from it without
        /// reading the entry barrier, so the barrier does not park here.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        initial: bool,
        #[serde(default)]
        human_wait: bool,
        #[serde(default)]
        review_wait: bool,
        #[serde(default)]
        review_failure: bool,
    },
    Step {
        step_id: String,
        epoch: i64,
    },
    /// The Task's newest running non-interactive execution, when the current
    /// entry owns it. A settled execution is history, not a witness.
    Execution {
        execution_id: String,
        role: String,
    },
    Budget {
        key: String,
        window_id: String,
        spent: i64,
    },
    Operation {
        operation_id: String,
        generation: i64,
    },
    /// `settled`: terminal in the inherited or the Project workflow.
    Child {
        task_id: String,
        status: String,
        settled: bool,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterialBlocker {
    pub requires_intervention: bool,
    pub interruption: Option<Value>,
}
/// Today's incident digest inputs for the Task's blocker: the interruption as
/// the `task.interruption_changed` event states it, minus what the digest
/// strips (the reporting execution among them), so an execution replacement
/// is not a new blocker. Witnesses, observations, leases and new condition
/// tags never enter this projection. No Attention reader switches.
pub fn material_blocker(condition: &TaskCondition) -> MaterialBlocker {
    if matches!(
        condition,
        TaskCondition::Entering { .. }
            | TaskCondition::Running { .. }
            | TaskCondition::Deferred { .. }
    ) || matches!(
        condition,
        TaskCondition::Parked {
            primary: ParkReason::Held { .. } | ParkReason::Capacity { .. },
            ..
        }
    ) {
        return MaterialBlocker {
            requires_intervention: false,
            interruption: None,
        };
    }
    condition
        .evidence()
        .material
        .clone()
        .unwrap_or(MaterialBlocker {
            requires_intervention: false,
            interruption: None,
        })
}
pub(super) fn legacy_material_blocker(
    a: Option<&str>,
    b: Option<&str>,
    f: Option<&str>,
) -> Option<MaterialBlocker> {
    if a.is_none() && b.is_none() && f.is_none() {
        return None;
    }
    let selected = f
        .map(|r| ("failed", r))
        .or_else(|| b.map(|r| ("blocked", r)))
        .or_else(|| a.map(|r| ("annotation", r)));
    let interruption = selected.map(|(source, raw)| {
        let value = crate::repository::parse_event_json_object(raw);
        let mut details = crate::repository::event_interruption_details(source, &value);
        crate::strip_attention_delivery_metadata(&mut details);
        details
    });
    Some(MaterialBlocker {
        requires_intervention: crate::task_interruption_requires_intervention(a, b, f),
        interruption,
    })
}

/// Terminal classification of one `workflow_definition` text. Parsed once per
/// distinct text; a Task write compares the text and never re-parses it.
pub(super) struct Terminals {
    has_states: bool,
    /// First declaration of each state name: whether its kind is terminal.
    states: BTreeMap<String, bool>,
    /// First declarations whose kind is initial.
    initial: Vec<String>,
    cancellation_state: String,
    human_states: BTreeMap<String, HumanState>,
}
#[derive(Clone)]
struct HumanState {
    role: Option<String>,
    gate: bool,
    requires: bool,
    optional: bool,
}
/// A definition text and its classification.
type Parsed = (Arc<str>, Arc<Terminals>);
impl Terminals {
    pub(super) fn of(definition: &str) -> Arc<Self> {
        static CACHE: OnceLock<Mutex<Vec<Parsed>>> = OnceLock::new();
        let cache = CACHE.get_or_init(Default::default);
        let mut cache = cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some((_, hit)) = cache.iter().find(|(text, _)| **text == *definition) {
            return hit.clone();
        }
        let workflow: Value = serde_json::from_str(definition).unwrap_or_default();
        let declared = workflow["states"].as_array();
        let mut states = BTreeMap::new();
        let mut initial = Vec::new();
        let mut human_states = BTreeMap::new();
        for state in declared.into_iter().flatten() {
            if let Some(name) = state["name"].as_str() {
                if !states.contains_key(name) && state["kind"] == "initial" {
                    initial.push(name.to_owned());
                }
                human_states
                    .entry(name.to_owned())
                    .or_insert_with(|| HumanState {
                        role: state["role"].as_str().map(str::to_owned),
                        gate: state["kind"] == "gate",
                        requires: state["gate_config"]["requires_user_approval"] == true,
                        optional: state["gate_config"]["optional_when_unassigned"] == true,
                    });
                states
                    .entry(name.to_owned())
                    .or_insert(state["kind"] == "terminal");
            }
        }
        let parsed = Arc::new(Self {
            has_states: declared.is_some_and(|states| !states.is_empty()),
            states,
            initial,
            human_states,
            cancellation_state: workflow["cancellation_state"]
                .as_str()
                .unwrap_or("cancelled")
                .to_owned(),
        });
        if cache.len() >= 64 {
            cache.remove(0);
        }
        cache.push((definition.into(), parsed.clone()));
        parsed
    }
    fn project_terminal(&self, state: &str) -> bool {
        if self.has_states {
            self.states.get(state).copied().unwrap_or(false)
        } else {
            matches!(state, "done" | "cancelled")
        }
    }
    /// The Task's own effective workflow: a subtask in a shared state uses the
    /// inherited subtask workflow, anything else the Project's.
    fn outcome(&self, state: &str, subtask: bool) -> Option<TerminalOutcome> {
        let inherited = subtask && matches!(state, "todo" | "in_progress" | "done" | "cancelled");
        let terminal = if inherited {
            matches!(state, "done" | "cancelled")
        } else {
            self.project_terminal(state)
        };
        terminal.then(|| {
            if state == self.cancellation_state {
                TerminalOutcome::Cancelled
            } else {
                TerminalOutcome::Completed
            }
        })
    }
    /// Whether the Task's own effective workflow admits from `state`: the
    /// inherited subtask workflow and the default Project workflow start in
    /// `todo`.
    fn initial(&self, state: &str, subtask: bool) -> bool {
        let inherited = subtask && matches!(state, "todo" | "in_progress" | "done" | "cancelled");
        if inherited || !self.has_states {
            state == "todo"
        } else {
            self.initial.iter().any(|name| name == state)
        }
    }
    /// Whether a Task or child in `state` classifies differently under `other`.
    pub(super) fn differs(&self, other: &Self, state: &str) -> bool {
        self.initial(state, false) != other.initial(state, false)
            || self.initial(state, true) != other.initial(state, true)
            || self.outcome(state, false) != other.outcome(state, false)
            || self.outcome(state, true) != other.outcome(state, true)
            || self.child_settled(state) != other.child_settled(state)
    }
    /// Legacy `subtask_is_terminal`: terminal in the inherited subtask
    /// workflow or in the Project workflow.
    fn child_settled(&self, state: &str) -> bool {
        matches!(state, "done" | "cancelled") || self.project_terminal(state)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConditionFacts {
    pub integration: Option<IntegrationReason>,
    pub integration_handoff_ready: bool,
    pub human_wait: bool,
    pub review_wait: bool,
    pub review_failure: bool,
    pub owner_park: Option<Value>,
    pub version: i64,
    pub task_id: String,
    pub state: String,
    pub epoch: i64,
    pub transition_id: Option<String>,
    pub since: String,
    pub terminal: Option<TerminalOutcome>,
    /// The state is an initial one in the Task's effective workflow.
    pub initial: bool,
    pub hooks: Option<String>,
    /// `(id, role)` of the newest running non-interactive execution, when
    /// the current entry owns it.
    pub execution: Option<(String, String)>,
    /// `(kind, window, spent)`. Witnessed only by an exhausted condition.
    pub budgets: Vec<(String, String, i64)>,
    /// `(operation, generation)` of every cancellation fencing the Task.
    pub operations: Vec<(String, i64)>,
    pub children_pending: bool,
    /// `(id, status, settled)` in sequence order, while the flag is pending.
    pub children: Vec<(String, String, bool)>,
}
impl TaskCondition {
    pub fn evidence(&self) -> &ConditionEvidence {
        match self {
            Self::Clear { evidence }
            | Self::Entering { evidence, .. }
            | Self::Running { evidence, .. }
            | Self::Deferred { evidence, .. }
            | Self::Parked { evidence, .. }
            | Self::Failed { evidence, .. }
            | Self::Settled { evidence, .. } => evidence,
        }
    }
    pub(super) fn evidence_mut(&mut self) -> &mut ConditionEvidence {
        match self {
            Self::Clear { evidence }
            | Self::Entering { evidence, .. }
            | Self::Running { evidence, .. }
            | Self::Deferred { evidence, .. }
            | Self::Parked { evidence, .. }
            | Self::Failed { evidence, .. }
            | Self::Settled { evidence, .. } => evidence,
        }
    }
    /// Refresh a stale legacy projection while carrying supported typed owners.
    /// Used by the scheduler's read snapshot before the repair sweep writes it.
    pub fn restate_legacy(&self, input: &LegacyConditionInput) -> Self {
        match ConditionFacts::recover(self) {
            Some(facts) => facts.condition(input),
            None => map_legacy_condition(input),
        }
    }
    pub fn reasons(&self) -> impl Iterator<Item = &ParkReason> {
        let (first, rest) = match self {
            Self::Parked {
                primary,
                additional,
                ..
            } => (Some(primary), additional.as_slice()),
            Self::Failed {
                failure,
                additional,
                ..
            } => (Some(failure), additional.as_slice()),
            _ => (None, [].as_slice()),
        };
        first.into_iter().chain(rest)
    }
    pub(super) fn budget_exhausted(&self) -> bool {
        self.reasons()
            .any(|reason| matches!(reason, ParkReason::BudgetExhausted { .. }))
    }
}

/// Legacy's `coordination_review_pending` reading of `metadata_json`: a JSON
/// `true`, as `as_bool` sees it. Anything else is not pending.
pub(super) fn children_pending(metadata: Option<&str>) -> bool {
    metadata.is_some_and(|raw| {
        raw.contains("coordination_review_pending")
            && serde_json::from_str::<Fields<'_>>(raw).is_ok_and(|fields| {
                fields
                    .get("coordination_review_pending")
                    .is_some_and(|raw| shape(raw) == Shape::True)
            })
    })
}

/// Which fact families one snapshot statement reads beside the Task row.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Families {
    /// Entry receipt, terminal classification and whether the parent watches.
    pub entry: bool,
    pub hooks: bool,
    pub execution: bool,
}
impl Families {
    pub(super) const ROW: Self = Self {
        entry: false,
        hooks: false,
        execution: false,
    };
    /// Every family one statement can read: an entry change, and the base of
    /// the full recompute.
    pub(super) const ENTRY: Self = Self {
        entry: true,
        hooks: true,
        execution: true,
    };
    pub(super) const HOOKS: Self = Self {
        entry: false,
        hooks: true,
        execution: false,
    };
    pub(super) const EXECUTION: Self = Self {
        entry: false,
        hooks: false,
        execution: true,
    };
    /// One cached statement text per combination.
    pub(super) fn sql(self) -> &'static str {
        static SQL: OnceLock<Vec<String>> = OnceLock::new();
        let index = usize::from(self.entry)
            | usize::from(self.hooks) << 1
            | usize::from(self.execution) << 2;
        &SQL.get_or_init(|| {
            (0..8usize)
                .map(|bits| {
                    let mut columns = String::from("t.error_annotation,t.blocked_json,t.failed_json,t.entry_barrier_json,t.metadata_json,t.condition_json,t.version,t.status,t.status_epoch,t.created_at,t.parent_task_id,t.project_id,(SELECT reason_json FROM task_schedule_park WHERE task_id=t.id AND epoch=t.status_epoch) AS owner_park,(SELECT status FROM review WHERE task_id=t.id ORDER BY attempt_number DESC,created_at DESC,id DESC LIMIT 1) AS human_review_status,(SELECT created_at FROM review WHERE task_id=t.id ORDER BY attempt_number DESC,created_at DESC,id DESC LIMIT 1) AS human_review_created_at");
                    let mut joins = String::new();
                    if bits & 1 != 0 {
                        // An engine or board entry logs its epoch and is the
                        // entry. Otherwise the entry is the newest unstamped
                        // receipt that changed state: a claim's receipt, or a
                        // pre-upgrade entry. A same-state receipt (a recovery
                        // marker) never is: it refreshes the entry token for
                        // executions, see `bind_latest`.
                        columns.push_str(",p.workflow_definition AS workflow,le.id AS entry_id,le.created_at AS entry_at,l0.id AS entry0_id,l0.created_at AS entry0_at,EXISTS(SELECT 1 FROM task pt WHERE pt.id=t.parent_task_id AND (instr(COALESCE(pt.metadata_json,''),'coordination_review_pending')>0 OR instr(pt.condition_json,'\"kind\":\"child\"')>0)) AS parent_watches");
                        joins.push_str(" JOIN project p ON p.id=t.project_id LEFT JOIN transition_log le ON le.id=(SELECT id FROM transition_log WHERE task_id=t.id AND to_state=t.status AND status_epoch=t.status_epoch ORDER BY created_at,rowid LIMIT 1) LEFT JOIN transition_log l0 ON l0.id=CASE WHEN le.id IS NULL THEN (SELECT id FROM transition_log WHERE task_id=t.id AND to_state=t.status AND status_epoch IS NULL AND from_state IS NOT to_state ORDER BY created_at DESC,rowid DESC LIMIT 1) END");
                    }
                    if bits & 2 != 0 {
                        // Exact status/epoch prevents old entries from fabricating hook ownership.
                        columns.push_str(",(SELECT id FROM task_step WHERE task_id=t.id AND expected_status=t.status AND expected_epoch=t.status_epoch AND kind='hooks' AND status IN ('pending','claimed') ORDER BY seq LIMIT 1) AS hooks");
                    }
                    if bits & 4 != 0 {
                        // The partial index holds only running executions, so
                        // this never walks the Task's execution history.
                        columns.push_str(",e.id AS execution_id,e.role AS execution_role,e.created_at AS execution_at,e.executor_config_snapshot_json AS execution_snapshot");
                        joins.push_str(" LEFT JOIN execution e ON e.id=(SELECT id FROM execution WHERE status='running' AND task_id=t.id AND role!='interactive' ORDER BY created_at DESC,id DESC LIMIT 1)");
                    }
                    format!("SELECT {columns} FROM task t{joins} WHERE t.id=?")
                })
                .collect()
        })[index]
    }
}

/// One Task row as a producer reads it, with the requested fact families.
pub(super) struct Snapshot {
    pub id: String,
    pub input: LegacyConditionInput,
    pub stored: Vec<u8>,
    pub version: i64,
    pub state: String,
    pub epoch: i64,
    pub created_at: String,
    pub parent: Option<String>,
    pub project: String,
    pub parent_watches: bool,
    families: Families,
    row: sqlx::sqlite::SqliteRow,
}
impl Snapshot {
    pub(super) async fn read(
        c: &mut SqliteConnection,
        id: &str,
        families: Families,
    ) -> Result<Option<Self>> {
        let query = families.sql();
        let row = match sqlx::query(query).bind(id).fetch_optional(&mut *c).await {
            Ok(row) => row,
            // Migration replay before the scheduler table was introduced.
            Err(sqlx::Error::Database(e))
                if e.message().contains("no such table: task_schedule_park") =>
            {
                let old = query.replace("(SELECT reason_json FROM task_schedule_park WHERE task_id=t.id AND epoch=t.status_epoch) AS owner_park", "NULL AS owner_park,(SELECT status FROM review WHERE task_id=t.id ORDER BY attempt_number DESC,created_at DESC,id DESC LIMIT 1) AS human_review_status,(SELECT created_at FROM review WHERE task_id=t.id ORDER BY attempt_number DESC,created_at DESC,id DESC LIMIT 1) AS human_review_created_at");
                sqlx::query(&old).bind(id).fetch_optional(&mut *c).await?
            }
            Err(e) => return Err(e.into()),
        };
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(Self {
            id: id.to_owned(),
            input: LegacyConditionInput::from_row(&row)?,
            stored: row.try_get(5)?,
            version: row.try_get(6)?,
            state: row.try_get(7)?,
            epoch: row.try_get(8)?,
            created_at: row.try_get(9)?,
            parent: row.try_get(10)?,
            project: row.try_get(11)?,
            parent_watches: families.entry && row.try_get::<bool, _>("parent_watches")?,
            families,
            row,
        }))
    }
    pub(super) fn children_pending(&self) -> bool {
        children_pending(self.input.metadata_json.as_deref())
    }
    /// Replace every family this snapshot read; carry the rest of `facts`.
    pub(super) async fn refresh(
        &self,
        c: &mut SqliteConnection,
        facts: &mut ConditionFacts,
    ) -> Result<()> {
        facts.owner_park = self
            .row
            .try_get::<Option<String>, _>("owner_park")?
            .and_then(|s| serde_json::from_str(&s).ok());
        facts.version = self.version;
        let mut latest_review_status: Option<String> = None;
        if self.families.entry || self.families.hooks || self.families.execution {
            latest_review_status = self.row.try_get("human_review_status")?;
            facts.review_wait = latest_review_status.as_deref() == Some("awaiting_human");
        }
        facts.task_id = self.id.clone();
        facts.state = self.state.clone();
        facts.epoch = self.epoch;
        if self.families.entry {
            facts.human_wait = self.human_wait(c, latest_review_status.as_deref()).await?;
            let workflow: String = self.row.try_get("workflow")?;
            let classes = Terminals::of(&workflow);
            facts.terminal = classes.outcome(&self.state, self.parent.is_some());
            facts.initial = classes.initial(&self.state, self.parent.is_some());
            facts.since = self.created_at.clone();
            facts.transition_id = None;
            // Same-epoch audit receipts never replace the first receipt.
            if let Some(entry) = self.row.try_get::<Option<String>, _>("entry_id")? {
                facts.transition_id = Some(entry);
                if self.epoch != 0 {
                    facts.since = self.row.try_get("entry_at")?;
                }
            } else if let Some(entry) = self.row.try_get::<Option<String>, _>("entry0_id")? {
                facts.transition_id = Some(entry);
                facts.since = self.row.try_get("entry0_at")?;
            }
        }
        if self.families.entry || self.families.hooks || self.families.execution {
            facts.review_failure = latest_review_status.as_deref() == Some("failed")
                && self
                    .row
                    .try_get::<Option<String>, _>("human_review_created_at")?
                    .is_some_and(|at| at >= facts.since);
        }
        if self.families.hooks {
            facts.human_wait = self.human_wait(c, latest_review_status.as_deref()).await?;
            facts.hooks = self.row.try_get("hooks")?;
        }
        if self.families.execution {
            facts.human_wait = self.human_wait(c, latest_review_status.as_deref()).await?;
            facts.execution = None;
            if let Some(execution) = self.row.try_get::<Option<String>, _>("execution_id")? {
                let snapshot: Value = self
                    .row
                    .try_get::<Option<String>, _>("execution_snapshot")?
                    .as_deref()
                    .and_then(|raw| serde_json::from_str(raw).ok())
                    .unwrap_or_default();
                // An execution names the receipt it was admitted under. It
                // belongs to this entry when that receipt is the entry, or a
                // later receipt of it: stamped with this epoch, or unstamped
                // (a recovery marker, a same-state claim) and not older than
                // the entry.
                let bound = if let Some(token) = snapshot["state_entry_token"].as_str() {
                    snapshot["task_state"] == facts.state
                        && (facts.transition_id.as_deref() == Some(token)
                            || sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM transition_log WHERE id=?1 AND task_id=?2 AND to_state=?3 AND (status_epoch=?4 OR (status_epoch IS NULL AND created_at>=?5)))")
                                .bind(token)
                                .bind(&facts.task_id)
                                .bind(&facts.state)
                                .bind(facts.epoch)
                                .bind(&facts.since)
                                .fetch_one(&mut *c)
                                .await?)
                } else {
                    self.row.try_get::<String, _>("execution_at")? >= facts.since
                };
                // The entry-bound execution is the durable owner. Its role
                // is captured from admission, not re-guessed from today's
                // Project definition (subtasks and custom workflows differ).
                if bound {
                    facts.execution = Some((execution, self.row.try_get("execution_role")?));
                }
            }
        }
        Ok(())
    }
}

impl ConditionFacts {
    /// The full recompute: every family, read in the caller's transaction.
    pub async fn load(c: &mut SqliteConnection, id: &str) -> Result<Self> {
        let snapshot = Snapshot::read(c, id, Families::ENTRY)
            .await?
            .ok_or(DbError::NotFound)?;
        Self::load_all(c, &snapshot).await
    }
    pub(super) async fn load_all(c: &mut SqliteConnection, snapshot: &Snapshot) -> Result<Self> {
        let stored = std::str::from_utf8(&snapshot.stored)
            .ok()
            .and_then(|raw| decode(raw).ok());
        let mut facts = Self {
            integration: stored
                .as_ref()
                .and_then(|stored| stored.integration_reason().cloned()),
            integration_handoff_ready: stored
                .as_ref()
                .is_some_and(TaskCondition::integration_handoff_ready),
            ..Self::default()
        };
        snapshot.refresh(c, &mut facts).await?;
        facts.load_budgets(c).await?;
        facts.load_operations(c).await?;
        facts.children_pending = snapshot.children_pending();
        if facts.children_pending {
            facts.load_children(c, &snapshot.project).await?;
        }
        Ok(facts)
    }
    pub(super) async fn load_budgets(&mut self, c: &mut SqliteConnection) -> Result<()> {
        self.budgets = sqlx::query_as(
            "SELECT kind,window_id,spent FROM task_budget WHERE task_id=? AND spent>0 ORDER BY kind",
        )
        .bind(&self.task_id)
        .fetch_all(&mut *c)
        .await?;
        Ok(())
    }
    /// Legacy's fence, not a looser one: see [`crate::remote_cancel::TASK_FENCE`].
    pub(super) async fn load_operations(&mut self, c: &mut SqliteConnection) -> Result<()> {
        self.operations = sqlx::query_as(&format!(
            "SELECT DISTINCT r.operation_id,r.generation {} ORDER BY r.operation_id",
            crate::remote_cancel::TASK_FENCE
        ))
        .bind(&self.task_id)
        .fetch_all(&mut *c)
        .await?;
        Ok(())
    }
    pub(super) async fn load_children(
        &mut self,
        c: &mut SqliteConnection,
        project: &str,
    ) -> Result<()> {
        let children: Vec<(String, String)> = sqlx::query_as("SELECT id,status FROM task WHERE parent_task_id=? AND deleted_at IS NULL ORDER BY subtask_order,id")
            .bind(&self.task_id)
            .fetch_all(&mut *c)
            .await?;
        self.children.clear();
        if children.is_empty() {
            return Ok(());
        }
        let workflow: String =
            sqlx::query_scalar("SELECT workflow_definition FROM project WHERE id=?")
                .bind(project)
                .fetch_one(&mut *c)
                .await?;
        let terminals = Terminals::of(&workflow);
        self.children = children
            .into_iter()
            .map(|(id, status)| {
                let settled = terminals.child_settled(&status);
                (id, status, settled)
            })
            .collect();
        Ok(())
    }
    /// The facts a stated condition was built from. `None` for a condition no
    /// producer of this revision wrote (column default, older encoding).
    pub(super) fn recover(condition: &TaskCondition) -> Option<Self> {
        let evidence = condition.evidence();
        Self::carried(
            match condition {
                TaskCondition::Settled { outcome, .. } => Some(outcome.clone()),
                _ => None,
            },
            &evidence.witnesses,
        )
    }
    /// [`Self::recover`] straight from the stored text. Only the carried
    /// facts are decoded: the legacy evidence copies are skipped, not parsed.
    pub(super) fn recover_stored(stored: &[u8]) -> Option<Self> {
        #[derive(Deserialize)]
        struct Stored {
            #[serde(default)]
            outcome: Option<TerminalOutcome>,
            evidence: StoredEvidence,
        }
        #[derive(Deserialize)]
        struct StoredEvidence {
            #[serde(default)]
            witnesses: Vec<ConditionWitness>,
        }
        let stored: Stored = serde_json::from_slice(stored).ok()?;
        Self::carried(stored.outcome, &stored.evidence.witnesses)
    }
    fn carried(terminal: Option<TerminalOutcome>, witnesses: &[ConditionWitness]) -> Option<Self> {
        let mut witnesses = witnesses.iter();
        let Some(ConditionWitness::Entry {
            task_id,
            epoch,
            transition_id,
            since,
            initial,
            human_wait,
            review_wait,
            review_failure,
        }) = witnesses.next()
        else {
            return None;
        };
        let mut facts = Self {
            task_id: task_id.clone(),
            epoch: *epoch,
            transition_id: transition_id.clone(),
            since: since.clone(),
            terminal,
            initial: *initial,
            human_wait: *human_wait,
            review_wait: *review_wait,
            review_failure: *review_failure,
            ..Default::default()
        };
        for witness in witnesses {
            match witness {
                ConditionWitness::Integration {
                    reason,
                    handoff_ready,
                } => {
                    facts.integration_handoff_ready = *handoff_ready;
                    if facts.integration.replace(reason.clone()).is_some() {
                        return None;
                    }
                }
                ConditionWitness::Entry { .. } => return None,
                ConditionWitness::Step { step_id, .. } => facts.hooks = Some(step_id.clone()),
                ConditionWitness::Execution { execution_id, role } => {
                    facts.execution = Some((execution_id.clone(), role.clone()))
                }
                ConditionWitness::Budget {
                    key,
                    window_id,
                    spent,
                } => facts.budgets.push((key.clone(), window_id.clone(), *spent)),
                ConditionWitness::Operation {
                    operation_id,
                    generation,
                } => facts.operations.push((operation_id.clone(), *generation)),
                ConditionWitness::Child {
                    task_id,
                    status,
                    settled,
                } => facts
                    .children
                    .push((task_id.clone(), status.clone(), *settled)),
            }
        }
        Some(facts)
    }
    fn witnesses(&self, exhausted: bool) -> Vec<ConditionWitness> {
        let mut witnesses = vec![ConditionWitness::Entry {
            task_id: self.task_id.clone(),
            epoch: self.epoch,
            transition_id: self.transition_id.clone(),
            since: self.since.clone(),
            initial: self.initial,
            human_wait: self.human_wait,
            review_wait: self.review_wait,
            review_failure: self.review_failure,
        }];
        if let Some(reason) = &self.integration {
            witnesses.push(ConditionWitness::Integration {
                reason: reason.clone(),
                handoff_ready: self.integration_handoff_ready,
            });
        }
        if let Some(step) = &self.hooks {
            witnesses.push(ConditionWitness::Step {
                step_id: step.clone(),
                epoch: self.epoch,
            });
        }
        if let Some((execution, role)) = &self.execution {
            witnesses.push(ConditionWitness::Execution {
                execution_id: execution.clone(),
                role: role.clone(),
            });
        }
        if exhausted {
            witnesses.extend(self.budgets.iter().map(|(key, window, spent)| {
                ConditionWitness::Budget {
                    key: key.clone(),
                    window_id: window.clone(),
                    spent: *spent,
                }
            }));
        }
        witnesses.extend(self.operations.iter().map(|(operation, generation)| {
            ConditionWitness::Operation {
                operation_id: operation.clone(),
                generation: *generation,
            }
        }));
        if self.children_pending {
            witnesses.extend(self.children.iter().map(|(id, status, settled)| {
                ConditionWitness::Child {
                    task_id: id.clone(),
                    status: status.clone(),
                    settled: *settled,
                }
            }));
        }
        witnesses
    }
    /// The condition these facts and the legacy fields state together.
    pub fn condition(&self, input: &LegacyConditionInput) -> TaskCondition {
        self.condition_of(&input.view())
    }
    pub(super) fn condition_of(&self, view: &LegacyView<'_>) -> TaskCondition {
        self.apply(map_view_from(self.initial, view))
    }
    /// Combine the legacy mapping with these facts. Legacy is authoritative:
    /// a condition parks exactly where today's readers hold the Task.
    pub fn apply(&self, condition: TaskCondition) -> TaskCondition {
        let condition = self.apply_lifecycle(super::readers::apply_owner_park(
            integration::without_reason(condition),
            self.owner_park.as_ref(),
        ));
        match &self.integration {
            Some(reason) => integration::overlay(condition, reason, self.integration_handoff_ready),
            None => condition,
        }
    }
    fn apply_lifecycle(&self, mut condition: TaskCondition) -> TaskCondition {
        let exhausted = condition.budget_exhausted();
        let children = self.children_pending && !self.children.is_empty();
        {
            let evidence = condition.evidence_mut();
            evidence.witnesses = self.witnesses(exhausted);
            if let Some(read) = &mut evidence.presentation {
                read.human_wait = read.interruption_present
                    || (self.hooks.is_none() && (read.explicit_human_wait || self.human_wait));
                read.review_wait = self.review_wait;
                read.review_failure = self.review_failure;
            }
            if self.children_pending && !children {
                // Legacy dispatches a flagged root without visible children as
                // an ordinary Task. Keep the diagnosis; do not park.
                evidence.observations.push(unknown(
                    &LegacyConditionField::MetadataJson,
                    Some("coordination_review_pending"),
                    UnknownConditionProblem::UnownedEntry,
                ));
            }
        }
        if let Some(outcome) = &self.terminal {
            return TaskCondition::Settled {
                outcome: outcome.clone(),
                evidence: std::mem::take(condition.evidence_mut()),
            };
        }
        let remote_cancel = !self.operations.is_empty();
        let remote_reason = ParkReason::RemoteCancelPending {
            source: source(&LegacyConditionField::PendingRemoteCancel, None),
        };
        let remaining = || {
            self.children
                .iter()
                .filter(|(_, _, settled)| !settled)
                .map(|(id, _, _)| id.clone())
                .collect::<Vec<_>>()
        };
        if condition.is_blocked() {
            let already_remote = condition
                .reasons()
                .any(|reason| matches!(reason, ParkReason::RemoteCancelPending { .. }));
            let (TaskCondition::Parked { additional, .. }
            | TaskCondition::Failed { additional, .. }) = &mut condition
            else {
                unreachable!("a blocked condition is parked or failed")
            };
            if remote_cancel && !already_remote {
                additional.push(remote_reason);
            }
            if children {
                let remaining = remaining();
                if !remaining.is_empty() {
                    additional.push(ParkReason::Children {
                        root_id: self.task_id.clone(),
                        remaining,
                    });
                }
            }
            return condition;
        }
        let lifecycle =
            remote_cancel || self.hooks.is_some() || children || self.execution.is_some();
        if !lifecycle {
            return condition;
        }
        let evidence = std::mem::take(condition.evidence_mut());
        if remote_cancel {
            return TaskCondition::Parked {
                primary: remote_reason,
                additional: Vec::new(),
                resume: ConditionContinuation::Reconcile,
                since: Some(self.since.clone()),
                evidence,
            };
        }
        if let Some(step) = &self.hooks {
            return TaskCondition::Entering {
                state: self.state.clone(),
                epoch: self.epoch,
                step_id: step.clone(),
                phase: "post_commit_hooks".into(),
                since: self.since.clone(),
                evidence,
            };
        }
        if let Some((execution, role)) = &self.execution {
            return TaskCondition::Running {
                execution_id: execution.clone(),
                role: role.clone(),
                epoch: self.epoch,
                since: self.since.clone(),
                evidence,
            };
        }
        if children {
            let remaining = remaining();
            if remaining.is_empty() {
                return TaskCondition::Deferred {
                    until: None,
                    reason: RetryCause::ChildrenReady,
                    resume: ConditionContinuation::AdvanceAggregateReview {
                        child_ids: self.children.iter().map(|(id, _, _)| id.clone()).collect(),
                    },
                    evidence,
                };
            }
            return TaskCondition::Parked {
                primary: ParkReason::Children {
                    root_id: self.task_id.clone(),
                    remaining,
                },
                additional: Vec::new(),
                resume: ConditionContinuation::Reconcile,
                since: Some(self.since.clone()),
                evidence,
            };
        }
        unreachable!("a lifecycle fact always states a condition")
    }
}

impl Snapshot {
    /// One read of the canonical human-work facts, captured by the producer.
    /// No public reader reconstructs them or reads migrated metadata.
    async fn human_wait(
        &self,
        c: &mut SqliteConnection,
        latest_review_status: Option<&str>,
    ) -> Result<bool> {
        let hooks = if self.families.hooks {
            self.row.try_get::<Option<String>, _>("hooks")?.is_some()
        } else {
            ConditionFacts::recover_stored(&self.stored).is_some_and(|f| f.hooks.is_some())
        };
        if hooks {
            return Ok(false);
        }
        if self.state == "review" {
            if latest_review_status == Some("awaiting_human") {
                return Ok(true);
            }
            if latest_review_status == Some("failed") {
                let running: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM execution WHERE task_id=? AND status='running')",
                )
                .bind(&self.id)
                .fetch_one(&mut *c)
                .await?;
                if !running {
                    return Ok(true);
                }
            }
        }
        if self.parent.is_some()
            && matches!(
                self.state.as_str(),
                "todo" | "in_progress" | "done" | "cancelled"
            )
        {
            return Ok(false);
        }
        let workflow: String = if self.families.entry {
            self.row.try_get("workflow")?
        } else {
            sqlx::query_scalar("SELECT workflow_definition FROM project WHERE id=?")
                .bind(&self.project)
                .fetch_one(&mut *c)
                .await?
        };
        let classes = Terminals::of(&workflow);
        let state = if classes.has_states {
            classes.human_states.get(&self.state).cloned()
        } else {
            match self.state.as_str() {
                "planning" => Some(HumanState {
                    role: Some("planner".into()),
                    gate: true,
                    requires: false,
                    optional: true,
                }),
                "review" => Some(HumanState {
                    role: Some("reviewer".into()),
                    gate: true,
                    requires: false,
                    optional: false,
                }),
                _ => None,
            }
        };
        let Some(state) = state else {
            return Ok(false);
        };
        if self.state != "planning" && !state.gate {
            return Ok(false);
        }
        let role = state.role.as_deref();
        let assignment = if let Some(role) = role {
            sqlx::query_as::<_,(Option<String>,Option<String>)>("SELECT assignee_type,assignee_id FROM task_role_assignment WHERE task_id=? AND role_name=?").bind(&self.id).bind(role).fetch_optional(&mut *c).await?
        } else {
            None
        };
        let user = assignment
            .as_ref()
            .is_some_and(|(kind, id)| kind.as_deref() == Some("user") && id.is_some());
        if self.state == "planning" {
            return Ok(user);
        }
        if !state.gate {
            return Ok(false);
        }
        if !user && !state.requires {
            return Ok(false);
        }
        let entry = sqlx::query_scalar::<_,String>("SELECT created_at FROM transition_log WHERE task_id=? AND to_state=? ORDER BY created_at DESC,rowid DESC LIMIT 1").bind(&self.id).bind(&self.state).fetch_optional(&mut *c).await?.unwrap_or_else(||self.created_at.clone());
        let decided: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM transition_log WHERE task_id=? AND from_state=? AND created_at>=? AND bridge_kind IN ('gate_approved','gate_rejected'))").bind(&self.id).bind(&self.state).bind(entry).fetch_one(&mut *c).await?;
        if state.requires {
            if state.optional
                && !assignment
                    .as_ref()
                    .is_some_and(|(kind, id)| kind.is_some() && id.is_some())
            {
                return Ok(false);
            }
            return Ok(!decided);
        }
        Ok(user && !decided)
    }
}
