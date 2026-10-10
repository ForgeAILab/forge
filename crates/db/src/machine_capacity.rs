use sqlx::{Row, Sqlite, Transaction};

use crate::Result;

/// Runtime-provided server policy. Bare databases are unlimited; configuration
/// resolution and hardware discovery belong to the runtime, not persistence.
#[derive(Debug, Default)]
pub struct MachineRunCap(std::sync::RwLock<RunCapState>);
#[derive(Debug, Default)]
struct RunCapState {
    configured: Option<u32>,
    effective: Option<i64>,
    embedded_machine_id: String,
}
impl MachineRunCap {
    /// Runtime composition supplies identity without changing a bare DB's policy.
    pub fn initialize_identity(&self, id: &str) {
        let mut state = self.0.write().unwrap_or_else(|poison| poison.into_inner());
        if state.embedded_machine_id.is_empty() {
            state.embedded_machine_id = id.to_owned();
        }
    }
    pub fn set(&self, configured: Option<u32>, resolved: u32, embedded_machine_id: &str) {
        *self.0.write().unwrap_or_else(|poison| poison.into_inner()) = RunCapState {
            configured,
            effective: (resolved > 0).then_some(i64::from(resolved)),
            embedded_machine_id: embedded_machine_id.to_owned(),
        };
    }
    pub fn configured(&self) -> Option<u32> {
        self.0
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .configured
    }
    pub fn effective(&self) -> Option<i64> {
        self.0
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .effective
    }
    pub fn embedded_machine_id(&self) -> String {
        self.0
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .embedded_machine_id
            .clone()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentCapacity {
    pub running_executions: i64,
    pub reservations: i64,
}

impl AgentCapacity {
    pub fn occupied_slots(self) -> i64 {
        self.running_executions.saturating_add(self.reservations)
    }

    pub fn has_capacity(self, max_concurrent_tasks: i64) -> bool {
        self.occupied_slots() < max_concurrent_tasks
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MachineCapacity {
    pub running_executions: i64,
    pub reservations: i64,
    pub active_chat_turns: i64,
    pub check_runs: i64,
    /// Informational: these checks share an already occupied Task slot.
    pub borrowed_check_runs: i64,
    /// Queued checks on this machine that a consumer still waits for. They
    /// hold no slot; an execution admission leaves room for them.
    pub queued_checks: i64,
    pub max_concurrent_runs: Option<i64>,
}

impl MachineCapacity {
    pub fn active_runs(self) -> i64 {
        self.running_executions
            .saturating_add(self.reservations)
            .saturating_add(self.active_chat_turns)
            .saturating_add(self.check_runs)
    }

    /// A free slot, whoever takes it. This is the check runner's question and
    /// what Operations shows; an execution asks [`Self::admits_execution`].
    pub fn has_capacity(self) -> bool {
        self.max_concurrent_runs
            .filter(|limit| *limit > 0)
            .is_none_or(|limit| self.active_runs() < limit)
    }

    /// The slots checks may hold on their own while a Task waits for a run
    /// slot here: all but one, and the one slot of a machine that has one.
    pub fn check_share(self) -> Option<i64> {
        self.max_concurrent_runs
            .filter(|limit| *limit > 0)
            .map(|limit| (limit - 1).max(1))
    }

    /// Free slots a new execution must leave for queued checks: work closer
    /// to done goes first, up to the check share.
    pub fn slots_kept_for_checks(self) -> i64 {
        self.check_share().map_or(0, |share| {
            self.queued_checks
                .min(share.saturating_sub(self.check_runs).max(0))
        })
    }

    /// Whether a new execution (reservation or start) may take a slot.
    pub fn admits_execution(self) -> bool {
        self.max_concurrent_runs
            .filter(|limit| *limit > 0)
            .is_none_or(|limit| {
                self.active_runs()
                    .saturating_add(self.slots_kept_for_checks())
                    < limit
            })
    }

    /// Whether a queued check may take a slot of its own. `queued_ahead`:
    /// the checks before it in the queue, which get the free slots first.
    /// `run_waiters`: a Task waits for a run slot on this machine, so checks
    /// stay within their share and one slot keeps turning over for executions.
    pub fn admits_check(self, queued_ahead: i64, run_waiters: bool) -> bool {
        self.max_concurrent_runs
            .filter(|limit| *limit > 0)
            .is_none_or(|limit| self.active_runs().saturating_add(queued_ahead) < limit)
            && !(run_waiters
                && self
                    .check_share()
                    .is_some_and(|share| self.check_runs >= share))
    }
}

/// Missing or zero reported cap contributes no ceiling. Admin limits are positive.
pub fn effective_machine_cap(reported: Option<i64>, admin: Option<u32>) -> Option<i64> {
    match (
        reported.filter(|cap| *cap > 0),
        admin.filter(|cap| *cap > 0).map(i64::from),
    ) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(cap), None) | (None, Some(cap)) => Some(cap),
        (None, None) => None,
    }
}

/// The embedded daemon and direct server execution share the server setting
/// and any administrator ceiling on the embedded machine's row.
pub async fn server_machine_cap(
    db: &crate::SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
) -> Result<Option<i64>> {
    let admin: Option<i64> =
        sqlx::query_scalar("SELECT MIN(run_limit) FROM daemon WHERE machine_id = ?")
            .bind(db.server_run_cap.embedded_machine_id())
            .fetch_one(&mut **tx)
            .await?;
    Ok(match (db.server_run_cap.effective(), admin) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    })
}

/// Call before inserting the reservation, and again before inserting the
/// Running execution, in the caller's BEGIN IMMEDIATE transaction. A ready
/// placement between turns consumes no slot; an expired reservation releases
/// its slot immediately in the count, without requiring a sweep.
pub async fn count_agent_capacity(
    transaction: &mut Transaction<'_, Sqlite>,
    agent_id: &str,
) -> Result<AgentCapacity> {
    let row = sqlx::query(
        "SELECT
            (SELECT COUNT(*) FROM execution
             WHERE agent_id = ? AND status = 'running') AS running_executions,
            (SELECT COUNT(*) FROM workspace_placement p
             WHERE p.agent_id = ? AND p.state IN ('reserved', 'preparing')
               AND julianday(COALESCE(p.reserved_until, datetime(p.updated_at, '+10 minutes'))) > julianday('now')
               AND NOT EXISTS (SELECT 1 FROM execution e
                               WHERE e.workspace_id = p.workspace_id
                                 AND e.status = 'running')) AS reservations",
    )
    .bind(agent_id)
    .bind(agent_id)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(AgentCapacity {
        running_executions: row.try_get("running_executions")?,
        reservations: row.try_get("reservations")?,
    })
}

/// Session routing comes from the placement, including unpinned Agents and
/// server-owned shared mounts. Agent pins are used only for Agent Chat turns,
/// which have no workspace placement. Running executions are never counted
/// twice through a reservation for the same workspace.
pub async fn count_machine_capacity(
    transaction: &mut Transaction<'_, Sqlite>,
    daemon_id: Option<&str>,
    max_concurrent_runs: Option<i64>,
    embedded_machine_id: &str,
) -> Result<MachineCapacity> {
    let sql = format!("{} SELECT COALESCE(SUM(running_executions), 0) AS running_executions,
        COALESCE(SUM(reservations), 0) AS reservations, COALESCE(SUM(active_chat_turns), 0) AS active_chat_turns,
        COALESCE(SUM(check_runs), 0) AS check_runs, COALESCE(SUM(borrowed_check_runs), 0) AS borrowed_check_runs,
        COALESCE(SUM(queued_checks), 0) AS queued_checks
        FROM occupancy WHERE machine_key = COALESCE(?, 'server_host')", include_str!("machine_occupancy.sql"));
    let row = sqlx::query(&sql)
        .bind(embedded_machine_id)
        .bind(daemon_id)
        .fetch_one(&mut **transaction)
        .await?;
    Ok(MachineCapacity {
        running_executions: row.try_get("running_executions")?,
        reservations: row.try_get("reservations")?,
        active_chat_turns: row.try_get("active_chat_turns")?,
        check_runs: row.try_get("check_runs")?,
        borrowed_check_runs: row.try_get("borrowed_check_runs")?,
        queued_checks: row.try_get("queued_checks")?,
        max_concurrent_runs,
    })
}

/// Whether this run can borrow the same Task's existing physical-machine slot.
/// Evaluate inside the same BEGIN IMMEDIATE as admission. Chat cannot lend.
pub async fn check_borrows_machine_slot(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &str,
    machine: Option<&str>,
    embedded_machine_id: &str,
) -> Result<bool> {
    let sql=format!("{} SELECT EXISTS(SELECT 1 FROM physical_jobs p JOIN check_consumer c ON c.task_id=p.task_id JOIN task t ON t.id=c.task_id AND t.status_epoch=c.status_epoch AND t.deleted_at IS NULL WHERE c.run_id=? AND c.cancelled_at IS NULL AND (p.running_executions=1 OR p.reservations=1) AND p.machine_key=COALESCE(?,'server_host'))",include_str!("machine_occupancy.sql"));
    Ok(sqlx::query_scalar(&sql)
        .bind(embedded_machine_id)
        .bind(run_id)
        .bind(machine)
        .fetch_one(&mut **tx)
        .await?)
}

/// How many queued checks of the same machine are ahead of this one. Checks
/// are admitted in queue order: a later one takes a slot only when the
/// earlier ones still leave it one.
pub async fn checks_queued_ahead(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &str,
    embedded_machine_id: &str,
) -> Result<i64> {
    let sql = format!(
        "{} SELECT COUNT(*) FROM check_queue me JOIN check_queue other
            ON other.machine_key=me.machine_key AND other.id<>me.id AND other.queue_rank IS NOT NULL
            AND (other.queue_rank, other.created_at, other.id) < (COALESCE(me.queue_rank, 2), me.created_at, me.id)
            WHERE me.id=?",
        include_str!("machine_occupancy.sql")
    );
    Ok(sqlx::query_scalar(&sql)
        .bind(embedded_machine_id)
        .bind(run_id)
        .fetch_one(&mut **tx)
        .await?)
}

/// Whether a Task waits for a run slot this machine could give. Only the
/// scheduler's run-slot waits count (`daemon_id = '*'`): a Task that waits
/// for a machine's readiness or reconnect asks for no slot. A waiter whose
/// worktree lives on another machine cannot use a slot here, and a Task with
/// a running execution holds a slot rather than waiting for one.
pub async fn run_slot_waiters(
    tx: &mut Transaction<'_, Sqlite>,
    machine: Option<&str>,
    embedded_machine_id: &str,
) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_schedule_wait w
            JOIN task t ON t.id=w.task_id AND t.deleted_at IS NULL
            LEFT JOIN workspace_placement p ON p.task_id=COALESCE(t.parent_task_id,t.id)
                AND p.state<>'cleaned'
            WHERE w.daemon_id='*'
              AND NOT EXISTS(SELECT 1 FROM execution e WHERE e.task_id=t.id AND e.status='running')
              AND (p.id IS NULL OR CASE
                    WHEN COALESCE(p.execution_daemon_id,p.daemon_id) IS NULL
                      OR COALESCE(p.execution_daemon_id,p.daemon_id) IN (SELECT id FROM daemon WHERE machine_id=?)
                    THEN 'server_host' ELSE COALESCE(p.execution_daemon_id,p.daemon_id) END
                  = COALESCE(?, 'server_host')))",
    )
    .bind(embedded_machine_id)
    .bind(machine)
    .fetch_one(&mut **tx)
    .await?)
}

#[derive(Debug)]
pub struct MachineCapacityRow {
    pub daemon_id: Option<String>,
    pub hostname: String,
    pub capacity: MachineCapacity,
}

/// One read-only snapshot for the dispatcher and Operations, including idle machines.
pub async fn list_machine_capacity(
    transaction: &mut Transaction<'_, Sqlite>,
    embedded_machine_id: &str,
    server_cap: Option<i64>,
) -> Result<Vec<MachineCapacityRow>> {
    let sql = format!("{}, machines AS (
        SELECT 'server_host' AS machine_key, NULL AS daemon_id, 'Server host' AS hostname,
            ? AS reported, MIN(run_limit) AS admin FROM daemon WHERE id IN (SELECT id FROM embedded)
        UNION ALL SELECT id, id, hostname, max_concurrent_runs, run_limit FROM daemon WHERE removed_at IS NULL AND id NOT IN (SELECT id FROM embedded)
    ) SELECT m.*, COALESCE(o.running_executions, 0) AS running_executions,
        COALESCE(o.reservations, 0) AS reservations, COALESCE(o.active_chat_turns, 0) AS active_chat_turns,
        COALESCE(o.check_runs, 0) AS check_runs, COALESCE(o.borrowed_check_runs, 0) AS borrowed_check_runs,
        COALESCE(o.queued_checks, 0) AS queued_checks
        FROM machines m LEFT JOIN occupancy o USING(machine_key)", include_str!("machine_occupancy.sql"));
    let rows = sqlx::query(&sql)
        .bind(embedded_machine_id)
        .bind(server_cap)
        .fetch_all(&mut **transaction)
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(MachineCapacityRow {
                daemon_id: row.try_get("daemon_id")?,
                hostname: row.try_get("hostname")?,
                capacity: MachineCapacity {
                    running_executions: row.try_get("running_executions")?,
                    reservations: row.try_get("reservations")?,
                    active_chat_turns: row.try_get("active_chat_turns")?,
                    check_runs: row.try_get("check_runs")?,
                    borrowed_check_runs: row.try_get("borrowed_check_runs")?,
                    queued_checks: row.try_get("queued_checks")?,
                    max_concurrent_runs: effective_machine_cap(
                        row.try_get("reported")?,
                        row.try_get("admin")?,
                    ),
                },
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::MachineCapacity;

    fn machine(cap: i64, executions: i64, checks: i64, queued: i64) -> MachineCapacity {
        MachineCapacity {
            running_executions: executions,
            check_runs: checks,
            queued_checks: queued,
            max_concurrent_runs: Some(cap),
            ..Default::default()
        }
    }

    #[test]
    fn executions_leave_room_for_queued_checks_up_to_the_check_share() {
        // No queued check: plain capacity.
        assert!(machine(2, 1, 0, 0).admits_execution());
        assert!(!machine(2, 2, 0, 0).admits_execution());
        // A queued check keeps the free slot of a two-slot machine...
        assert!(!machine(2, 1, 0, 1).admits_execution());
        // ...until checks hold their share (all but one slot).
        assert!(machine(2, 0, 1, 3).admits_execution());
        assert_eq!(machine(4, 0, 1, 5).slots_kept_for_checks(), 2);
        assert!(machine(4, 0, 1, 5).admits_execution());
        assert!(!machine(4, 1, 1, 5).admits_execution());
        // One slot: the queued check is first.
        assert!(!machine(1, 0, 0, 1).admits_execution());
        assert!(machine(1, 0, 0, 0).admits_execution());
        // Unlimited machines reserve nothing.
        let unlimited = MachineCapacity {
            queued_checks: 9,
            ..Default::default()
        };
        assert!(unlimited.admits_execution() && unlimited.admits_check(9, true));
    }

    #[test]
    fn checks_keep_queue_order_and_their_share() {
        // A free slot, nobody ahead.
        assert!(machine(2, 1, 0, 1).admits_check(0, true));
        // The one free slot belongs to the check ahead.
        assert!(!machine(2, 1, 0, 2).admits_check(1, false));
        // Two free slots: the second in the queue runs as well.
        assert!(machine(2, 0, 0, 2).admits_check(1, false));
        // Checks hold their share and a Task waits for a run slot.
        assert!(!machine(2, 0, 1, 1).admits_check(0, true));
        assert!(machine(2, 0, 1, 1).admits_check(0, false));
        assert!(machine(1, 0, 0, 1).admits_check(0, true));
        assert!(!machine(3, 0, 2, 1).admits_check(0, true));
    }

    #[tokio::test]
    async fn bare_database_has_unlimited_capacity() {
        let db = crate::SqliteDb::new(crate::create_sqlite_pool("sqlite::memory:").await.unwrap());
        assert_eq!(db.server_run_cap.effective(), None);
        assert_eq!(db.server_run_cap.embedded_machine_id(), "");
    }
}
