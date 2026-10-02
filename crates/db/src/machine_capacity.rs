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
    pub max_concurrent_runs: Option<i64>,
}

impl MachineCapacity {
    pub fn active_runs(self) -> i64 {
        self.running_executions
            .saturating_add(self.reservations)
            .saturating_add(self.active_chat_turns)
    }

    pub fn has_capacity(self) -> bool {
        self.max_concurrent_runs
            .filter(|limit| *limit > 0)
            .is_none_or(|limit| self.active_runs() < limit)
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
        COALESCE(SUM(reservations), 0) AS reservations, COALESCE(SUM(active_chat_turns), 0) AS active_chat_turns
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
        max_concurrent_runs,
    })
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
        UNION ALL SELECT id, id, hostname, max_concurrent_runs, run_limit FROM daemon WHERE id NOT IN (SELECT id FROM embedded)
    ) SELECT m.*, COALESCE(o.running_executions, 0) AS running_executions,
        COALESCE(o.reservations, 0) AS reservations, COALESCE(o.active_chat_turns, 0) AS active_chat_turns
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
                    max_concurrent_runs: effective_machine_cap(
                        row.try_get("reported")?,
                        row.try_get("admin")?,
                    ),
                },
            })
        })
        .collect()
}

/// A launch consumes its own admitted slot, even if chat or a lower cap fills
/// the machine during preparation. Expired and idle-ready placements own none.
pub async fn launch_holds_slot(
    tx: &mut Transaction<'_, Sqlite>,
    workspace_id: Option<&str>,
    agent_id: Option<&str>,
) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspace_placement p
        WHERE p.workspace_id = ? AND p.agent_id = ?
        AND (p.state IN ('reserved', 'preparing') OR (p.state = 'ready' AND p.reserved_until IS NOT NULL))
        AND julianday(COALESCE(p.reserved_until, datetime(p.updated_at, '+10 minutes'))) > julianday('now')
        AND NOT EXISTS (SELECT 1 FROM execution e WHERE e.workspace_id = p.workspace_id AND e.status = 'running'))")
        .bind(workspace_id).bind(agent_id).fetch_one(&mut **tx).await?)
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn bare_database_has_unlimited_capacity() {
        let db = crate::SqliteDb::new(crate::create_sqlite_pool("sqlite::memory:").await.unwrap());
        assert_eq!(db.server_run_cap.effective(), None);
        assert_eq!(db.server_run_cap.embedded_machine_id(), "");
    }
}
