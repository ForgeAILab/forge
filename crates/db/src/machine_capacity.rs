use sqlx::{Row, Sqlite, Transaction};

use crate::Result;

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
            .bind(config::embedded_machine_id())
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
/// its slot when the expiry sweep changes its state.
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
) -> Result<MachineCapacity> {
    let row = sqlx::query(
        "WITH routes AS (
            SELECT e.workspace_id,
                CASE WHEN e.workspace_id IS NOT NULL
                    THEN COALESCE(p.execution_daemon_id, p.daemon_id)
                    ELSE COALESCE(
                        CASE WHEN json_valid(e.executor_config_snapshot_json)
                            THEN json_extract(e.executor_config_snapshot_json, '$.daemon_id') END,
                        a.daemon_id)
                    END AS daemon_id
            FROM execution e
            LEFT JOIN workspace_placement p ON p.workspace_id = e.workspace_id
            LEFT JOIN agent_current a ON a.id = e.agent_id
            WHERE e.status = 'running'
         ), embedded AS (SELECT id FROM daemon WHERE machine_id = ?)
         SELECT
            (SELECT COUNT(*) FROM routes r WHERE
                CASE WHEN ? IS NULL THEN r.daemon_id IS NULL OR r.daemon_id IN (SELECT id FROM embedded)
                     ELSE r.daemon_id = ? END) AS running_executions,
            (SELECT COUNT(*) FROM workspace_placement p WHERE
                CASE WHEN ? IS NULL THEN COALESCE(p.execution_daemon_id, p.daemon_id) IS NULL
                    OR COALESCE(p.execution_daemon_id, p.daemon_id) IN (SELECT id FROM embedded)
                    ELSE COALESCE(p.execution_daemon_id, p.daemon_id) = ? END
                AND p.state IN ('reserved', 'preparing')
                AND NOT EXISTS (SELECT 1 FROM execution e
                    WHERE e.workspace_id = p.workspace_id AND e.status = 'running')) AS reservations,
            (SELECT COUNT(*) FROM agent_chat_turn_job j
             JOIN agent_current a ON a.id = j.responder_identity_id WHERE
                CASE WHEN ? IS NULL THEN a.daemon_id IS NULL OR a.daemon_id IN (SELECT id FROM embedded)
                    ELSE a.daemon_id = ? END
                AND j.status IN ('leased', 'running')) AS active_chat_turns",
    )
    .bind(config::embedded_machine_id())
    .bind(daemon_id).bind(daemon_id)
    .bind(daemon_id).bind(daemon_id)
    .bind(daemon_id).bind(daemon_id)
    .fetch_one(&mut **transaction).await?;
    Ok(MachineCapacity {
        running_executions: row.try_get("running_executions")?,
        reservations: row.try_get("reservations")?,
        active_chat_turns: row.try_get("active_chat_turns")?,
        max_concurrent_runs,
    })
}
