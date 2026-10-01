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
pub struct DaemonCapacity {
    pub running_executions: i64,
    pub reservations: i64,
    pub active_chat_turns: i64,
    pub max_sessions: Option<i64>,
}

impl DaemonCapacity {
    pub fn occupied_sessions(self) -> i64 {
        self.running_executions
            .saturating_add(self.reservations)
            .saturating_add(self.active_chat_turns)
    }

    pub fn has_capacity(self) -> bool {
        self.max_sessions
            .is_none_or(|limit| self.occupied_sessions() < limit)
    }
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
pub async fn count_daemon_capacity(
    transaction: &mut Transaction<'_, Sqlite>,
    daemon_id: &str,
    max_sessions: Option<i64>,
) -> Result<DaemonCapacity> {
    let row = sqlx::query(
        "SELECT
            (SELECT COUNT(*) FROM execution e
             LEFT JOIN workspace_placement p ON p.workspace_id = e.workspace_id
             LEFT JOIN agent_current a ON a.id = e.agent_id
             WHERE e.status = 'running'
               AND CASE WHEN e.workspace_id IS NOT NULL
                   THEN COALESCE(p.execution_daemon_id, p.daemon_id)
                   ELSE COALESCE(
                       CASE WHEN json_valid(e.executor_config_snapshot_json)
                           THEN json_extract(e.executor_config_snapshot_json, '$.daemon_id') END,
                       a.daemon_id)
                   END = ?) AS running_executions,
            (SELECT COUNT(*) FROM workspace_placement p
             WHERE COALESCE(p.execution_daemon_id, p.daemon_id) = ?
               AND p.state IN ('reserved', 'preparing')
               AND NOT EXISTS (SELECT 1 FROM execution e
                               WHERE e.workspace_id = p.workspace_id
                                 AND e.status = 'running')) AS reservations,
            (SELECT COUNT(*) FROM agent_chat_turn_job j
             JOIN agent_current a ON a.id = j.responder_identity_id
             WHERE a.daemon_id = ? AND j.status IN ('leased', 'running')) AS active_chat_turns",
    )
    .bind(daemon_id)
    .bind(daemon_id)
    .bind(daemon_id)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(DaemonCapacity {
        running_executions: row.try_get("running_executions")?,
        reservations: row.try_get("reservations")?,
        active_chat_turns: row.try_get("active_chat_turns")?,
        max_sessions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> db::SqlitePool {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE execution (id TEXT, agent_id TEXT, workspace_id TEXT, status TEXT, executor_config_snapshot_json TEXT);
             CREATE TABLE workspace_placement (
                 id TEXT, workspace_id TEXT, agent_id TEXT, daemon_id TEXT,
                 execution_daemon_id TEXT, state TEXT);
             CREATE TABLE agent_current (id TEXT, daemon_id TEXT);
             CREATE TABLE agent_chat_turn_job (responder_identity_id TEXT, status TEXT);",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    #[tokio::test]
    async fn agent_reservations_count_until_state_releases_them() {
        let pool = pool().await;
        let mut transaction = db::begin_immediate(&pool).await.unwrap();
        sqlx::raw_sql(
            "INSERT INTO workspace_placement VALUES
                ('preparing', 'one', 'agent', NULL, NULL, 'preparing'),
                ('reserved', 'two', 'agent', NULL, NULL, 'reserved'),
                ('ready', 'three', 'agent', NULL, NULL, 'ready'),
                ('failed', 'four', 'agent', NULL, NULL, 'failed');",
        )
        .execute(&mut *transaction)
        .await
        .unwrap();
        let count = count_agent_capacity(&mut transaction, "agent")
            .await
            .unwrap();
        assert_eq!(count.occupied_slots(), 2);
        assert!(!count.has_capacity(2));

        sqlx::query("UPDATE workspace_placement SET state = 'failed' WHERE id = 'reserved'")
            .execute(&mut *transaction)
            .await
            .unwrap();
        assert!(count_agent_capacity(&mut transaction, "agent")
            .await
            .unwrap()
            .has_capacity(2));

        sqlx::raw_sql(
            "UPDATE workspace_placement SET state = 'ready' WHERE id = 'preparing';
             INSERT INTO execution VALUES ('run', 'agent', 'one', 'running', NULL);",
        )
        .execute(&mut *transaction)
        .await
        .unwrap();
        let count = count_agent_capacity(&mut transaction, "agent")
            .await
            .unwrap();
        assert_eq!(count.running_executions, 1);
        assert_eq!(count.reservations, 0);
    }

    #[tokio::test]
    async fn daemon_counts_unpinned_runs_reservations_shared_mounts_and_chat() {
        let pool = pool().await;
        let mut transaction = db::begin_immediate(&pool).await.unwrap();
        sqlx::raw_sql(
            "INSERT INTO agent_current VALUES ('unpinned', NULL), ('chat', 'daemon');
             INSERT INTO workspace_placement VALUES
                ('one', 'one', 'unpinned', 'daemon', NULL, 'ready'),
                ('two', 'two', 'unpinned', 'daemon', NULL, 'preparing'),
                ('three', 'three', 'unpinned', NULL, 'daemon', 'reserved'),
                ('four', 'four', 'unpinned', 'other', 'other', 'ready'),
                ('idle', 'idle', 'unpinned', 'daemon', NULL, 'ready'),
                ('override', 'override', 'unpinned', 'daemon', 'other', 'ready');
             INSERT INTO execution VALUES
                ('one', 'unpinned', 'one', 'running', NULL),
                ('two', 'unpinned', 'two', 'running', NULL),
                ('other', 'unpinned', 'four', 'running', NULL),
                ('override', 'unpinned', 'override', 'running', NULL);
             INSERT INTO agent_chat_turn_job VALUES
                ('chat', 'running'), ('chat', 'leased'), ('chat', 'completed');",
        )
        .execute(&mut *transaction)
        .await
        .unwrap();
        let count = count_daemon_capacity(&mut transaction, "daemon", Some(5))
            .await
            .unwrap();
        assert_eq!(count.running_executions, 2);
        assert_eq!(count.reservations, 1);
        assert_eq!(count.active_chat_turns, 2);
        assert_eq!(count.occupied_sessions(), 5);
        assert!(!count.has_capacity());
        let agent = count_agent_capacity(&mut transaction, "unpinned")
            .await
            .unwrap();
        assert_eq!(agent.running_executions, 4);
        assert_eq!(agent.reservations, 1);
    }
    #[tokio::test]
    async fn executions_without_workspaces_use_snapshot_or_agent_pin_for_daemon_capacity() {
        let pool = pool().await;
        let mut transaction = db::begin_immediate(&pool).await.unwrap();
        sqlx::raw_sql(
            "INSERT INTO agent_current VALUES ('pinned', 'daemon'), ('moved', 'other');
             INSERT INTO execution VALUES
                ('pin', 'pinned', NULL, 'running', NULL),
                ('snapshot', 'moved', NULL, 'running', '{\"daemon_id\":\"daemon\"}');",
        )
        .execute(&mut *transaction)
        .await
        .unwrap();
        let count = count_daemon_capacity(&mut transaction, "daemon", Some(2))
            .await
            .unwrap();
        assert_eq!(count.running_executions, 2);
        assert!(!count.has_capacity());
        assert_eq!(
            count_daemon_capacity(&mut transaction, "other", None)
                .await
                .unwrap()
                .occupied_sessions(),
            0
        );
    }
}
