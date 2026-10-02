pub use db::machine_capacity::{effective_machine_cap, AgentCapacity, MachineCapacity};

pub async fn count_agent_capacity(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
) -> crate::Result<AgentCapacity> {
    db::machine_capacity::count_agent_capacity(transaction, agent_id)
        .await
        .map_err(Into::into)
}

pub async fn count_machine_capacity(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    daemon_id: Option<&str>,
    cap: Option<i64>,
) -> crate::Result<MachineCapacity> {
    db::machine_capacity::count_machine_capacity(transaction, daemon_id, cap)
        .await
        .map_err(Into::into)
}

pub async fn server_machine_cap(
    db: &db::SqliteDb,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> crate::Result<Option<i64>> {
    db::machine_capacity::server_machine_cap(db, transaction)
        .await
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool() -> db::SqlitePool {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE daemon (id TEXT, machine_id TEXT);
             CREATE TABLE execution (id TEXT, agent_id TEXT, workspace_id TEXT, status TEXT, executor_config_snapshot_json TEXT);
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
    async fn machine_capacity_server_host_counts_all_routes_without_duplicates() {
        let pool = pool().await;
        let mut tx = db::begin_immediate(&pool).await.unwrap();
        sqlx::query("INSERT INTO daemon VALUES ('embedded', ?), ('remote', 'remote-machine')")
            .bind(crate::embedded_daemon::embedded_machine_id())
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::raw_sql("INSERT INTO agent_current VALUES ('local', NULL), ('embedded-chat', 'embedded'), ('remote-chat', 'remote');
            INSERT INTO workspace_placement VALUES
                ('direct', 'direct', 'local', NULL, NULL, 'preparing'),
                ('embedded', 'embedded', 'local', NULL, 'embedded', 'ready'),
                ('reservation', 'reservation', 'local', NULL, NULL, 'reserved'),
                ('embedded-reservation', 'embedded-reservation', 'local', NULL, 'embedded', 'preparing'),
                ('remote', 'remote', 'local', 'remote', NULL, 'reserved'),
                ('ready', 'ready', 'local', NULL, NULL, 'ready');
            INSERT INTO execution VALUES
                ('direct', 'local', 'direct', 'running', NULL),
                ('embedded', 'local', 'embedded', 'running', NULL),
                ('remote', 'local', 'remote', 'running', NULL),
                ('no-workspace', 'local', NULL, 'running', NULL);
            INSERT INTO agent_chat_turn_job VALUES ('local', 'leased'), ('embedded-chat', 'running'), ('remote-chat', 'running'), ('local', 'completed');")
            .execute(&mut *tx).await.unwrap();
        let server = count_machine_capacity(&mut tx, None, Some(7))
            .await
            .unwrap();
        assert_eq!(server.running_executions, 3);
        assert_eq!(server.reservations, 2);
        assert_eq!(server.active_chat_turns, 2);
        assert_eq!(server.active_runs(), 7);
        assert!(!server.has_capacity());
        let remote = count_machine_capacity(&mut tx, Some("remote"), None)
            .await
            .unwrap();
        assert_eq!(remote.active_runs(), 2);
        assert_eq!(remote.reservations, 0);
        assert!(remote.has_capacity());
    }

    #[test]
    fn machine_capacity_effective_reported_and_admin_caps() {
        assert_eq!(effective_machine_cap(Some(6), Some(2)), Some(2));
        assert_eq!(effective_machine_cap(Some(3), Some(8)), Some(3));
        assert_eq!(effective_machine_cap(Some(3), None), Some(3));
        assert_eq!(effective_machine_cap(Some(0), Some(2)), Some(2));
        assert_eq!(effective_machine_cap(None, None), None);
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
    async fn machine_capacity_daemon_counts_unpinned_runs_reservations_shared_mounts_and_chat() {
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
        let count = count_machine_capacity(&mut transaction, Some("daemon"), Some(5))
            .await
            .unwrap();
        assert_eq!(count.running_executions, 2);
        assert_eq!(count.reservations, 1);
        assert_eq!(count.active_chat_turns, 2);
        assert_eq!(count.active_runs(), 5);
        assert!(!count.has_capacity());
        let agent = count_agent_capacity(&mut transaction, "unpinned")
            .await
            .unwrap();
        assert_eq!(agent.running_executions, 4);
        assert_eq!(agent.reservations, 1);
    }
    #[tokio::test]
    async fn machine_capacity_executions_without_workspaces_use_snapshot_or_agent_pin() {
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
        let count = count_machine_capacity(&mut transaction, Some("daemon"), Some(2))
            .await
            .unwrap();
        assert_eq!(count.running_executions, 2);
        assert!(!count.has_capacity());
        assert_eq!(
            count_machine_capacity(&mut transaction, Some("other"), None)
                .await
                .unwrap()
                .active_runs(),
            0
        );
    }
}
