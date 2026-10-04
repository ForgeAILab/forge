use super::*;
use crate::{
    daemon_transport::{DaemonConnection, DaemonConnectionRegistry},
    MergeService,
};
use db::{SqliteDb, WorkspacePlacementRepo};
use events::EventBus;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn workspace_backend_large_ci_tail_preserves_server_placement_verdict() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    let (_, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    let daemon_id = placement.daemon_id.clone().unwrap();
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let (connection, mut outbound) = DaemonConnection::new(daemon_id.clone());
    let connection_id = connection.id();
    registry.register(daemon_id.clone(), connection);
    registry.dispatch_incoming_for_connection(&daemon_id, connection_id, api_types::DaemonFrame::Notification {
        method: api_types::METHOD_DAEMON_HANDSHAKE.into(),
        params: json!({"protocol_revision":api_types::DAEMON_PROTOCOL_REVISION,"capabilities":["workspace.v1","journal.ack","execution.terminal.usage_reports", api_types::DAEMON_CAPABILITY_PLAN_TRANSPORT]}),
    });
    let responses = registry.clone();
    let response_daemon = daemon_id.clone();
    let responder = tokio::spawn(async move {
        while let Some(api_types::DaemonFrame::Request { id, method, params }) =
            outbound.recv().await
        {
            let result = match method.as_str() {
                api_types::METHOD_WORKSPACE_DESCRIBE => json!({
                    "workspace_handle":params["workspace_handle"],"generation":1,"exists":true,
                    "head_sha":"head","dirty":false,"branch":"main","locked":false,
                    "active_execution_ids":[],"journaled_execution_ids":[]
                }),
                api_types::METHOD_WORKSPACE_RUN => {
                    let exit_code = if params["command"].as_str().unwrap().ends_with("exit 0") {
                        0
                    } else {
                        7
                    };
                    json!({"entry_id":format!("forge:operation:{}",params["operation_id"].as_str().unwrap()),
                        "operation_id":params["operation_id"],"exit_code":exit_code,
                        "stdout":format!("\n[Forge: CI log truncated]\n{}last failure line\n", "x".repeat(1024 * 1024 - 64)),
                        "stderr":"","duration_ms":1,"timed_out":false,"stdout_truncated":true,"stderr_truncated":false})
                }
                api_types::METHOD_JOURNAL_ACK => {
                    json!({"entry_id":params["entry_id"],"acknowledged":true})
                }
                _ => panic!("unexpected method {method}"),
            };
            responses.dispatch_incoming_for_connection(
                &response_daemon,
                connection_id,
                api_types::DaemonFrame::Response { id, result },
            );
        }
    });
    let remote = daemon::DaemonWorkspaceBackend::new(db.clone(), registry);
    let spec = |exit| RunSpec {
        purpose: WorkspaceRunPurpose::CiStep,
        command: format!(
            "head -c 1100000 /dev/zero | tr '\\000' x; printf 'last failure line\\n'; exit {exit}"
        ),
        env: Default::default(),
        timeout_secs: 0,
        max_output_bytes: usize::MAX,
    };
    let mut remote_results = Vec::new();
    for exit in [0, 7] {
        remote_results.push(remote.run(&placement, &spec(exit)).await.unwrap());
    }
    let mut bounded = spec(0);
    bounded.max_output_bytes = 4096;
    assert!(remote
        .run(&placement, &bounded)
        .await
        .unwrap_err()
        .to_string()
        .contains("output exceeds size budget"));
    responder.abort();

    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_string_lossy().into_owned();
    sqlx::query(
        "UPDATE workspace_placement SET owner_kind = 'server', daemon_id = NULL, runtime_id = NULL, execution_daemon_id = NULL, workspace_handle = ? WHERE id = ?",
    )
    .bind(&path)
    .bind(&placement.id)
    .execute(db.pool())
    .await
    .unwrap();
    let server_placement = WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
        .await
        .unwrap()
        .unwrap();
    let embedded = embedded::EmbeddedWorkspaceBackend::new(
        db.clone(),
        Arc::new(MergeService::new(
            db.clone(),
            Arc::new(EventBus::default()),
            root.path().to_path_buf(),
        )),
        root.path().to_path_buf(),
    );
    for (index, exit) in [0, 7].into_iter().enumerate() {
        let server = embedded.run(&server_placement, &spec(exit)).await.unwrap();
        assert!(server.stdout_tail.len() > 1024 * 1024);
        assert_eq!(remote_results[index].exit_code, server.exit_code);
        assert_eq!(remote_results[index].exit_code == 0, server.exit_code == 0);
        assert!(remote_results[index]
            .stdout_tail
            .ends_with("last failure line\n"));
    }
}
