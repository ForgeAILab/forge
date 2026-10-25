//! Workspace RPC receipt, retry, and daemon transport tests.

use crate::daemon_transport::{lock, DaemonConnection};
use std::sync::Mutex;

use async_trait::async_trait;
use events::EventBus;
use serde_json::json;

use super::*;
use crate::daemon_transport::DaemonExecutionEventHandler;

pub(crate) const DAEMON_ID: &str = "daemon-test";
#[test]
fn daemon_errors_redact_clone_url_credentials_at_server_boundary() {
    let remote = "https://username:PASSWORD@127.0.0.1/repo?token=QUERYSECRET";
    let error = WorkspaceClientError::Daemon(DaemonErrorPayload {
        code: "clone_failed".into(),
        message: remote.into(),
        details: Some(json!({"failure":"PASSWORD QUERYSECRET"})),
    });
    let sanitized = redact_remote_error(error, remote).to_string();
    for secret in ["username", "PASSWORD", "QUERYSECRET"] {
        assert!(!sanitized.contains(secret), "{sanitized}");
    }
}

struct NoopHandler;

#[async_trait]
impl DaemonExecutionEventHandler for NoopHandler {
    async fn handle_log(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: ExecutionLogNotification,
    ) -> std::result::Result<(), ServiceError> {
        Ok(())
    }

    async fn handle_terminal(
        &self,
        _daemon_id: &str,
        _connection_id: u64,
        _notification: ExecutionTerminalNotification,
    ) -> std::result::Result<(), ServiceError> {
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct RecordedRequest {
    pub id: String,
    pub method: String,
    pub params: Value,
}

pub(crate) enum Reply {
    Ignore,
    Value(Value),
    Error(DaemonErrorPayload),
    Prepare,
    Describe,
    Run,
    RunLimits { timed_out: bool, truncated: bool },
    Verify { mismatch: bool },
}

pub(crate) fn rejection(code: &str, message: &str, details: Option<Value>) -> Reply {
    Reply::Error(DaemonErrorPayload {
        code: code.to_owned(),
        message: message.to_owned(),
        details,
    })
}

pub(crate) struct ScriptedDaemon {
    pub registry: Arc<DaemonConnectionRegistry>,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    task: tokio::task::JoinHandle<()>,
}

#[tokio::test]
async fn daemon_transport_ack_retry_continues_after_receipt_refusal() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    let (_, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let daemon_id = placement.daemon_id.clone().unwrap();
    let (connection_id, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
    let client = DaemonWorkspaceClient::new(registry.clone()).with_receipts(db.clone());
    for index in 0..2 {
        client.retain_result(&daemon_id, METHOD_WORKSPACE_PREPARE,
            &json!({"placement_id": placement.id, "runtime_id": placement.runtime_id, "generation":placement.generation, "operation_id":format!("op-{index}")}),
            &json!({"entry_id":format!("entry-{index}"), "operation_id":format!("op-{index}")})).await.unwrap();
    }
    let responder = {
        let daemon_id = daemon_id.clone();
        tokio::spawn(async move {
            let mut accepted = None;
            for index in 0..2 {
                let DaemonFrame::Request { id, method, params } = outbound.recv().await.unwrap()
                else {
                    panic!("ack request");
                };
                assert_eq!(method, METHOD_JOURNAL_ACK);
                if index == 1 {
                    accepted = params["entry_id"].as_str().map(str::to_owned);
                }
                registry.dispatch_incoming_for_connection(
                    &daemon_id,
                    connection_id,
                    DaemonFrame::Response {
                        id,
                        result: json!({"entry_id":params["entry_id"], "acknowledged":index == 1}),
                    },
                );
            }
            accepted.unwrap()
        })
    };
    client.retry_acknowledgements(&daemon_id).await.unwrap();
    let accepted = responder.await.unwrap();
    let acknowledged: Vec<String> = sqlx::query_scalar(
        "SELECT idempotency_key FROM command_receipt WHERE operation LIKE '%.ack'",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(acknowledged, vec![accepted]);
}

impl ScriptedDaemon {
    pub fn new(replies: Vec<Reply>) -> Self {
        let registry = Arc::new(DaemonConnectionRegistry::new(
            Arc::new(EventBus::new(16)),
            Arc::new(NoopHandler),
        ));
        let (connection, mut outbound) = DaemonConnection::new(DAEMON_ID.to_owned());
        let connection_id = connection.id();
        registry.register(DAEMON_ID.to_owned(), connection);
        let mut capabilities: Vec<String> = DAEMON_REQUIRED_CAPABILITIES
            .iter()
            .map(|capability| (*capability).to_owned())
            .collect();
        capabilities.push(DAEMON_CAPABILITY_WORKSPACE.to_owned());
        assert!(registry.dispatch_incoming_for_connection(DAEMON_ID, connection_id, DaemonFrame::Notification {
            method: METHOD_DAEMON_HANDSHAKE.to_owned(),
            params: json!({ "protocol_revision": DAEMON_PROTOCOL_REVISION, "capabilities": capabilities }),
        }));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let transport = Arc::clone(&registry);
        let task = tokio::spawn(async move {
            for reply in replies {
                let DaemonFrame::Request { id, method, params } =
                    outbound.recv().await.expect("scripted request arrives")
                else {
                    panic!("expected request frame");
                };
                recorded.lock().unwrap().push(RecordedRequest {
                    id: id.clone(),
                    method,
                    params: params.clone(),
                });
                let result = match reply {
                    Reply::Ignore => continue,
                    Reply::Error(error) => {
                        transport.dispatch_incoming_for_connection(
                            DAEMON_ID,
                            connection_id,
                            DaemonFrame::Error {
                                id: Some(id),
                                error,
                            },
                        );
                        continue;
                    }
                    Reply::Value(value) => value,
                    Reply::Prepare => json!({
                        "entry_id": "prepare-entry", "operation_id": params["operation_id"],
                        "workspace_handle": "workspace-handle", "workspace_path": "/remote/workspaces/task/repo",
                        "base_sha": "base-sha", "branch": params["branch"], "generation": params["generation"],
                    }),
                    Reply::Describe => json!({
                        "workspace_handle": params["workspace_handle"], "generation": params["generation"],
                        "exists": true, "head_sha": "head-sha", "dirty": false, "branch": "task/test",
                        "locked": false, "active_execution_ids": [], "journaled_execution_ids": [],
                    }),
                    Reply::Run => json!({
                        "entry_id": "run-entry", "operation_id": params["operation_id"], "exit_code": 0,
                        "stdout": "ok", "stderr": "", "duration_ms": 1, "timed_out": false,
                        "stdout_truncated": false, "stderr_truncated": false,
                    }),
                    Reply::RunLimits {
                        timed_out,
                        truncated,
                    } => json!({
                        "entry_id": "run-entry", "operation_id": params["operation_id"], "exit_code": 0,
                        "stdout": "", "stderr": "", "duration_ms": 1, "timed_out": timed_out,
                        "stdout_truncated": truncated, "stderr_truncated": false,
                    }),
                    Reply::Verify { mismatch } => {
                        let probe_content = if let Some(probe) = params.get("probe") {
                            let content = tokio::fs::read_to_string(
                                probe["path"].as_str().expect("probe path"),
                            )
                            .await
                            .expect("server probe is readable");
                            assert_eq!(content, probe["content"].as_str().expect("probe content"));
                            Some(if mismatch {
                                "different mount".to_owned()
                            } else {
                                content
                            })
                        } else {
                            None
                        };
                        json!({ "repo_location_id": params["repo_location_id"], "path": params["path"],
                            "default_branch_sha": "base-sha", "origin_url": null, "probe_content": probe_content })
                    }
                };
                assert!(transport.dispatch_incoming_for_connection(
                    DAEMON_ID,
                    connection_id,
                    DaemonFrame::Response { id, result }
                ));
            }
        });
        Self {
            registry,
            requests,
            task,
        }
    }

    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }

    pub async fn finish(self) {
        self.task.await.expect("scripted daemon completes");
    }
}

fn prepare_params() -> WorkspacePrepareParams {
    WorkspacePrepareParams {
        fence: WorkspaceMutationFence {
            integration: api_types::WorkspaceIntegrationBinding::TaskStep,
            daemon_id: DAEMON_ID.to_owned(),
            runtime_id: "runtime-test".to_owned(),
            placement_id: "placement-test".to_owned(),
            operation_id: "prepare-id".to_owned(),
            generation: 1,
            expected: WorkspaceOperationExpected::Version { version: 0 },
        },
        repo_location_id: "location-test".to_owned(),
        workspace_id: "workspace-test".to_owned(),
        task_id: "task-test".to_owned(),
        base_ref: "main".to_owned(),
        branch: "task/test".to_owned(),
    }
}

fn run_params(
    placement: &db::WorkspacePlacement,
    operation_id: &str,
    purpose: WorkspaceRunPurpose,
) -> WorkspaceRunParams {
    WorkspaceRunParams {
        fence: WorkspaceMutationFence {
            integration: api_types::WorkspaceIntegrationBinding::TaskStep,
            daemon_id: placement.daemon_id.clone().unwrap(),
            runtime_id: placement.runtime_id.clone().unwrap(),
            placement_id: placement.id.clone(),
            operation_id: operation_id.to_owned(),
            generation: u64::try_from(placement.generation).unwrap(),
            expected: WorkspaceOperationExpected::BaseSha {
                sha: "base-head".to_owned(),
            },
        },
        workspace_handle: placement.workspace_handle.clone().unwrap(),
        purpose,
        command: "printf should-not-run".to_owned(),
        env: vec![],
        timeout_secs: 1,
        max_output_bytes: 1024,
    }
}

#[tokio::test]
async fn workspace_run_error_is_recorded_before_journal_ack() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    let (_, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    let daemon_id = placement.daemon_id.clone().unwrap();
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let (connection_id, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
    let client = DaemonWorkspaceClient::new(registry.clone()).with_receipts(db.clone());
    let operation_id = "policy-error-operation";
    let params = run_params(&placement, operation_id, WorkspaceRunPurpose::Hook);
    let responder = {
        let registry = registry.clone();
        let daemon_id = daemon_id.clone();
        tokio::spawn(async move {
            let DaemonFrame::Request { id, method, params } = outbound.recv().await.unwrap() else {
                panic!("workspace run request")
            };
            assert_eq!(method, METHOD_WORKSPACE_RUN);
            registry.dispatch_incoming_for_connection(
                &daemon_id,
                connection_id,
                DaemonFrame::Error {
                    id: Some(id),
                    error: DaemonErrorPayload {
                        code: PURPOSE_DENIED.to_owned(),
                        message: "hook denied".to_owned(),
                        details: Some(json!({
                            "entry_id": "policy-error-entry",
                            "operation_id": params["operation_id"],
                        })),
                    },
                },
            );
            let DaemonFrame::Request { id, method, params } = outbound.recv().await.unwrap() else {
                panic!("journal acknowledgement")
            };
            assert_eq!(method, METHOD_JOURNAL_ACK);
            assert_eq!(params["entry_id"], "policy-error-entry");
            registry.dispatch_incoming_for_connection(
                &daemon_id,
                connection_id,
                DaemonFrame::Response {
                    id,
                    result: json!({
                        "entry_id": "policy-error-entry",
                        "acknowledged": true,
                    }),
                },
            );
        })
    };

    assert!(matches!(
        client.run(&daemon_id, params).await,
        Err(WorkspaceClientError::Daemon(error)) if error.code == PURPOSE_DENIED
    ));
    responder.await.unwrap();
    let operations: Vec<String> = sqlx::query_scalar(
        "SELECT operation FROM command_receipt WHERE idempotency_key = ? ORDER BY operation",
    )
    .bind("policy-error-entry")
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        operations,
        vec!["daemon.workspace.run", "daemon.workspace.run.ack"]
    );
}

#[tokio::test]
async fn reconciled_workspace_run_error_is_recorded_before_journal_ack() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    let (_, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    let daemon_id = placement.daemon_id.clone().unwrap();
    let registry = Arc::new(DaemonConnectionRegistry::without_handlers());
    let (connection_id, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
    let client = DaemonWorkspaceClient::new(registry.clone()).with_receipts(db.clone());
    let operation_id = "interrupted-error-operation";
    let params = run_params(&placement, operation_id, WorkspaceRunPurpose::CiStep);
    let responder = {
        let registry = registry.clone();
        let daemon_id = daemon_id.clone();
        tokio::spawn(async move {
            let mut methods = Vec::new();
            for step in 0..3 {
                let DaemonFrame::Request { id, method, params } = outbound.recv().await.unwrap()
                else {
                    panic!("workspace reconciliation request")
                };
                methods.push(method.clone());
                let frame = match step {
                    0 => DaemonFrame::Error {
                        id: Some(id),
                        error: DaemonErrorPayload {
                            code: DAEMON_UNAVAILABLE.to_owned(),
                            message: "interrupted".to_owned(),
                            details: Some(json!({
                                "entry_id": "interrupted-intent-entry",
                                "interrupted": true,
                            })),
                        },
                    },
                    1 => DaemonFrame::Response {
                        id,
                        result: json!({
                            "entry_id": "interrupted-error-entry",
                            "operation_id": params["operation_id"],
                            "outcome": {
                                "kind": "error",
                                "error": {
                                    "code": DAEMON_UNAVAILABLE,
                                    "message": "command outcome unknown",
                                    "details": null,
                                },
                            },
                        }),
                    },
                    _ => {
                        assert_eq!(params["entry_id"], "interrupted-error-entry");
                        DaemonFrame::Response {
                            id,
                            result: json!({
                                "entry_id": "interrupted-error-entry",
                                "acknowledged": true,
                            }),
                        }
                    }
                };
                registry.dispatch_incoming_for_connection(&daemon_id, connection_id, frame);
            }
            methods
        })
    };

    assert!(matches!(
        client.run(&daemon_id, params).await,
        Err(WorkspaceClientError::Daemon(error)) if error.code == DAEMON_UNAVAILABLE
    ));
    assert_eq!(
        responder.await.unwrap(),
        vec![
            METHOD_WORKSPACE_RUN,
            METHOD_WORKSPACE_DESCRIBE,
            METHOD_JOURNAL_ACK,
        ]
    );
    let operations: Vec<String> = sqlx::query_scalar(
        "SELECT operation FROM command_receipt WHERE idempotency_key = ? ORDER BY operation",
    )
    .bind("interrupted-error-entry")
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        operations,
        vec!["daemon.workspace.run", "daemon.workspace.run.ack"]
    );
}

#[tokio::test(start_paused = true)]
async fn prepare_timeout_retry_reuses_operation_id() {
    let daemon = ScriptedDaemon::new(vec![Reply::Ignore, Reply::Prepare]);
    let client = DaemonWorkspaceClient::new(Arc::clone(&daemon.registry))
        .with_timeout(Duration::from_secs(1));
    let result = client
        .prepare(DAEMON_ID, prepare_params())
        .await
        .expect("retried prepare succeeds");
    assert_eq!(result.workspace.workspace_handle, "workspace-handle");
    let requests = daemon.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, METHOD_WORKSPACE_PREPARE);
    assert_eq!(requests[0].params, requests[1].params);
    assert_eq!(requests[0].params["operation_id"], "prepare-id");
    assert_ne!(requests[0].id, requests[1].id);
    assert!(lock(&daemon.registry.get(DAEMON_ID).unwrap().pending).is_empty());
    daemon.finish().await;
}

#[tokio::test(start_paused = true)]
async fn prepare_timeout_retry_is_bounded() {
    let daemon = ScriptedDaemon::new(vec![Reply::Ignore, Reply::Ignore]);
    let client = DaemonWorkspaceClient::new(Arc::clone(&daemon.registry))
        .with_timeout(Duration::from_secs(1));
    assert!(matches!(
        client.prepare(DAEMON_ID, prepare_params()).await,
        Err(WorkspaceClientError::Transport(
            ServiceError::DaemonTimeout { .. }
        ))
    ));
    assert_eq!(daemon.requests().len(), 2);
    assert!(lock(&daemon.registry.get(DAEMON_ID).unwrap().pending).is_empty());
    daemon.finish().await;
}

#[tokio::test]
async fn daemon_errors_keep_their_codes_and_details() {
    let details = json!({ "current_generation": 2, "entry_id": "error-entry" });
    let daemon = ScriptedDaemon::new(vec![rejection(
        STALE_GENERATION,
        "stale",
        Some(details.clone()),
    )]);
    let client = DaemonWorkspaceClient::new(Arc::clone(&daemon.registry));
    let error = client
        .prepare(DAEMON_ID, prepare_params())
        .await
        .expect_err("stale generation rejected");
    let WorkspaceClientError::Daemon(error) = error else {
        panic!("expected structured rejection")
    };
    assert_eq!(error.code, STALE_GENERATION);
    assert_eq!(error.details, Some(details));
    assert_eq!(daemon.requests().len(), 1);
    daemon.finish().await;
}

#[tokio::test]
async fn daemon_timeout_rejection_is_not_a_transport_retry() {
    let daemon = ScriptedDaemon::new(vec![rejection(DAEMON_TIMEOUT, "operation timed out", None)]);
    let client = DaemonWorkspaceClient::new(Arc::clone(&daemon.registry));
    assert!(matches!(client.prepare(DAEMON_ID, prepare_params()).await,
        Err(WorkspaceClientError::Daemon(error)) if error.code == DAEMON_TIMEOUT));
    assert_eq!(daemon.requests().len(), 1);
    daemon.finish().await;
}

#[tokio::test]
async fn cancelled_request_removes_pending_sender() {
    let daemon = ScriptedDaemon::new(vec![Reply::Ignore]);
    let client = DaemonWorkspaceClient::new(Arc::clone(&daemon.registry));
    let request = tokio::spawn(async move { client.prepare(DAEMON_ID, prepare_params()).await });
    while daemon.requests().is_empty() {
        tokio::task::yield_now().await;
    }
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert!(lock(&daemon.registry.get(DAEMON_ID).unwrap().pending).is_empty());
    daemon.finish().await;
}

#[tokio::test]
async fn disconnected_daemon_is_unavailable() {
    let daemon = ScriptedDaemon::new(vec![]);
    daemon.registry.unregister(DAEMON_ID);
    let client = DaemonWorkspaceClient::new(Arc::clone(&daemon.registry));
    assert!(matches!(
        client.prepare(DAEMON_ID, prepare_params()).await,
        Err(WorkspaceClientError::Transport(
            ServiceError::DaemonUnavailable { .. }
        ))
    ));
    assert!(daemon.requests().is_empty());
    daemon.finish().await;
}

#[tokio::test]
async fn interrupted_run_reconciles_without_resending_the_command() {
    let params = WorkspaceRunParams {
        fence: prepare_params().fence,
        workspace_handle: "handle".into(),
        purpose: WorkspaceRunPurpose::CiStep,
        command: "printf once".into(),
        env: vec![],
        timeout_secs: 0,
        max_output_bytes: u64::MAX,
    };
    let daemon = ScriptedDaemon::new(vec![
        rejection(
            DAEMON_UNAVAILABLE,
            "interrupted",
            Some(json!({"entry_id":"entry", "interrupted":true})),
        ),
        Reply::Value(
            json!({"entry_id":"entry", "operation_id":"prepare-id", "outcome":{
                "kind":"error", "error":{"code":DAEMON_UNAVAILABLE,"message":"command outcome unknown","details":null}
            }}),
        ),
    ]);
    let client = DaemonWorkspaceClient::new(Arc::clone(&daemon.registry));
    assert!(matches!(client.run(DAEMON_ID, params).await,
        Err(WorkspaceClientError::Daemon(error)) if error.code == DAEMON_UNAVAILABLE));
    let requests = daemon.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, METHOD_WORKSPACE_RUN);
    assert_eq!(requests[1].method, METHOD_WORKSPACE_DESCRIBE);
    assert_eq!(requests[1].params["operation"], "reconcile");
    assert_eq!(
        requests[0].params["operation_id"],
        requests[1].params["operation_id"]
    );
    daemon.finish().await;
}
#[tokio::test]
async fn older_daemon_cancel_rejection_is_unreachable_and_has_no_retry() {
    let daemon = ScriptedDaemon::new(vec![rejection(
        api_types::UNSUPPORTED_METHOD,
        "unknown workspace.cancel",
        None,
    )]);
    let client = DaemonWorkspaceClient::new(daemon.registry.clone());
    assert!(matches!(
        client.cancel_workspace_operation(DAEMON_ID, "old-op").await,
        Err(WorkspaceClientError::Transport(
            ServiceError::DaemonUnavailable { .. }
        ))
    ));
    assert_eq!(daemon.requests().len(), 1);
    assert_eq!(
        daemon.requests()[0].method,
        api_types::METHOD_WORKSPACE_CANCEL
    );
    daemon.finish().await;
}
