//! Daemon frame, execution accounting, and workspace transport round-trip tests.

#[test]
fn machine_capabilities_are_optional_at_current_revision() {
    let old: Vec<String> = super::DAEMON_REQUIRED_CAPABILITIES
        .iter()
        .map(|fact| (*fact).into())
        .collect();
    assert!(super::daemon_protocol_is_compatible(
        super::DAEMON_PROTOCOL_REVISION,
        &old
    ));
    assert_eq!(super::DAEMON_PROTOCOL_REVISION, 7);
    let probe: super::MachineProbeParams = serde_json::from_value(serde_json::json!({"daemon_id":"d","runtime_id":"r","repo_location_id":null,"commands":[{"name":"cargo","command":"cargo --version","timeout_seconds":10}],"env":{}})).unwrap();
    assert_eq!(probe.commands[0].name, "cargo");
    let provision: super::RepoLocationProvisionParams = serde_json::from_value(serde_json::json!({"daemon_id":"d","runtime_id":"r","repo_id":"repo","remote_url":"file:///repository","default_branch":"main","timeout_seconds":1800})).unwrap();
    assert!(serde_json::to_value(provision)
        .unwrap()
        .get("credentials")
        .is_none());
    for purpose in ["environment_probe", "repo_provision"] {
        assert!(
            serde_json::from_value::<super::WorkspaceRunPurpose>(serde_json::json!(purpose))
                .is_ok()
        );
    }
}

use super::*;
use crate::UsageTelemetryState;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

fn assert_round_trip<T: Serialize + DeserializeOwned>(value: Value) -> T {
    let decoded: T = serde_json::from_value(value.clone()).expect("deserialize wire payload");
    assert_eq!(
        serde_json::to_value(&decoded).expect("serialize wire payload"),
        value
    );
    decoded
}

fn mutation_params(fields: Value) -> Value {
    let mut value = json!({
        "integration": {"kind":"task_step"},
        "daemon_id": "daemon-1",
        "runtime_id": "runtime-1",
        "placement_id": "placement-1",
        "operation_id": "operation-1",
        "generation": 2,
        "expected": { "kind": "base_sha", "sha": "base-sha" }
    });
    value
        .as_object_mut()
        .expect("mutation object")
        .extend(fields.as_object().expect("mutation fields").clone());
    value
}

fn workspace_params(fields: Value) -> Value {
    let mut value = json!({
        "daemon_id": "daemon-1",
        "runtime_id": "runtime-1",
        "placement_id": "placement-1",
        "workspace_handle": "opaque-handle-1",
        "generation": 2
    });
    value
        .as_object_mut()
        .expect("workspace object")
        .extend(fields.as_object().expect("workspace fields").clone());
    value
}

fn prepared_result() -> Value {
    json!({
        "entry_id": "journal-1",
        "operation_id": "operation-1",
        "workspace_handle": "opaque-handle-1",
        "workspace_path": "/daemon/workspaces/task-1/worktree",
        "base_sha": "base-sha",
        "branch": "forge/task-1",
        "generation": 2
    })
}

#[test]
fn request_frame_round_trips() {
    let frame = DaemonFrame::Request {
        id: "req-1".to_owned(),
        method: "fs.list".to_owned(),
        params: serde_json::json!({ "path": "/tmp" }),
    };

    let json = serde_json::to_value(&frame).expect("serialize request frame");
    assert_eq!(json["type"], "request");
    assert!(json.get("id").is_some());
    assert!(json.get("method").is_some());
    assert!(json.get("params").is_some());

    let decoded: DaemonFrame = serde_json::from_value(json).expect("deserialize request frame");
    assert!(matches!(decoded, DaemonFrame::Request { .. }));
}

#[test]
fn response_frame_round_trips() {
    let frame = DaemonFrame::Response {
        id: "req-1".to_owned(),
        result: serde_json::json!({ "ok": true }),
    };

    let json = serde_json::to_value(&frame).expect("serialize response frame");
    assert_eq!(json["type"], "response");

    let decoded: DaemonFrame = serde_json::from_value(json).expect("deserialize response frame");
    assert!(matches!(decoded, DaemonFrame::Response { .. }));
}

#[test]
fn error_frame_round_trips() {
    let frame = DaemonFrame::Error {
        id: Some("req-1".to_owned()),
        error: DaemonErrorPayload {
            code: "daemon_timeout".to_owned(),
            message: "daemon timed out".to_owned(),
            details: None,
        },
    };

    let json = serde_json::to_value(&frame).expect("serialize error frame");
    assert_eq!(json["type"], "error");

    let decoded: DaemonFrame = serde_json::from_value(json).expect("deserialize error frame");
    assert!(matches!(decoded, DaemonFrame::Error { .. }));
}

#[test]
fn notification_frame_round_trips() {
    let frame = DaemonFrame::Notification {
        method: "execution.log".to_owned(),
        params: serde_json::json!({
            "execution_id": "exec-1",
            "seq": 1,
            "stream": "stdout",
            "line": "started",
            "ts": "2026-05-14T00:00:00Z"
        }),
    };

    let json = serde_json::to_value(&frame).expect("serialize notification frame");
    assert_eq!(json["type"], "notification");

    let decoded: DaemonFrame =
        serde_json::from_value(json).expect("deserialize notification frame");
    assert!(matches!(decoded, DaemonFrame::Notification { .. }));
}

#[test]
fn terminal_output_notification_round_trips() {
    let notification = TerminalOutputNotification {
        session_id: "term-1".to_owned(),
        data: "hello\r\n".to_owned(),
        ts: "2026-05-20T00:00:00Z".to_owned(),
    };

    let json = serde_json::to_value(&notification).expect("serialize terminal output");
    assert_eq!(json["session_id"], "term-1");
    assert_eq!(json["data"], "hello\r\n");

    let decoded: TerminalOutputNotification =
        serde_json::from_value(json).expect("deserialize terminal output");
    assert_eq!(decoded.session_id, "term-1");
    assert_eq!(decoded.data, "hello\r\n");
    assert_eq!(decoded.ts, "2026-05-20T00:00:00Z");
}

#[test]
fn terminal_usage_reports_preserve_nullable_counters_and_decimal_cost() {
    let notification = ExecutionTerminalNotification {
        terminal_report_id: "terminal-report-1".to_owned(),
        execution_id: "execution-1".to_owned(),
        exit_code: Some(0),
        signal: None,
        error: None,
        ts: "2026-05-20T00:00:00Z".to_owned(),
        status: Some("completed".to_owned()),
        agent_session_id: None,
        summary: None,
        after_sha: None,
        outbox_entries: vec![],
        plan_text: None,
        usage_reports: vec![RemoteUsageReport {
            report_id: "report-1".to_owned(),
            request_id: Some("request-1".to_owned()),
            report_sequence: 0,
            candidate_key: Some("candidate-a".to_owned()),
            attempt_ordinal: 1,
            provider_id: Some("openai".to_owned()),
            model_id: Some("gpt-5".to_owned()),
            input_tokens: None,
            output_tokens: Some(12),
            cache_read_tokens: Some(0),
            cache_write_tokens: None,
            telemetry_state: UsageTelemetryState::Metered,
            context_tokens: Some(20),
            selected_tier: Some("short".to_owned()),
            reported_cost_usd: Some("0.000000001".to_owned()),
            partial: true,
        }],
        failure_class: None,
        retry_at: None,
        resolved_candidate: None,
        route_attempts: None,
    };

    let value = serde_json::to_value(&notification).expect("terminal serializes");
    assert_eq!(value["terminal_report_id"], "terminal-report-1");
    assert_eq!(
        value["usage_reports"][0]["input_tokens"],
        serde_json::Value::Null
    );
    assert_eq!(
        value["usage_reports"][0]["reported_cost_usd"],
        "0.000000001"
    );

    let decoded: ExecutionTerminalNotification =
        serde_json::from_value(value).expect("terminal deserializes");
    assert_eq!(decoded.usage_reports, notification.usage_reports);
}

#[test]
fn terminal_notification_without_report_vector_is_rejected() {
    let value = serde_json::json!({
        "terminal_report_id": "terminal-report-1",
        "execution_id": "execution-1",
        "exit_code": 0,
        "signal": null,
        "error": null,
        "ts": "2026-05-20T00:00:00Z"
    });
    assert!(serde_json::from_value::<ExecutionTerminalNotification>(value).is_err());
}

#[test]
fn daemon_protocol_gate_requires_revision_and_capabilities() {
    let capabilities = DAEMON_REQUIRED_CAPABILITIES
        .iter()
        .map(|capability| (*capability).to_owned())
        .collect::<Vec<_>>();
    assert!(daemon_protocol_is_compatible(
        DAEMON_PROTOCOL_REVISION,
        &capabilities
    ));
    assert!(!daemon_protocol_is_compatible(
        DAEMON_MIN_PROTOCOL_REVISION - 1,
        &capabilities
    ));
    assert!(!daemon_protocol_is_compatible(
        DAEMON_PROTOCOL_REVISION,
        &capabilities[..1]
    ));
    assert!(daemon_protocol_is_compatible(
        DAEMON_PROTOCOL_REVISION + 1,
        &capabilities
    ));
    let revision_2_capabilities = vec![
        DAEMON_CAPABILITY_USAGE_REPORTS.to_owned(),
        "execution.terminal.ack".to_owned(),
    ];
    assert!(!daemon_protocol_is_compatible(2, &revision_2_capabilities));
    assert!(!daemon_protocol_is_compatible(
        DAEMON_MIN_PROTOCOL_REVISION,
        &revision_2_capabilities[..1]
    ));
    assert!(!daemon_protocol_is_compatible(
        DAEMON_PROTOCOL_REVISION,
        &revision_2_capabilities
    ));
    assert_eq!(DAEMON_PROTOCOL_REVISION, 7);
    assert_eq!(DAEMON_MIN_PROTOCOL_REVISION, 7);
}

#[test]
fn workspace_handshake_round_trips_adapter_facts_and_run_policy() {
    let handshake = assert_round_trip::<DaemonHandshakeNotification>(json!({
        "protocol_revision": DAEMON_PROTOCOL_REVISION,
        "capabilities": [
            DAEMON_CAPABILITY_USAGE_REPORTS,
            DAEMON_CAPABILITY_JOURNAL_ACK,
            DAEMON_CAPABILITY_WORKSPACE
        ],
        "executor_capabilities": {
            "codex": {
                "structured_events": true,
                "usage": true,
                "resume": false,
                "cancel_ack": true,
                "terminal_observed": true
            }
        },
        "workspace_run_policy": {
            "allowed_purposes": ["ci_step", "hook", "environment_setup"]
        }
    }));
    assert_eq!(DAEMON_CAPABILITY_WORKSPACE, "workspace.v1");
    assert!(handshake.executor_capabilities["codex"].structured_events);
    assert!(!handshake.executor_capabilities["codex"].resume);
    assert_eq!(handshake.workspace_run_policy.allowed_purposes.len(), 3);
}

#[test]
fn missing_adapter_facts_and_run_policy_are_unsupported() {
    let handshake: DaemonHandshakeNotification = serde_json::from_value(json!({
        "protocol_revision": 2,
        "capabilities": ["execution.terminal.usage_reports", "execution.terminal.ack"]
    }))
    .expect("revision 2 handshake remains readable");
    assert!(handshake.executor_capabilities.is_empty());
    assert!(handshake.workspace_run_policy.allowed_purposes.is_empty());

    let facts: ExecutorAdapterCapabilityFacts =
        serde_json::from_value(json!({ "structured_events": true }))
            .expect("partial capability facts");
    assert!(facts.structured_events);
    assert!(!facts.usage);
    assert!(!facts.resume);
    assert!(!facts.cancel_ack);
    assert!(!facts.terminal_observed);
}

#[test]
fn repo_location_verify_round_trips() {
    assert_eq!(METHOD_REPO_LOCATION_VERIFY, "repo_location.verify");
    assert_round_trip::<RepoLocationVerifyParams>(json!({
        "repo_location_id": "location-1",
        "daemon_id": "daemon-1",
        "runtime_id": "runtime-1",
        "path": "/daemon/repos/project",
        "kind": "shared_mount",
        "default_branch": "main",
        "remote_url": "https://example.com/project.git",
        "expected_version": 4,
        "probe": { "path": "/daemon/repos/project/probe", "content": "probe-1" }
    }));
    assert_round_trip::<RepoLocationVerifyResult>(json!({
        "repo_location_id": "location-1",
        "path": "/daemon/repos/project",
        "default_branch_sha": "base-sha",
        "origin_url": null,
        "probe_content": "probe-1"
    }));
}

#[test]
fn workspace_prepare_round_trips() {
    assert_eq!(METHOD_WORKSPACE_PREPARE, "workspace.prepare");
    let params = assert_round_trip::<WorkspacePrepareParams>(mutation_params(json!({
        "repo_location_id": "location-1",
        "workspace_id": "workspace-1",
        "task_id": "task-1",
        "base_ref": "main",
        "branch": "forge/task-1"
    })));
    assert_eq!(params.fence.operation_id, "operation-1");
    assert_eq!(params.fence.generation, 2);
    assert_round_trip::<WorkspacePrepareResult>(prepared_result());
}

#[test]
fn workspace_describe_round_trips_execution_reconciliation_ids() {
    assert_eq!(METHOD_WORKSPACE_DESCRIBE, "workspace.describe");
    assert_round_trip::<WorkspaceDescribeParams>(workspace_params(json!({})));
    let result = assert_round_trip::<WorkspaceDescribeResult>(json!({
        "workspace_handle": "opaque-handle-1",
        "generation": 2,
        "exists": true,
        "head_sha": "head-sha",
        "dirty": false,
        "branch": "forge/task-1",
        "locked": true,
        "active_execution_ids": ["execution-active"],
        "journaled_execution_ids": ["execution-finished"]
    }));
    assert_eq!(result.active_execution_ids, ["execution-active"]);
    assert_eq!(result.journaled_execution_ids, ["execution-finished"]);
}

#[test]
fn workspace_run_round_trips_purpose_and_output_bounds() {
    assert_eq!(METHOD_WORKSPACE_RUN, "workspace.run");
    assert_round_trip::<WorkspaceRunParams>(mutation_params(json!({
        "workspace_handle": "opaque-handle-1",
        "purpose": "ci_step",
        "command": "cargo test -p api-types daemon_transport::tests",
        "env": [["CI", "true"]],
        "timeout_secs": 120,
        "max_output_bytes": 4096
    })));
    assert_round_trip::<WorkspaceRunResult>(json!({
        "entry_id": "journal-1",
        "operation_id": "operation-1",
        "exit_code": 1,
        "stdout": "output tail\n",
        "stderr": "error tail\n",
        "duration_ms": 1234,
        "timed_out": false,
        "stdout_truncated": true,
        "stderr_truncated": false,
        "stdout_drain_incomplete": false,
        "stderr_drain_incomplete": false
    }));
    assert!(serde_json::from_value::<WorkspaceRunPurpose>(json!("shell")).is_err());
    assert_round_trip::<WorkspaceRunPolicy>(json!({ "allowed_purposes": ["ci_step"] }));
}

#[test]
fn workspace_diff_round_trips() {
    assert_eq!(METHOD_WORKSPACE_DIFF, "workspace.diff");
    assert_round_trip::<WorkspaceDiffParams>(workspace_params(json!({
        "base_ref": "base-sha",
        "head_ref": null,
        "max_bytes": 4096
    })));
    assert_round_trip::<WorkspaceDiffResult>(json!({
        "base_ref": "main",
        "head_ref": "forge/task-1",
        "base_sha": "base-sha",
        "head_sha": "head-sha",
        "files": [{ "path": "README.md", "status": "modified", "additions": 2, "deletions": 1 }],
        "stats": { "files_changed": 1, "total_additions": 2, "total_deletions": 1 },
        "diff": "diff --git a/README.md b/README.md\n",
        "truncated": false
    }));
}

#[test]
fn workspace_read_round_trips_binary_content() {
    assert_eq!(METHOD_WORKSPACE_READ, "workspace.read");
    assert_round_trip::<WorkspaceReadParams>(workspace_params(json!({
        "path": "artifacts/screenshot.png",
        "limit": 1024
    })));
    let result = assert_round_trip::<WorkspaceReadResult>(json!({
        "path": "artifacts/screenshot.png",
        "bytes": [0, 137, 255],
        "truncated": true
    }));
    assert_eq!(result.bytes, [0, 137, 255]);
}

#[test]
fn workspace_merge_round_trips_embedded_outcomes_and_evidence() {
    assert_eq!(METHOD_WORKSPACE_MERGE, "workspace.merge");
    assert_round_trip::<WorkspaceMergeParams>(mutation_params(json!({
        "workspace_handle": "opaque-handle-1",
        "repo_location_id": "location-1",
        "target_branch": "main",
        "expected_target_sha": "target-sha",
        "handed_off_paths": ["src/main.rs"]
    })));
    for outcome in [
        json!({ "kind": "done", "before_sha": "target-sha", "after_sha": "merged-sha", "branch": "main" }),
        json!({ "kind": "conflict", "details": "conflict", "conflict_paths": ["src/main.rs"] }),
        json!({ "kind": "dirty", "files": ["src/main.rs"] }),
        json!({ "kind": "target_dirty", "files": ["README.md"] }),
        json!({ "kind": "review_required", "reason": "reviewed commit changed" }),
        json!({ "kind": "target_moved", "reason": "target changed", "target_branch": "main" }),
        json!({ "kind": "unresolved_conflict_markers", "paths": ["src/main.rs"] }),
    ] {
        assert_round_trip::<WorkspaceMergeResult>(json!({
            "entry_id": "journal-1",
            "operation_id": "operation-1",
            "outcome": outcome,
            "diffstat": { "files_changed": 1, "total_additions": 2, "total_deletions": 1 }
        }));
    }
}

#[test]
fn workspace_reset_round_trips() {
    assert_eq!(METHOD_WORKSPACE_RESET, "workspace.reset");
    assert_round_trip::<WorkspaceResetParams>(mutation_params(json!({
        "workspace_handle": "opaque-handle-1",
        "base_ref": "main",
        "branch": "forge/task-1"
    })));
    let mut result = prepared_result();
    result["generation"] = json!(3);
    assert_round_trip::<WorkspaceResetResult>(result);
}

#[test]
fn workspace_cleanup_round_trips_version_fence_and_journal_identity() {
    assert_eq!(METHOD_WORKSPACE_CLEANUP, "workspace.cleanup");
    let mut params = mutation_params(json!({ "workspace_handle": "opaque-handle-1" }));
    params["expected"] = json!({ "kind": "version", "version": 4 });
    let params = assert_round_trip::<WorkspaceCleanupParams>(params);
    assert_eq!(
        params.fence.expected,
        WorkspaceOperationExpected::Version { version: 4 }
    );
    assert_round_trip::<WorkspaceCleanupResult>(json!({
        "entry_id": "journal-cleanup-1",
        "operation_id": "operation-1",
        "workspace_handle": "opaque-handle-1",
        "generation": 2,
        "cleaned": true
    }));
}

#[test]
fn journal_ack_round_trips_without_execution_specific_fields() {
    assert_eq!(METHOD_JOURNAL_ACK, "journal.ack");
    assert_round_trip::<JournalAckParams>(json!({ "entry_id": "journal-1" }));
    assert_round_trip::<JournalAckResult>(json!({
        "entry_id": "journal-1",
        "acknowledged": true
    }));
    assert!(serde_json::from_value::<JournalAckParams>(json!({
        "terminal_report_id": "terminal-1",
        "execution_id": "execution-1"
    }))
    .is_err());
}

#[test]
fn absent_plan_preserves_retained_terminal_payload() {
    let value = json!({"terminal_report_id":"retained", "execution_id":"execution", "exit_code":0,
            "signal":null, "error":null, "ts":"now", "usage_reports":[]});
    let report: ExecutionTerminalNotification = serde_json::from_value(value.clone()).unwrap();
    assert!(report.plan_text.is_none());
    assert_eq!(serde_json::to_value(report).unwrap(), value);
}

#[test]
fn execution_terminal_outbox_round_trips_plan_worklog_and_evidence() {
    let value = json!({
        "terminal_report_id": "terminal-1",
        "execution_id": "execution-1",
        "exit_code": 0,
        "signal": null,
        "error": null,
        "ts": "2026-09-29T00:00:00Z",
        "usage_reports": [],
        "plan_text": "- [ ] transported plan\n",
        "outbox_entries": [
            { "type": "worklog", "position": "3", "kind": "validation", "summary": "20 tests passed" },
            { "type": "evidence", "position": "1", "kind": "log", "caption": "test log", "content": "20 tests OK\n" },
            {
                "type": "evidence", "position": "4", "kind": "screenshot",
                "caption": "captured result", "path": "artifacts/shot.png",
                "artifact": { "filename": "shot.png", "content_type": "image/png", "bytes": [137, 80, 78, 71, 0, 255] }
            }
        ]
    });
    let terminal = assert_round_trip::<ExecutionTerminalNotification>(value.clone());
    assert_eq!(terminal.outbox_entries.len(), 3);
    assert_eq!(
        terminal.plan_text.as_deref(),
        Some("- [ ] transported plan\n")
    );
    assert!(matches!(
        &terminal.outbox_entries[0],
        ExecutionOutboxEntry::Worklog {
            position,
            ..
        } if position == "3"
    ));
    let mut older = value;
    older
        .as_object_mut()
        .expect("terminal object")
        .remove("outbox_entries");
    let terminal: ExecutionTerminalNotification =
        serde_json::from_value(older).expect("revision 2 terminal notification");
    assert!(terminal.outbox_entries.is_empty());
}

#[test]
fn workspace_review_owner_operations_round_trip() {
    let workspace = json!({"daemon_id":"daemon", "runtime_id":"runtime", "placement_id":"placement",
            "workspace_handle":"opaque", "generation":1});
    let mut request = workspace.clone();
    request["operation"] = json!("git");
    request["query"] =
        json!({"kind":"candidate_paths", "base_sha":"base", "commit_sha":"candidate"});
    request["optional"] = json!(false);
    request["limit"] = json!(1024);
    assert_round_trip::<WorkspaceInspectParams>(request);
    let mut request = workspace.clone();
    request["operation"] = json!("files");
    request["path"] = json!("docs");
    request["repository"] = json!(true);
    request["max_entries"] = json!(5);
    request["max_bytes"] = json!(8192);
    assert_round_trip::<WorkspaceInspectParams>(request);
    let mut request = workspace.clone();
    request["operation_id"] = json!("operation");
    request["integration"] = json!({"kind":"task_step"});
    request["expected"] = json!({"kind":"base_sha", "sha":"candidate"});
    request["operation"] = json!({"kind":"review_checkout", "commit_sha":"candidate",
            "environment":{"env":{}, "assets":[], "checks":[],
                "recheck_interval_seconds":600}, "prepare":true});
    assert_round_trip::<WorkspaceOwnerOperationParams>(request);
    let mut request = workspace;
    request["integration"] = json!({"kind":"task_step"});
    request["operation"] = json!("reconcile");
    request["operation_id"] = json!("operation");
    assert_round_trip::<WorkspaceReconcileParams>(request);
    assert_round_trip::<WorkspaceReconcileResult>(
        json!({"entry_id":"entry", "operation_id":"operation",
            "outcome":{"kind":"error", "error":{"code":DAEMON_UNAVAILABLE,"message":"interrupted","details":null}},
            "owner_fence":null}),
    );
    let lookup = assert_round_trip::<WorkspaceReconcileResult>(
        json!({"entry_id":"entry", "operation_id":"operation",
            "outcome":{"kind":"result", "result":{}},
            "owner_fence":{"queue_id":"queue","generation":null,"attempt_id":null,"intent":"unknown"}}),
    );
    assert_eq!(
        lookup.owner_fence.unwrap().intent,
        IntegrationIntentRecord::Unknown
    );
    assert_round_trip::<IntegrationAnnounceParams>(
        json!({"daemon_id":"d","runtime_id":"r","fence":{"queue_id":"queue","attempt_id":"attempt","generation":2,"lease_owner":"worker","target_owner":{}},"live_queue_ids":["queue"]}),
    );
    assert_round_trip::<ImportObjectsResult>(
        json!({"kind":"imported","receipt":{"key":"a-1-out","tip_sha":"abc","ref_name":"refs/forge/integration/a-1-out","replayed":true}}),
    );
    assert_round_trip::<ObjectTransferRefusal>(json!({"kind":"too_large","bytes":3,"max_bytes":2}));
    assert_eq!(
        object_transfer_key("attempt", 7, ObjectTransferDirection::Inbound),
        "attempt-7-in"
    );
    assert_eq!(MAX_OBJECT_TRANSFER_BYTES, 256 * 1024 * 1024);
}

#[test]
fn workspace_error_codes_round_trip() {
    for (code, expected) in [
        (STALE_GENERATION, "stale_generation"),
        (WRONG_OWNER, "wrong_owner"),
        (PURPOSE_DENIED, "purpose_denied"),
        (OUTSIDE_WORKSPACE_ROOT, "outside_workspace_root"),
    ] {
        assert_eq!(code, expected);
        assert_round_trip::<DaemonErrorPayload>(json!({
            "code": code,
            "message": "operation refused",
            "details": { "placement_id": "placement-1" }
        }));
    }
}
