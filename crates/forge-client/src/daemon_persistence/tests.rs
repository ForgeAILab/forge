//! Daemon journal replay, retention, capacity, redaction, and crash recovery tests.

use super::*;

fn notification(id: &str) -> ExecutionTerminalNotification {
    serde_json::from_value(serde_json::json!({
        "terminal_report_id": id, "execution_id": "execution-1", "exit_code": 0,
        "signal": null, "error": null, "ts": "2026-09-29T00:00:00Z", "usage_reports": []
    }))
    .unwrap()
}

#[test]
fn retain_replay_and_authenticated_ack_are_crash_safe() {
    let dir = tempfile::tempdir().unwrap();
    let store = DaemonJournal::new(dir.path());
    let report = notification("report/one");
    store.retain(&report).unwrap();
    store.retain(&report).unwrap();
    let restarted = DaemonJournal::new(dir.path());
    assert_eq!(
        restarted.pending().unwrap()[0]
            .replay_notification()
            .unwrap()
            .1,
        serde_json::to_value(report).unwrap()
    );
    assert!(
        restarted
            .acknowledge(&JournalAckParams {
                entry_id: "report/one".into()
            })
            .unwrap()
            .acknowledged
    );
    assert!(restarted.pending().unwrap().is_empty());
    assert!(
        restarted
            .acknowledge(&JournalAckParams {
                entry_id: "report/one".into()
            })
            .unwrap()
            .acknowledged
    );
}

#[test]
fn exact_duplicate_is_noop_and_conflicting_replay_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store = DaemonJournal::new(dir.path());
    let report = notification("report-1");
    store.retain(&report).unwrap();
    let mut other = report.clone();
    other.summary = Some("different".into());
    assert!(store
        .retain(&other)
        .unwrap_err()
        .to_string()
        .contains(TERMINAL_REPORT_CONFLICT));
    assert_eq!(
        store.pending().unwrap()[0].replay_notification().unwrap().1,
        serde_json::to_value(report).unwrap()
    );
}

#[test]
fn queue_bounds_preserve_unacknowledged_reports() {
    let dir = tempfile::tempdir().unwrap();
    let store = DaemonJournal::with_limits(dir.path(), 1, u64::MAX);
    store.retain(&notification("one")).unwrap();
    assert!(store
        .retain(&notification("two"))
        .unwrap_err()
        .to_string()
        .contains("record bound"));
    assert_eq!(store.pending().unwrap().len(), 1);
}

fn attempt_receipt(generation: i64) -> JournalOperation {
    attempt_receipt_on("queue-1", generation)
}

fn attempt_receipt_on(queue: &str, generation: i64) -> JournalOperation {
    let request = api_types::WorkspaceIntegrationRequest {
        fence: api_types::IntegrationOwnerFence {
            queue_id: queue.into(),
            attempt_id: format!("attempt-{queue}"),
            generation,
            lease_owner: "queue-worker".into(),
            target_owner: serde_json::json!({"owner_kind":"daemon"}),
        },
        kind: api_types::WorkspaceIntegrationKind::FastForward,
        witness: serde_json::json!({}),
    };
    let mut operation = run_intent(&request.operation_id());
    operation.method = api_types::METHOD_WORKSPACE_MERGE.into();
    operation.request = serde_json::json!({});
    operation.fence.integration = api_types::WorkspaceIntegrationBinding::Attempt { request };
    operation.outcome = Some(Ok(serde_json::json!({"outcome":{"kind":"done"}})));
    operation
}

/// Two hundred merged attempts on one queue never approach the journal
/// bound: each acknowledged receipt is pruned by the next generation.
#[test]
fn acknowledged_attempt_receipts_are_retained_then_pruned_within_the_bound() {
    let dir = tempfile::tempdir().unwrap();
    let store = DaemonJournal::with_limits(dir.path(), 4, MAX_JOURNAL_BYTES);
    store.initialize().unwrap();
    for generation in 1..=200_i64 {
        let operation = attempt_receipt(generation);
        store
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
        // Unacknowledged receipts are never prunable.
        assert!(store
            .prune_acknowledged_attempts(|candidate, _| candidate.entry_id == operation.entry_id)
            .unwrap()
            .is_empty());
        assert!(
            store
                .acknowledge(&JournalAckParams {
                    entry_id: operation.entry_id.clone(),
                })
                .unwrap()
                .acknowledged
        );
        // Acknowledged: kept for a duplicate of the same key, no longer
        // replayed, and it survives a restart.
        let kept = store
            .operation(&operation.fence.operation_id)
            .unwrap()
            .unwrap();
        assert!(kept.acknowledged);
        assert!(store.pending().unwrap().is_empty());
        if generation % 50 == 0 {
            let restarted = DaemonJournal::with_limits(dir.path(), 4, MAX_JOURNAL_BYTES);
            restarted.initialize().unwrap();
            assert!(restarted
                .operation(&operation.fence.operation_id)
                .unwrap()
                .is_some());
        }
        let removed = store
                .prune_acknowledged_attempts(|operation, _| {
                    matches!(&operation.fence.integration, api_types::WorkspaceIntegrationBinding::Attempt { request } if request.fence.generation < generation)
                })
                .unwrap();
        assert_eq!(removed.len(), usize::from(generation > 1));
    }
    let entries = fs::read_dir(store.directory())
        .unwrap()
        .filter(|entry| is_entry_path(&entry.as_ref().unwrap().path()))
        .count();
    assert_eq!(entries, 1);
}

/// A server that never acknowledges. The journal's bound is what it has
/// not been acknowledged for: `max_entries` records (1024 in production)
/// and `max_bytes` (32 MiB). At the bound a new record is refused, so the
/// owner admits no further effect; nothing is evicted to make room, the
/// directory does not grow, and the owner's pruning rules (fence moved
/// on, age, number) cannot touch a receipt the server has not stored,
/// whatever they say.
#[test]
fn a_server_that_never_acknowledges_neither_grows_the_journal_nor_loses_a_receipt() {
    fn usage(root: &Path) -> (usize, u64) {
        let mut total = (0, 0);
        let mut dirs = vec![root.to_owned()];
        while let Some(dir) = dirs.pop() {
            for entry in fs::read_dir(dir).unwrap() {
                let entry = entry.unwrap();
                let metadata = entry.metadata().unwrap();
                if metadata.is_dir() {
                    dirs.push(entry.path());
                } else {
                    total = (total.0 + 1, total.1 + metadata.len());
                }
            }
        }
        total
    }
    let dir = tempfile::tempdir().unwrap();
    let store = DaemonJournal::with_limits(dir.path(), 8, MAX_JOURNAL_BYTES);
    store.initialize().unwrap();
    let kept: Vec<JournalOperation> = (0..8)
        .map(|index| attempt_receipt_on(&format!("queue-{index}"), 1))
        .collect();
    for operation in &kept {
        store
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
    }
    let full = usage(dir.path());
    // Two hundred more claims arrive; the server has acknowledged nothing.
    for index in 0..200 {
        let refused = store
            .retain_entry(&JournalEntry::Operation {
                operation: attempt_receipt_on(&format!("over-{index}"), 1),
            })
            .unwrap_err()
            .to_string();
        assert!(refused.contains("record bound"), "{refused}");
    }
    assert_eq!(usage(dir.path()), full, "a refused record writes nothing");
    // The pruning rules are handed "prune everything": nothing goes.
    assert!(store
        .prune_acknowledged_attempts(|_, _| true)
        .unwrap()
        .is_empty());
    assert_eq!(usage(dir.path()), full);
    assert_eq!(store.pending().unwrap().len(), 8);
    for operation in &kept {
        let stored = store
            .operation(&operation.fence.operation_id)
            .unwrap()
            .unwrap();
        assert!(!stored.acknowledged);
        assert_eq!(stored.entry_id, operation.entry_id);
    }
    // The byte bound refuses the same way.
    let small = tempfile::tempdir().unwrap();
    let bytes = DaemonJournal::with_limits(small.path(), MAX_JOURNAL_ENTRIES, 4096);
    bytes.initialize().unwrap();
    let mut accepted = 0;
    let refused = loop {
        match bytes.retain_entry(&JournalEntry::Operation {
            operation: attempt_receipt_on(&format!("bytes-{accepted}"), 1),
        }) {
            Ok(_) => accepted += 1,
            Err(error) => break error.to_string(),
        }
        assert!(accepted < 64, "the byte bound never refused");
    };
    assert!(refused.contains("byte bound"), "{refused}");
    assert_eq!(bytes.pending().unwrap().len(), accepted);
    assert!(bytes
        .prune_acknowledged_attempts(|_, _| true)
        .unwrap()
        .is_empty());
    // Once the server acknowledges, the slots are free again, and only
    // then do the pruning rules apply: to the acknowledged receipts.
    for operation in &kept {
        store
            .acknowledge(&JournalAckParams {
                entry_id: operation.entry_id.clone(),
            })
            .unwrap();
    }
    let later: Vec<JournalOperation> = (0..8)
        .map(|index| attempt_receipt_on(&format!("later-{index}"), 1))
        .collect();
    for operation in &later {
        store
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
    }
    let mut removed = store.prune_acknowledged_attempts(|_, _| true).unwrap();
    removed.sort();
    let mut acknowledged: Vec<String> = kept
        .iter()
        .map(|operation| operation.fence.operation_id.clone())
        .collect();
    acknowledged.sort();
    assert_eq!(removed, acknowledged);
    assert_eq!(store.pending().unwrap().len(), 8);
    for operation in &later {
        assert!(store
            .operation(&operation.fence.operation_id)
            .unwrap()
            .is_some());
    }
}

/// Four hundred acknowledged attempt receipts on a journal bounded at
/// four records (each write is several fsyncs, so the count is kept to
/// what runs in seconds): none of them takes a slot, so the bound keeps
/// meaning "work the server has not acknowledged". The fifth
/// unacknowledged entry is refused, no acknowledged or unacknowledged
/// receipt is evicted to make room, and a restart counts the same way.
#[test]
fn acknowledged_attempt_receipts_never_fill_the_record_bound() {
    let dir = tempfile::tempdir().unwrap();
    let store = DaemonJournal::with_limits(dir.path(), 4, MAX_JOURNAL_BYTES);
    store.initialize().unwrap();
    for index in 0..400 {
        let operation = attempt_receipt_on(&format!("queue-{index}"), 1);
        store
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
        store
            .acknowledge(&JournalAckParams {
                entry_id: operation.entry_id.clone(),
            })
            .unwrap();
    }
    assert_eq!(store.acknowledged_attempt_ages().unwrap().len(), 400);
    let unacknowledged: Vec<JournalOperation> = (0..4)
        .map(|index| attempt_receipt_on(&format!("pending-{index}"), 1))
        .collect();
    for operation in &unacknowledged {
        store
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
    }
    let refused = |store: &DaemonJournal| {
        store
            .retain_entry(&JournalEntry::Operation {
                operation: attempt_receipt_on("pending-over", 1),
            })
            .unwrap_err()
            .to_string()
    };
    assert!(refused(&store).contains("record bound"));
    let restarted = DaemonJournal::with_limits(dir.path(), 4, MAX_JOURNAL_BYTES);
    restarted.initialize().unwrap();
    assert!(refused(&restarted).contains("record bound"));
    assert_eq!(restarted.pending().unwrap().len(), 4);
    assert_eq!(restarted.acknowledged_attempt_ages().unwrap().len(), 400);
    // Nothing was evicted: every receipt, acknowledged or not, is there.
    for operation in &unacknowledged {
        assert!(
            !restarted
                .operation(&operation.fence.operation_id)
                .unwrap()
                .unwrap()
                .acknowledged
        );
    }
    assert!(
        restarted
            .operation(&attempt_receipt_on("queue-0", 1).fence.operation_id)
            .unwrap()
            .unwrap()
            .acknowledged
    );
    // Acknowledging one frees its slot.
    restarted
        .acknowledge(&JournalAckParams {
            entry_id: unacknowledged[0].entry_id.clone(),
        })
        .unwrap();
    restarted
        .retain_entry(&JournalEntry::Operation {
            operation: attempt_receipt_on("pending-over", 1),
        })
        .unwrap();
    // The owner's rule prunes by number: the oldest go first.
    let ages = restarted.acknowledged_attempt_ages().unwrap();
    assert_eq!(ages.len(), 401);
    // (Ages only grow between the listing and the prune, so a few more
    // than the 201 strictly older ones may go; never an unacknowledged one.)
    let removed = restarted
        .prune_acknowledged_attempts(|_, age| age > ages[199])
        .unwrap();
    assert!(removed.len() >= 201, "{}", removed.len());
    assert_eq!(
        restarted.acknowledged_attempt_ages().unwrap().len(),
        401 - removed.len()
    );
    assert_eq!(restarted.pending().unwrap().len(), 4);
}

/// A long-lived daemon serving many queues that each merged once and
/// went idle: the age rule removes their acknowledged receipts, so the
/// journal does not grow without bound.
#[test]
fn idle_queue_receipts_expire_by_age() {
    let dir = tempfile::tempdir().unwrap();
    let store = DaemonJournal::with_limits(dir.path(), 301, MAX_JOURNAL_BYTES);
    store.initialize().unwrap();
    let queues = 300;
    let week = std::time::Duration::from_secs(7 * 24 * 60 * 60);
    for index in 0..queues {
        let operation = attempt_receipt_on(&format!("queue-{index}"), 1);
        store
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
        store
            .acknowledge(&JournalAckParams {
                entry_id: operation.entry_id.clone(),
            })
            .unwrap();
        if index % 3 != 0 {
            // Acknowledged more than a week ago.
            fs::File::options()
                .write(true)
                .open(store.path_for_entry(&operation.entry_id).unwrap())
                .unwrap()
                .set_modified(std::time::SystemTime::now() - week - week)
                .unwrap();
        }
    }
    assert!(store.pending().unwrap().is_empty());
    store
        .retain_entry(&JournalEntry::Operation {
            operation: attempt_receipt_on("queue-new", 1),
        })
        .unwrap();
    // Acknowledged receipts hold no slot of the record bound.
    store
        .retain_entry(&JournalEntry::Operation {
            operation: attempt_receipt_on("queue-over", 1),
        })
        .unwrap();
    let removed = store
        .prune_acknowledged_attempts(|_, acknowledged_for| acknowledged_for > week)
        .unwrap();
    assert_eq!(removed.len(), 200);
    store
        .retain_entry(&JournalEntry::Operation {
            operation: attempt_receipt_on("queue-over", 1),
        })
        .unwrap();
    let restarted = DaemonJournal::with_limits(dir.path(), 301, MAX_JOURNAL_BYTES);
    restarted.initialize().unwrap();
    let entries = fs::read_dir(restarted.directory())
        .unwrap()
        .filter(|entry| is_entry_path(&entry.as_ref().unwrap().path()))
        .count();
    assert_eq!(entries, 102, "100 recent receipts and two pending intents");
    assert_eq!(restarted.pending().unwrap().len(), 2);
}

#[test]
fn acknowledged_operation_receipts_do_not_consume_the_pending_queue_bound() {
    let dir = tempfile::tempdir().unwrap();
    let store = DaemonJournal::with_limits(dir.path(), 1, 2048);
    let operation = JournalOperation {
        entry_id: operation_entry_id("run-once"),
        fence: WorkspaceMutationFence {
            integration: api_types::WorkspaceIntegrationBinding::TaskStep,
            daemon_id: "daemon-1".into(),
            runtime_id: "runtime-1".into(),
            placement_id: "placement-1".into(),
            operation_id: "run-once".into(),
            generation: 1,
            expected: api_types::WorkspaceOperationExpected::Version { version: 1 },
        },
        workspace_handle: Some("handle".into()),
        method: api_types::METHOD_WORKSPACE_RUN.into(),
        request: serde_json::json!({"command":"printf once"}),
        outcome: Some(Ok(serde_json::json!({"stdout":"once"}))),
        acknowledged: false,
        effect_started: true,
    };
    store
        .retain_entry(&JournalEntry::Operation {
            operation: operation.clone(),
        })
        .unwrap();
    store
        .acknowledge(&JournalAckParams {
            entry_id: operation.entry_id,
        })
        .unwrap();
    store.retain(&notification("terminal-after-run")).unwrap();
    assert_eq!(store.pending().unwrap().len(), 1);
    assert!(store.operation("run-once").unwrap().is_none());
}

#[test]
fn operation_receipt_is_bounded_replays_before_ack_and_shrinks_after_ack() {
    let dir = tempfile::tempdir().unwrap();
    let store = DaemonJournal::with_limits(dir.path(), 2, 2 * MAX_CI_LOG_BYTES as u64);
    let mut operation = JournalOperation {
        entry_id: operation_entry_id("ci-result"),
        fence: WorkspaceMutationFence {
            integration: api_types::WorkspaceIntegrationBinding::TaskStep,
            daemon_id: "daemon-1".into(),
            runtime_id: "runtime-1".into(),
            placement_id: "placement-1".into(),
            operation_id: "ci-result".into(),
            generation: 1,
            expected: api_types::WorkspaceOperationExpected::Version { version: 1 },
        },
        workspace_handle: Some("handle".into()),
        method: METHOD_WORKSPACE_RUN.into(),
        request: serde_json::json!({"purpose":"ci_step", "max_output_bytes":u64::MAX}),
        outcome: None,
        acknowledged: false,
        effect_started: true,
    };
    store
        .retain_entry(&JournalEntry::Operation {
            operation: operation.clone(),
        })
        .unwrap();
    operation.outcome = Some(Ok(
        serde_json::json!({"stdout": "🦀".repeat(MAX_CI_LOG_BYTES), "stderr":""}),
    ));
    store.finish_operation(&operation).unwrap();
    drop(store); // Crash between durable completion and server acknowledgement.
    let restarted = DaemonJournal::with_limits(dir.path(), 2, 2 * MAX_CI_LOG_BYTES as u64);
    let receipt = restarted.operation("ci-result").unwrap().unwrap();
    let result = receipt.outcome.unwrap().unwrap();
    let stdout = result["stdout"].as_str().unwrap();
    assert!(stdout.len() <= MAX_CI_LOG_BYTES);
    assert!(stdout.starts_with(CI_LOG_TRUNCATION_MARKER));
    assert_eq!(result["stdout_truncated"], true);
    let before = restarted.current_usage().unwrap().1;
    let too_small = DaemonJournal::with_limits(dir.path(), 2, before);
    assert!(too_small
        .retain(&notification("would-overflow"))
        .unwrap_err()
        .to_string()
        .contains("byte bound"));
    restarted
        .acknowledge(&JournalAckParams {
            entry_id: operation.entry_id.clone(),
        })
        .unwrap();
    assert!(restarted.current_usage().unwrap().1 < before);
    assert!(!restarted
        .path_for_entry(&operation.entry_id)
        .unwrap()
        .exists());
    assert!(restarted.operation("ci-result").unwrap().is_none());
    assert!(
        restarted
            .acknowledge(&JournalAckParams {
                entry_id: operation.entry_id.clone()
            })
            .unwrap()
            .acknowledged
    );
    // Startup compacts pre-fix oversized CI logs to the shared byte cap.
    let mut legacy = operation;
    legacy.outcome = Some(Ok(
        serde_json::json!({"stdout": "x".repeat(MAX_JOURNAL_ENTRY_SIZE + 1), "stderr":""}),
    ));
    fs::write(
        restarted.path_for_entry(&legacy.entry_id).unwrap(),
        serde_json::to_vec(&JournalEntry::Operation {
            operation: legacy.clone(),
        })
        .unwrap(),
    )
    .unwrap();
    legacy.fence.operation_id = "other-legacy-ci".into();
    legacy.entry_id = operation_entry_id(&legacy.fence.operation_id);
    legacy.outcome = Some(Ok(
        serde_json::json!({"stdout": "x".repeat(MAX_CI_LOG_BYTES + 1), "stderr":""}),
    ));
    fs::write(
        restarted.path_for_entry(&legacy.entry_id).unwrap(),
        serde_json::to_vec(&JournalEntry::Operation { operation: legacy }).unwrap(),
    )
    .unwrap();
    let migrated = DaemonJournal::with_limits(dir.path(), 2, 2 * MAX_CI_LOG_BYTES as u64);
    migrated.initialize().unwrap();
    assert!(migrated.current_usage().unwrap().1 <= 2 * MAX_CI_LOG_BYTES as u64);
    assert_eq!(migrated.pending().unwrap().len(), 2);
}

#[test]
fn revision_three_journal_is_migrated_without_accepting_old_wire() {
    let dir = tempfile::tempdir().unwrap();
    let store = DaemonJournal::new(dir.path());
    let operation = run_intent("old-integration-intent");
    let entry_id = operation.entry_id.clone();
    store
        .retain_entry(&JournalEntry::Operation { operation })
        .unwrap();
    let path = store.path_for_entry(&entry_id).unwrap();
    let mut old: Value = read_json(&path).unwrap();
    old["operation"]["fence"]
        .as_object_mut()
        .unwrap()
        .remove("integration");
    old["operation"]["request"]
        .as_object_mut()
        .unwrap()
        .remove("integration");
    old["operation"]
        .as_object_mut()
        .unwrap()
        .remove("effect_started");
    assert!(
        serde_json::from_value::<WorkspaceMutationFence>(old["operation"]["fence"].clone())
            .is_err()
    );
    old["operation"]["request"]["operation_id"] = serde_json::json!("old-integration-intent");
    fs::write(&path, serde_json::to_vec(&old).unwrap()).unwrap();
    let restart = DaemonJournal::new(dir.path());
    restart.initialize().unwrap();
    let migrated = restart
        .operation("old-integration-intent")
        .unwrap()
        .unwrap();
    assert!(matches!(
        migrated.fence.integration,
        api_types::WorkspaceIntegrationBinding::TaskStep
    ));
    assert!(migrated.effect_started);
    assert!(migrated.outcome.is_none());
    let disk: Value = read_json(&path).unwrap();
    assert_eq!(
        disk["operation"]["request"]["integration"]["kind"],
        "task_step"
    );
}

#[test]
fn operation_journal_never_persists_environment_secret_values() {
    let dir = tempfile::tempdir().unwrap();
    let journal = DaemonJournal::new(dir.path());
    let secret = "super-secret-token-123";
    let mut operation = JournalOperation {
        entry_id: operation_entry_id("secret-run"),
        fence: WorkspaceMutationFence {
            integration: api_types::WorkspaceIntegrationBinding::TaskStep,
            daemon_id: "daemon-1".into(),
            runtime_id: "runtime-1".into(),
            placement_id: "placement-1".into(),
            operation_id: "secret-run".into(),
            generation: 1,
            expected: api_types::WorkspaceOperationExpected::Version { version: 1 },
        },
        workspace_handle: Some("handle".into()),
        method: METHOD_WORKSPACE_RUN.into(),
        request: serde_json::json!({"env":[["TOKEN",secret]], "command":format!("printf {secret}")}),
        outcome: None,
        acknowledged: false,
        effect_started: true,
    };
    journal
        .retain_entry(&JournalEntry::Operation {
            operation: operation.clone(),
        })
        .unwrap();
    let path = journal.path_for_entry(&operation.entry_id).unwrap();
    assert!(!fs::read_to_string(&path).unwrap().contains(secret));
    operation.outcome = Some(Ok(serde_json::json!({"stdout":secret})));
    journal.finish_operation(&operation).unwrap();
    let stored = fs::read_to_string(path).unwrap();
    assert!(!stored.contains(secret));
    let receipt = journal.operation("secret-run").unwrap().unwrap();
    assert_eq!(receipt.request["env"], serde_json::json!(["TOKEN"]));
    assert_eq!(receipt.request, journal_request(&operation.request));
    let mut changed = operation.request.clone();
    changed["env"][0][1] = serde_json::json!("changed-secret");
    changed["command"] = serde_json::json!("printf changed-secret");
    assert_eq!(receipt.request, journal_request(&changed)); // Secret values are deliberately excluded from replay identity.
    assert_eq!(receipt.outcome.unwrap().unwrap()["stdout"], "[REDACTED]");
    // Startup also scrubs receipts written by the pre-fix daemon.
    let path = journal.path_for_entry(&operation.entry_id).unwrap();
    drop(journal);
    fs::write(
        &path,
        serde_json::to_vec(&JournalEntry::Operation { operation }).unwrap(),
    )
    .unwrap();
    let restarted = DaemonJournal::new(dir.path());
    restarted.initialize().unwrap();
    assert!(!fs::read_to_string(path).unwrap().contains(secret));
    for kind in ["review_checkout", "materialize_assets"] {
        let mut operation = run_intent(kind);
        operation.method = api_types::METHOD_WORKSPACE_RESET.into();
        operation.request = serde_json::json!({"operation": {
            "kind": kind, "commit_sha":"commit-1", "prepare":true,
            "environment": {"env":{"TOKEN":secret}, "checks":[{"command":format!("echo {secret}")}], "assets":[]}
        }});
        restarted
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
        let path = restarted.path_for_entry(&operation.entry_id).unwrap();
        assert!(!fs::read_to_string(&path).unwrap().contains(secret));
        operation.outcome = Some(Err(DaemonErrorPayload {
            code: "check_failed".into(),
            message: format!("check printed {secret}"),
            details: None,
        }));
        restarted.finish_operation(&operation).unwrap();
        assert!(!fs::read_to_string(&path).unwrap().contains(secret));
        let receipt = restarted.operation(kind).unwrap().unwrap();
        assert_eq!(
            receipt.request["operation"]["environment"]["env"],
            serde_json::json!(["TOKEN"])
        );
        assert_eq!(
            receipt.request["operation"]["environment"]["checks"][0]["command"],
            "echo [REDACTED]"
        );
        assert_eq!(
            receipt.outcome.unwrap().unwrap_err().message,
            "check printed [REDACTED]"
        );
        // Existing raw owner-operation receipts are scrubbed on startup too.
        fs::write(
            &path,
            serde_json::to_vec(&JournalEntry::Operation { operation }).unwrap(),
        )
        .unwrap();
        DaemonJournal::new(dir.path()).initialize().unwrap();
        assert!(!fs::read_to_string(path).unwrap().contains(secret));
    }
}

fn run_intent(id: &str) -> JournalOperation {
    JournalOperation {
        entry_id: operation_entry_id(id),
        fence: WorkspaceMutationFence {
            integration: api_types::WorkspaceIntegrationBinding::TaskStep,
            daemon_id: "daemon-1".into(),
            runtime_id: "runtime-1".into(),
            placement_id: "placement-1".into(),
            operation_id: id.into(),
            generation: 1,
            expected: api_types::WorkspaceOperationExpected::Version { version: 1 },
        },
        workspace_handle: Some("handle-1".into()),
        method: METHOD_WORKSPACE_RUN.into(),
        request: serde_json::json!({"command":"echo 1", "env":[["CI","1"]], "max_output_bytes":u64::MAX}),
        outcome: None,
        acknowledged: false,
        effect_started: true,
    }
}

#[test]
fn short_environment_values_preserve_result_and_error_identity() {
    let dir = tempfile::tempdir().unwrap();
    let journal = DaemonJournal::new(dir.path());
    for failed in [false, true] {
        let mut operation = run_intent(if failed { "error-1" } else { "result-1" });
        let entry_id = operation.entry_id.clone();
        let operation_id = operation.fence.operation_id.clone();
        journal
            .retain_entry(&JournalEntry::Operation {
                operation: operation.clone(),
            })
            .unwrap();
        operation.outcome = Some(if failed {
            Err(DaemonErrorPayload {
                code: "error-1".into(),
                message: "printed 1".into(),
                details: Some(serde_json::json!({"entry_id":entry_id,"operation_id":operation_id})),
            })
        } else {
            Ok(
                serde_json::json!({"entry_id":entry_id, "operation_id":operation_id,
                    "exit_code":1, "stdout":"1", "stderr":"1"}),
            )
        });
        journal.finish_operation(&operation).unwrap();
        let restarted = DaemonJournal::new(dir.path());
        restarted.initialize().unwrap();
        let stored = restarted.operation(&operation_id).unwrap().unwrap();
        assert_eq!(stored.entry_id, entry_id);
        assert_eq!(stored.fence.operation_id, operation_id);
        assert_eq!(stored.fence.daemon_id, "daemon-1");
        assert_eq!(stored.workspace_handle.as_deref(), Some("handle-1"));
        match stored.outcome.unwrap() {
            Ok(value) => {
                assert_eq!(value["entry_id"], entry_id);
                assert_eq!(value["operation_id"], operation_id);
                assert_eq!(value["exit_code"], 1);
                assert_eq!(value["stdout"], "[REDACTED]");
                assert_eq!(value["stderr"], "[REDACTED]");
            }
            Err(error) => {
                assert_eq!(error.code, "error-1");
                assert_eq!(error.message, "printed [REDACTED]");
                assert_eq!(error.details.unwrap()["entry_id"], entry_id);
            }
        }
        let bytes = fs::read_to_string(restarted.path_for_entry(&entry_id).unwrap()).unwrap();
        // Numeric exit codes and IDs containing 1 stay intact; the secret
        // string value itself is absent from the persisted JSON bytes.
        assert!(!bytes.contains("\"1\""));
        assert!(!bytes.contains("echo 1"));
        assert!(
            restarted
                .acknowledge(&JournalAckParams { entry_id })
                .unwrap()
                .acknowledged
        );
        assert!(restarted.operation(&operation_id).unwrap().is_none());
    }
}

#[test]
fn caller_digest_cannot_skip_redaction_or_hash_secret_values() {
    let request =
        serde_json::json!({"command":"echo 1", "env":[["CI","1"]], "_request_digest":"trusted?"});
    let clean = journal_request(&request);
    assert_eq!(clean["command"], "echo [REDACTED]");
    assert_eq!(clean["env"], serde_json::json!(["CI"]));
    assert_ne!(clean["_request_digest"], "trusted?");
    assert_eq!(clean, journal_request(&clean));
    assert_eq!(
        clean,
        journal_request(&serde_json::json!({"command":"echo 0", "env":[["CI","0"]]}))
    );
}

#[test]
fn journal_tail_is_utf8_safe_and_stable_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let journal = DaemonJournal::new(dir.path());
    let mut operation = run_intent("tail");
    operation.outcome = Some(Ok(serde_json::json!({
        "stdout":format!("{}last failure lines", "🦀".repeat(MAX_CI_LOG_BYTES)),
        "stderr":"", "stdout_truncated":true
    })));
    journal
        .retain_entry(&JournalEntry::Operation {
            operation: operation.clone(),
        })
        .unwrap();
    let path = journal.path_for_entry(&operation.entry_id).unwrap();
    let bytes = fs::read(&path).unwrap();
    for _ in 0..3 {
        let restarted = DaemonJournal::new(dir.path());
        restarted.initialize().unwrap();
        assert_eq!(fs::read(&path).unwrap(), bytes);
        let result = restarted
            .operation("tail")
            .unwrap()
            .unwrap()
            .outcome
            .unwrap()
            .unwrap();
        let tail = result["stdout"].as_str().unwrap();
        assert!(tail.starts_with(CI_LOG_TRUNCATION_MARKER));
        assert!(tail.ends_with("last failure lines"));
        assert_eq!(tail.matches(CI_LOG_TRUNCATION_MARKER).count(), 1);
    }
}

#[test]
fn undecodable_entry_is_quarantined_without_blocking_replay() {
    let dir = tempfile::tempdir().unwrap();
    let journal = DaemonJournal::new(dir.path());
    journal.retain(&notification("valid")).unwrap();
    let corrupt = journal.directory().join("entry-bad.json");
    fs::write(&corrupt, b"{partial").unwrap();
    let restarted = DaemonJournal::new(dir.path());
    restarted.initialize().unwrap();
    assert!(!corrupt.exists());
    assert_eq!(
        fs::read(journal.directory().join("corrupt-entry-bad.json")).unwrap(),
        b"{partial"
    );
    assert_eq!(restarted.pending().unwrap().len(), 1);
    restarted.retain(&notification("next")).unwrap();
}

#[test]
fn quarantine_failures_and_non_regular_entries_do_not_stop_replay() {
    let dir = tempfile::tempdir().unwrap();
    let journal = DaemonJournal::new(dir.path());
    journal.retain(&notification("valid")).unwrap();
    let corrupt = journal.directory().join("entry-bad.json");
    fs::write(&corrupt, b"{partial").unwrap();
    let blocked = journal.directory().join("corrupt-entry-bad.json");
    fs::create_dir(&blocked).unwrap();
    fs::write(blocked.join("occupied"), b"keep").unwrap();
    fs::create_dir(journal.directory().join("entry-directory.json")).unwrap();
    let restarted = DaemonJournal::new(dir.path());
    restarted
        .fail_next_directory_sync
        .store(true, Ordering::Relaxed);
    restarted.initialize().unwrap();
    assert!(corrupt.exists()); // Rename failed, but the entry is skipped.
    assert_eq!(restarted.pending().unwrap().len(), 1);
    restarted.retain(&notification("next")).unwrap();
    assert_eq!(restarted.pending().unwrap().len(), 2);
}

#[test]
fn skipped_entries_are_absent_for_ack_retain_and_finish() {
    let dir = tempfile::tempdir().unwrap();
    let journal = DaemonJournal::new(dir.path());
    journal.retain(&notification("valid")).unwrap();
    let mut operation = run_intent("quarantine-blocked");
    let path = journal.path_for_entry(&operation.entry_id).unwrap();
    fs::write(&path, b"{broken").unwrap();
    let quarantine = journal.directory().join(format!(
        "corrupt-{}",
        path.file_name().unwrap().to_string_lossy()
    ));
    fs::create_dir(&quarantine).unwrap();
    fs::write(quarantine.join("occupied"), b"keep").unwrap();
    let restarted = DaemonJournal::new(dir.path());
    restarted.initialize().unwrap();
    assert!(
        restarted
            .acknowledge(&JournalAckParams {
                entry_id: operation.entry_id.clone()
            })
            .unwrap()
            .acknowledged
    );
    operation.outcome = Some(Ok(serde_json::json!({"exit_code":0})));
    assert!(restarted
        .finish_operation(&operation)
        .unwrap_err()
        .to_string()
        .contains("intent is missing"));
    operation.outcome = None;
    restarted
        .retain_entry(&JournalEntry::Operation {
            operation: operation.clone(),
        })
        .unwrap();
    assert!(restarted
        .operation(&operation.fence.operation_id)
        .unwrap()
        .is_some());
    operation.outcome = Some(Ok(serde_json::json!({"exit_code":0})));
    restarted.finish_operation(&operation).unwrap();
    assert_eq!(restarted.pending().unwrap().len(), 2);
}

#[test]
fn startup_scratch_removal_and_acknowledged_entry_sync_errors_do_not_stop_replay() {
    let dir = tempfile::tempdir().unwrap();
    let journal = DaemonJournal::new(dir.path());
    journal.retain(&notification("valid")).unwrap();
    let mut operation = run_intent("old-acknowledged");
    operation.outcome = Some(Ok(serde_json::json!({"exit_code":0})));
    operation.acknowledged = true;
    fs::write(
        journal.path_for_entry(&operation.entry_id).unwrap(),
        serde_json::to_vec(&JournalEntry::Operation { operation }).unwrap(),
    )
    .unwrap();
    fs::create_dir(journal.directory().join(".journal.tmp.blocked")).unwrap();
    let restarted = DaemonJournal::new(dir.path());
    restarted
        .fail_next_directory_sync
        .store(true, Ordering::Relaxed);
    restarted.initialize().unwrap();
    assert_eq!(restarted.pending().unwrap().len(), 1);
}

#[cfg(unix)]
#[test]
fn startup_acknowledged_entry_removal_failure_is_skipped() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let journal = DaemonJournal::new(dir.path());
    journal.retain(&notification("valid")).unwrap();
    let mut operation = run_intent("cannot-remove");
    operation.outcome = Some(Ok(serde_json::json!({"exit_code":0})));
    operation.acknowledged = true;
    fs::write(
        journal.path_for_entry(&operation.entry_id).unwrap(),
        serde_json::to_vec(&JournalEntry::Operation {
            operation: operation.clone(),
        })
        .unwrap(),
    )
    .unwrap();
    let permissions = fs::metadata(journal.directory()).unwrap().permissions();
    fs::set_permissions(journal.directory(), fs::Permissions::from_mode(0o500)).unwrap();
    let restarted = DaemonJournal::new(dir.path());
    let result = restarted.initialize();
    fs::set_permissions(journal.directory(), permissions).unwrap();
    result.unwrap();
    assert_eq!(restarted.pending().unwrap().len(), 1);
    assert!(
        restarted
            .acknowledge(&JournalAckParams {
                entry_id: operation.entry_id
            })
            .unwrap()
            .acknowledged
    );
}

#[test]
fn completion_headroom_survives_a_full_shared_journal_without_logs() {
    let dir = tempfile::tempdir().unwrap();
    let journal = DaemonJournal::with_limits(dir.path(), 16, 16 * 1024);
    let mut operation = run_intent("reserved-result");
    journal
        .retain_entry(&JournalEntry::Operation {
            operation: operation.clone(),
        })
        .unwrap();
    let path = journal.path_for_entry(&operation.entry_id).unwrap();
    let usage = journal.current_usage().unwrap().1;
    let reservation = run_result_reservation(&JournalEntry::Operation {
        operation: operation.clone(),
    })
    .unwrap();
    journal
        .save_workspace_state(&"r".repeat((journal.max_bytes - usage - reservation - 2) as usize))
        .unwrap();
    assert!(journal.retain(&notification("no-room")).is_err());
    operation.outcome = Some(Ok(serde_json::json!({
        "entry_id":operation.entry_id, "operation_id":operation.fence.operation_id,
        "exit_code":1, "stdout":"x".repeat(MAX_CI_LOG_BYTES), "stderr":"failure last line",
        "duration_ms":7, "timed_out":false, "stdout_truncated":false, "stderr_truncated":false
    })));
    let result = journal
        .finish_operation(&operation)
        .unwrap()
        .outcome
        .unwrap()
        .unwrap();
    assert_eq!(result["exit_code"], 1);
    assert_eq!(result["entry_id"], operation.entry_id);
    assert_eq!(result["stdout_truncated"], true);
    assert!(journal.current_usage().unwrap().1 <= journal.max_bytes);
    assert_eq!(
        journal
            .operation("reserved-result")
            .unwrap()
            .unwrap()
            .outcome
            .unwrap()
            .unwrap(),
        result
    );
    // A budget below even the marker retains only metadata and flags.
    let mut receipt = JournalEntry::Operation { operation };
    fit_run_receipt(&mut receipt, 0).unwrap();
    let JournalEntry::Operation { operation } = receipt else {
        unreachable!()
    };
    assert_eq!(operation.outcome.unwrap().unwrap()["stdout"], "");
    assert!(path.exists());
}

#[test]
fn usage_tracks_rename_and_removal_even_when_directory_sync_fails() {
    let dir = tempfile::tempdir().unwrap();
    let journal = DaemonJournal::new(dir.path());
    journal.initialize().unwrap();
    journal
        .fail_next_directory_sync
        .store(true, Ordering::Relaxed);
    assert!(journal
        .retain(&notification("renamed"))
        .unwrap_err()
        .to_string()
        .contains("sync failure"));
    assert_eq!(journal.current_usage().unwrap().0, 1);
    let path = journal.path_for_entry("renamed").unwrap();
    assert_eq!(
        journal.current_usage().unwrap().1,
        fs::metadata(&path).unwrap().len()
    );
    journal
        .fail_next_directory_sync
        .store(true, Ordering::Relaxed);
    assert!(journal
        .acknowledge(&JournalAckParams {
            entry_id: "renamed".into()
        })
        .is_err());
    assert!(!path.exists());
    assert_eq!(journal.current_usage().unwrap(), (0, 0));
    journal.retain(&notification("next")).unwrap();
    assert_eq!(journal.current_usage().unwrap().0, 1);
}

#[test]
fn terminal_output_redaction_preserves_accounting_identity() {
    let dir = tempfile::tempdir().unwrap();
    let journal = DaemonJournal::new(dir.path());
    let mut report = notification("report-1");
    report.summary = Some("printed 1".into());
    report.error = Some("error 1".into());
    report.plan_text = Some("- [ ] secret 1".into());
    report
        .outbox_entries
        .push(api_types::ExecutionOutboxEntry::Worklog {
            position: "1".into(),
            kind: api_types::ExecutionOutboxWorklogKind::Progress,
            summary: "1".into(),
        });
    sanitize_terminal_report(
        &mut report,
        &std::collections::BTreeMap::from([("CI".into(), "1".into())]),
    );
    journal.retain(&report).unwrap();
    let JournalEntry::Terminal { report: stored } = journal.pending().unwrap().remove(0) else {
        unreachable!()
    };
    assert_eq!(stored.terminal_report_id, "report-1");
    assert_eq!(stored.execution_id, "execution-1");
    assert_eq!(stored.summary.as_deref(), Some("printed [REDACTED]"));
    assert_eq!(stored.error.as_deref(), Some("error [REDACTED]"));
    assert_eq!(stored.plan_text.as_deref(), Some("- [ ] secret [REDACTED]"));
    let api_types::ExecutionOutboxEntry::Worklog {
        position, summary, ..
    } = &stored.outbox_entries[0]
    else {
        unreachable!()
    };
    assert_eq!(position, "1");
    assert_eq!(summary, "[REDACTED]");
}

#[test]
fn old_terminal_files_are_converted_without_data_loss() {
    let dir = tempfile::tempdir().unwrap();
    let old = dir.path().join(OLD_TERMINAL_DIRECTORY);
    fs::create_dir_all(&old).unwrap();
    let report = notification("old-report");
    fs::write(old.join("old.json"), serde_json::to_vec(&report).unwrap()).unwrap();
    let store = DaemonJournal::new(dir.path());
    store.initialize().unwrap();
    store.initialize().unwrap();
    assert!(!old.exists());
    assert_eq!(
        store.pending().unwrap()[0].replay_notification().unwrap().1,
        serde_json::to_value(report).unwrap()
    );
}
