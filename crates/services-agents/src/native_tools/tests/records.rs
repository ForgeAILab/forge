//! Tests for native Task plans, worklogs, evidence capture, and outbox ingestion.

use super::*;

#[tokio::test]
async fn native_work_condition_projection_uses_all_seven_kinds_without_legacy_aliases() {
    let fixture = native_plan_fixture("coder", "coder").await;
    let e = db::ConditionEvidence::default();
    let reason = db::ParkReason::Held {
        actor: "user".into(),
    };
    for c in [
        db::TaskCondition::Clear {
            evidence: e.clone(),
        },
        db::TaskCondition::Entering {
            state: "review".into(),
            epoch: 1,
            step_id: "step".into(),
            phase: "checks".into(),
            since: "now".into(),
            evidence: e.clone(),
        },
        db::TaskCondition::Running {
            execution_id: fixture.execution_id.clone(),
            role: "coder".into(),
            epoch: 1,
            since: "now".into(),
            evidence: e.clone(),
        },
        db::TaskCondition::Deferred {
            until: None,
            reason: db::RetryCause::Legacy,
            resume: db::ConditionContinuation::Reconcile,
            evidence: e.clone(),
        },
        db::TaskCondition::Parked {
            primary: reason.clone(),
            additional: vec![],
            resume: db::ConditionContinuation::Reconcile,
            since: None,
            evidence: e.clone(),
        },
        db::TaskCondition::Failed {
            failure: reason,
            additional: vec![],
            resume: db::ConditionContinuation::Reconcile,
            since: None,
            evidence: e.clone(),
        },
        db::TaskCondition::Settled {
            outcome: db::TerminalOutcome::Completed,
            evidence: e,
        },
    ] {
        sqlx::query("UPDATE task SET condition_json=?,error_annotation='legacy poison',blocked_json='legacy poison',failed_json='legacy poison' WHERE id=?").bind(serde_json::to_string(&c).unwrap()).bind(&fixture.task_id).execute(fixture.db.pool()).await.unwrap();
        let value = fixture
            .provider
            .read_work(&fixture.agent_id, &fixture.scope, 10)
            .await
            .unwrap();
        let item = &value["items"][0];
        assert_eq!(item["condition"], serde_json::to_value(c.public()).unwrap());
        for removed in ["blocked", "error", "failed", "error_annotation"] {
            assert!(item.get(removed).is_none(), "{removed}");
        }
    }
}
/// The native lead boundary itself: the registry's principal rule and the
/// `lead_run` adapter refuse a running Task session that leads no root.
/// The core lead-operation tests reach `TaskService` through a test
/// double, so this is where the native path is held.
#[tokio::test]
async fn a_run_that_leads_no_root_is_refused_every_lead_operation_natively() {
    let fixture = native_plan_fixture("planner", "planner").await;
    for (operation, payload) in [
        ("lead.read", json!({})),
        (
            "lead.subtask",
            json!({"action": "create", "title": "smuggled"}),
        ),
        (
            "lead.recover",
            json!({"task_id": fixture.task_id, "action": {"verb": "cancel"}, "version": 1}),
        ),
        (
            "lead.handoff",
            json!({"task_id": fixture.task_id, "note": "note"}),
        ),
        ("lead.request_input", json!({"question": "may I?"})),
    ] {
        let denied = ForgeToolProvider::propose(
                &fixture.provider,
                &fixture.agent_id,
                &fixture.scope,
                &fixture.runtime_session_id,
                operation,
                json!({"operation": operation, "payload": payload, "dedupe_key": operation, "correlation_id": operation}),
            )
            .await
            .expect_err("a run that leads no coordination root is refused");
        let denied = format!("{denied:?}");
        assert!(
            denied.contains("OperationNotInScope") || denied.contains("operation_not_in_scope"),
            "{operation}: {denied}"
        );
    }
    let children: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task WHERE parent_task_id = ?")
        .bind(&fixture.task_id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    assert_eq!(children, 0);
}

/// The verify hook's evidence store as production wires it: TaskService's
/// `ExecutionSupport` is the embedded agent service, which stores through
/// this provider. Core verify tests use a storage double instead.
#[tokio::test]
async fn verify_evidence_capture_through_execution_support_is_bounded_and_idempotent() {
    use services::task_service::execution::ExecutionSupport;
    let fixture = native_plan_fixture("coder", "coder").await;
    let media = tempfile::tempdir().unwrap();
    let embedded =
        crate::EmbeddedAgentService::new(Arc::clone(&fixture.db), b"verify-evidence-test-key");
    embedded.set_media_root(media.path().to_path_buf());
    let media_id = db::new_uuid_v4();
    for _ in 0..2 {
        let stored = embedded
            .capture_verify_evidence(
                &fixture.task_id,
                &media_id,
                "screen.png",
                "screenshot",
                "first screen",
                b"png bytes",
            )
            .await
            .expect("verify evidence stores");
        assert_eq!(stored, media_id);
    }
    let rows = db::TaskMediaRepo::list_active_media_for_task(&*fixture.db, &fixture.task_id)
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "a replayed capture resolves the same artifact"
    );
    let row = &rows[0];
    assert_eq!(row.content_type, "image/png");
    assert_eq!(row.author_type, db::CommentAuthorType::System);
    assert_eq!(row.author_name, "Forge Verify");
    assert_eq!(
        row.storage_key,
        format!("{}/{media_id}__screen.png", fixture.task_id)
    );
    assert_eq!(
        std::fs::read(media.path().join(&row.storage_key)).unwrap(),
        b"png bytes"
    );
    let asset = db::SharedMediaRepo::get_media_asset_for_task_media(&*fixture.db, &media_id)
        .await
        .unwrap()
        .expect("captured artifact has a media asset");
    assert_eq!(
        asset.checksum.as_deref(),
        Some(hex::encode(Sha256::digest(b"png bytes")).as_str())
    );
    for bytes in [
        Vec::new(),
        vec![0_u8; MAX_CAPTURED_EVIDENCE_BYTES as usize + 1],
    ] {
        embedded
            .capture_verify_evidence(
                &fixture.task_id,
                &db::new_uuid_v4(),
                "out.log",
                "log",
                "bounded",
                &bytes,
            )
            .await
            .expect_err("verify evidence is 1 byte to 25 MiB");
    }
}

#[tokio::test]
async fn native_task_registered_records_bind_exact_execution_and_refuse_forgery() {
    let fixture = native_plan_fixture_with_permissions(
        "coder",
        "coder",
        r#"["read_task","read_memory","task_read","task_write"]"#,
    )
    .await;
    // A resumed execution carries its parent's session id from creation,
    // and the Task session may have been recreated since (server restart,
    // profile change). The run is still this session's: it is found by
    // Task, identity, role and running status, never by that stale id.
    sqlx::query("UPDATE execution SET agent_session_id='parent-runtime-session' WHERE id=?")
        .bind(&fixture.execution_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    fixture
        .provider
        .set_media_root(fixture._temp.path().join("media"));
    for (operation, payload) in [
        (
            TASK_PLAN_OPERATION,
            json!({"action":"write","content":"- [ ] implement"}),
        ),
        (
            TASK_WORKLOG_OPERATION,
            json!({"action":"append","kind":"validation","summary":"passed"}),
        ),
        (
            TASK_EVIDENCE_OPERATION,
            json!({"action":"capture","kind":"custom","caption":"output","content":"OK"}),
        ),
    ] {
        let arguments = json!({"operation":operation,"payload":payload,"dedupe_key":operation,"correlation_id":operation});
        let result = ForgeToolProvider::propose(
            &fixture.provider,
            &fixture.agent_id,
            &fixture.scope,
            &fixture.runtime_session_id,
            operation,
            arguments.clone(),
        )
        .await
        .unwrap();
        assert_eq!(result["status"], "succeeded", "{result}");
        let mut forged = arguments.clone();
        forged["payload"]["execution_id"] = json!("forged");
        assert!(ForgeToolProvider::propose(
            &fixture.provider,
            &fixture.agent_id,
            &fixture.scope,
            &fixture.runtime_session_id,
            operation,
            forged
        )
        .await
        .is_err());
        assert!(ForgeToolProvider::propose(
            &fixture.provider,
            &fixture.agent_id,
            &fixture.scope,
            "other-session",
            operation,
            arguments
        )
        .await
        .is_err());
    }
    let comments: Vec<(Option<String>, Option<String>)> =
        sqlx::query_as("SELECT execution_id, role FROM task_comment WHERE task_id=?")
            .bind(&fixture.task_id)
            .fetch_all(fixture.db.pool())
            .await
            .unwrap();
    assert_eq!(comments.len(), 1);
    assert_eq!(
        comments[0].0.as_deref(),
        Some(fixture.execution_id.as_str())
    );
    assert_eq!(comments[0].1.as_deref(), Some("coder"));
    let worklog = |payload: Value, key: &'static str| {
        ForgeToolProvider::propose(
            &fixture.provider,
            &fixture.agent_id,
            &fixture.scope,
            &fixture.runtime_session_id,
            TASK_WORKLOG_OPERATION,
            json!({"operation":TASK_WORKLOG_OPERATION,"payload":payload,"dedupe_key":key,"correlation_id":key}),
        )
    };
    let count = || async {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_comment WHERE task_id=?")
            .bind(&fixture.task_id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap()
    };
    let replay = worklog(
        json!({"action":"append","kind":"validation","summary":"passed"}),
        TASK_WORKLOG_OPERATION,
    )
    .await
    .unwrap();
    assert_eq!(replay["status"], "succeeded");
    assert_eq!(count().await, 1);
    // The same key with a different entry is refused, not reported as
    // the first entry: nothing is written and the first is unchanged.
    let conflict = worklog(
        json!({"action":"append","kind":"validation","summary":"failed"}),
        TASK_WORKLOG_OPERATION,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{conflict:?}").contains("new correlation_id"),
        "{conflict:?}"
    );
    assert_eq!(count().await, 1);
    // The shapes runs sent before the operations were registered: no
    // `action` (each operation has exactly one), and an explicit null.
    for (payload, key) in [
        (
            json!({"kind":"progress","summary":"slice landed"}),
            "no-action",
        ),
        (
            json!({"action":null,"kind":"decision","summary":"chose B"}),
            "null-action",
        ),
    ] {
        let result = worklog(payload, key).await.unwrap();
        assert_eq!(result["status"], "succeeded", "{result}");
    }
    assert_eq!(count().await, 3);
    let evidence = ForgeToolProvider::propose(
            &fixture.provider,
            &fixture.agent_id,
            &fixture.scope,
            &fixture.runtime_session_id,
            TASK_EVIDENCE_OPERATION,
            json!({"operation":TASK_EVIDENCE_OPERATION,"payload":{"kind":"log","caption":"output","content":"OK","path":null,"filename":null},"dedupe_key":"evidence-no-action","correlation_id":"evidence-no-action"}),
        )
        .await
        .unwrap();
    assert_eq!(evidence["status"], "succeeded", "{evidence}");
    // A second running execution of the same role leaves no single run
    // to attribute a record to: the record is refused, the read is not.
    sqlx::query(
        "INSERT INTO execution (id, task_id, agent_id, role, status, created_at, updated_at)
             SELECT 'second-running', task_id, agent_id, role, status, created_at, updated_at
             FROM execution WHERE id = ?",
    )
    .bind(&fixture.execution_id)
    .execute(fixture.db.pool())
    .await
    .unwrap();
    assert!(worklog(
        json!({"kind":"progress","summary":"ambiguous"}),
        "ambiguous"
    )
    .await
    .is_err());
    assert_eq!(count().await, 3);
    let summary = ForgeToolProvider::read(
        &fixture.provider,
        &fixture.agent_id,
        &fixture.scope,
        "task.summary",
        json!({}),
    )
    .await;
    assert!(summary.is_ok(), "{summary:?}");
}

fn assert_correctable_plan_validation(error: AgentHostError) {
    match error {
        AgentHostError::StructuredOutcome(outcome) => {
            assert_eq!(outcome.code, OutcomeCode::ValidationError);
            assert_eq!(outcome.status, OutcomeStatus::Failed);
            assert_eq!(
                outcome.retry.as_ref().map(|retry| retry.action),
                Some(RetryAction::CorrectInput)
            );
            assert!(outcome.safe_message.contains("Markdown checklist"));
        }
        other => panic!("invalid plan input must be correctable, got {other:?}"),
    }
}

#[tokio::test]
async fn native_task_plan_writes_only_the_owning_execution_outbox() {
    let fixture = native_plan_fixture("planner", "planner").await;
    let result = fixture
        .provider
        .execute_task_plan_write(
            &fixture.agent_id,
            &fixture.runtime_session_id,
            &fixture.scope,
            &serde_json::from_value(
                json!({"action": "write", "content": "# Plan\n\n- [ ] implement it\n"}),
            )
            .unwrap(),
        )
        .await
        .expect("owning planner session writes a plan candidate");

    assert_eq!(result["task_id"], fixture.task_id);
    assert_eq!(result["execution_id"], fixture.execution_id);
    assert_eq!(result["checklist_items"], 1);
    let outbox = executors::execution_outbox_path(&fixture.worktree, &fixture.execution_id)
        .expect("outbox path");
    assert_eq!(
        std::fs::read_to_string(outbox.join("plan.md")).expect("outbox plan reads"),
        "# Plan\n\n- [ ] implement it\n"
    );
    assert!(
        !fixture
            .worktree
            .parent()
            .expect("Task directory")
            .join("plan.md")
            .exists(),
        "the native tool must not write the canonical Task plan"
    );
}

#[tokio::test]
async fn native_task_plan_denies_a_reviewer_session() {
    let fixture = native_plan_fixture("reviewer", "reviewer").await;
    let error = fixture
        .provider
        .execute_task_plan_write(
            &fixture.agent_id,
            &fixture.runtime_session_id,
            &fixture.scope,
            &serde_json::from_value(
                json!({"action": "write", "content": "- [ ] must not publish\n"}),
            )
            .unwrap(),
        )
        .await
        .expect_err("reviewer session has no plan-write authority");

    assert!(matches!(error, AgentHostError::Authority(_)));
    let outbox = executors::execution_outbox_path(&fixture.worktree, &fixture.execution_id)
        .expect("outbox path");
    assert!(!outbox.exists());
}

#[tokio::test]
async fn native_task_plan_fails_closed_for_duplicate_running_executions() {
    let fixture = native_plan_fixture("planner", "planner").await;
    let now = db::now_rfc3339();
    // Reproduce a legacy/corrupt duplicate directly: current admission
    // prevents creating two live rows for one workspace, while this tool
    // still has to fail closed if an upgraded database already has them.
    sqlx::query(
        "INSERT INTO execution (
                 id, task_id, agent_id, role, status, agent_session_id,
                 workspace_id, created_at, updated_at
             ) VALUES (?, ?, ?, 'planner', 'running', ?, ?, ?, ?)",
    )
    .bind(db::new_uuid_v4())
    .bind(&fixture.task_id)
    .bind(&fixture.agent_id)
    .bind(&fixture.runtime_session_id)
    .bind(&fixture.workspace_id)
    .bind(&now)
    .bind(&now)
    .execute(fixture.db.pool())
    .await
    .expect("duplicate running execution creates");

    let error = fixture
        .provider
        .execute_task_plan_write(
            &fixture.agent_id,
            &fixture.runtime_session_id,
            &fixture.scope,
            &serde_json::from_value(json!({"action": "write", "content": "- [ ] ambiguous\n"}))
                .unwrap(),
        )
        .await
        .expect_err("ambiguous execution authority fails closed");
    assert!(matches!(error, AgentHostError::Authority(_)));
    let outbox = executors::execution_outbox_path(&fixture.worktree, &fixture.execution_id)
        .expect("outbox path");
    assert!(!outbox.exists());
}

#[tokio::test]
async fn invalid_native_task_plan_preserves_the_previous_candidate() {
    let fixture = native_plan_fixture("planner", "planner").await;
    let original = "# Plan\n\n- [ ] keep this candidate\n";
    fixture
        .provider
        .execute_task_plan_write(
            &fixture.agent_id,
            &fixture.runtime_session_id,
            &fixture.scope,
            &serde_json::from_value(json!({"action": "write", "content": original})).unwrap(),
        )
        .await
        .expect("initial plan candidate writes");
    let outbox = executors::execution_outbox_path(&fixture.worktree, &fixture.execution_id)
        .expect("outbox path");
    let plan_path = outbox.join("plan.md");
    let before = std::fs::read(&plan_path).expect("initial candidate reads");

    let error = fixture
        .provider
        .execute_task_plan_write(
            &fixture.agent_id,
            &fixture.runtime_session_id,
            &fixture.scope,
            &serde_json::from_value(
                json!({"action": "write", "content": "# Plan without a checklist\n"}),
            )
            .unwrap(),
        )
        .await
        .expect_err("a checklist-free plan is invalid");

    assert_correctable_plan_validation(error);
    assert_eq!(
        std::fs::read(plan_path).expect("preserved candidate reads"),
        before,
        "validation must happen before replacement"
    );
}

#[tokio::test]
async fn retained_invalid_plan_outbox_replays_worklog_and_evidence_exactly_once() {
    let fixture = native_plan_fixture("planner", "planner").await;
    let media_root = fixture._temp.path().join("media");
    fixture.provider.set_media_root(media_root.clone());
    let outbox = executors::execution_outbox_path(&fixture.worktree, &fixture.execution_id)
        .expect("outbox path");
    std::fs::create_dir_all(&outbox).expect("outbox creates");
    std::fs::write(outbox.join("plan.md"), "# Plan without checklist\n")
        .expect("invalid plan writes");
    std::fs::write(
        outbox.join(executors::OUTBOX_WORKLOG_FILE),
        "{\"kind\":\"validation\",\"summary\":\"focused tests passed\"}\n",
    )
    .expect("worklog writes");
    std::fs::write(
        outbox.join(executors::OUTBOX_EVIDENCE_FILE),
        concat!(
            r#"{"kind":"log","caption":"focused tests passed","content":"OK"}"#,
            "\n",
        ),
    )
    .expect("evidence writes");

    // Reproduce a stop after the media commit but before the caption
    // comment. The next ingestion must resolve this durable identity.
    let media_id = outbox_evidence_media_id(&fixture.execution_id, "1");
    fixture
        .provider
        .store_task_evidence_with_id(
            EvidenceAuthor::Agent(&fixture.agent_id),
            &fixture.task_id,
            "log.txt",
            "text/plain".to_owned(),
            b"OK",
            media_id.clone(),
        )
        .await
        .expect("interrupted media commit creates");

    let worktree_path = fixture.worktree.to_string_lossy().into_owned();
    let input = ExecutionOutboxInput {
        task_id: &fixture.task_id,
        execution_id: &fixture.execution_id,
        agent_id: &fixture.agent_id,
        role: Some("planner"),
        worktree_path: &worktree_path,
    };
    let first = fixture.provider.ingest_execution_outbox(&input).await;
    assert_eq!(first.worklog_entries, 1, "{first:?}");
    assert_eq!(first.evidence_items, 1, "{first:?}");
    assert!(first.plan_rejected, "{first:?}");
    assert_eq!(first.rejected.len(), 1, "{first:?}");
    assert!(first.rejected[0].starts_with("plan.md:"));
    assert!(outbox.exists(), "the rejected plan retains its outbox");

    let replay = fixture.provider.ingest_execution_outbox(&input).await;
    assert_eq!(replay.worklog_entries, 1, "{replay:?}");
    assert_eq!(replay.evidence_items, 1, "{replay:?}");
    assert!(replay.plan_rejected, "{replay:?}");
    assert_eq!(replay.rejected, first.rejected);
    assert!(outbox.exists(), "the rejected plan remains diagnosable");

    let media = db::TaskMediaRepo::list_active_media_for_task(&*fixture.db, &fixture.task_id)
        .await
        .expect("media lists");
    assert_eq!(media.len(), 1, "replay must not create another media row");
    assert_eq!(media[0].id, media_id);
    assert!(
        media_root.join(&media[0].storage_key).is_file(),
        "the single media row keeps one stored artifact"
    );
    let asset_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM media_asset WHERE legacy_task_media_id = ?")
            .bind(&media_id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("asset count");
    assert_eq!(asset_count, 1, "replay must not create another media asset");

    let comments = db::TaskCommentRepo::list_comments(
        &*fixture.db,
        &fixture.task_id,
        db::PageRequest {
            cursor: None,
            limit: 10,
            include_total: false,
            sort_by: db::SortBy::CreatedAt,
            sort_order: db::SortOrder::Asc,
        },
    )
    .await
    .expect("comments list")
    .items;
    assert_eq!(comments.len(), 2, "replay must not append another entry");
    let worklog_key = format!("outbox:{}:worklog:1", fixture.execution_id);
    assert_eq!(
        comments[0].idempotency_key.as_deref(),
        Some(worklog_key.as_str())
    );
    let receipt_key = format!("outbox:{}:evidence:1", fixture.execution_id);
    assert_eq!(
        comments[1].idempotency_key.as_deref(),
        Some(receipt_key.as_str())
    );
}

#[tokio::test]
async fn execution_outbox_normalizes_unknown_evidence_kinds() {
    let fixture = native_plan_fixture("worker", "worker").await;
    fixture
        .provider
        .set_media_root(fixture._temp.path().join("media"));
    let outbox = executors::prepare_execution_outbox(&fixture.worktree, &fixture.execution_id)
        .expect("outbox creates");
    std::fs::write(fixture.worktree.join("benchmark.log"), "120 ops/s\n")
        .expect("benchmark writes");
    std::fs::write(
        outbox.join(executors::OUTBOX_EVIDENCE_FILE),
        concat!(
            r#"{"kind":"test","caption":"focused tests","content":"20 tests OK"}"#,
            "\n",
            r#"{"kind":"benchmark","caption":"throughput","path":"benchmark.log"}"#,
            "\n",
            r#"{"kind":"log","caption":"standard kind","content":"OK"}"#,
            "\n",
            r#"{"caption":"missing kind","content":"OK"}"#,
            "\n",
            r#"{"kind":7,"caption":"invalid kind","content":"OK"}"#,
            "\n",
        ),
    )
    .expect("evidence writes");
    let worktree_path = fixture.worktree.to_string_lossy().into_owned();
    let report = fixture
        .provider
        .ingest_execution_outbox(&ExecutionOutboxInput {
            task_id: &fixture.task_id,
            execution_id: &fixture.execution_id,
            agent_id: &fixture.agent_id,
            role: Some("worker"),
            worktree_path: &worktree_path,
        })
        .await;

    assert_eq!(report.evidence_items, 3, "{report:?}");
    assert_eq!(report.rejected.len(), 2, "{report:?}");
    assert!(report
        .rejected
        .iter()
        .all(|reason| reason.contains("kind must be a non-empty string")));
    let comments: Vec<String> =
        sqlx::query_scalar("SELECT content FROM task_comment WHERE task_id = ?")
            .bind(&fixture.task_id)
            .fetch_all(fixture.db.pool())
            .await
            .expect("comments list");
    assert!(
        comments.contains(&"Captured other evidence `other.txt`: [test] focused tests".to_owned())
    );
    assert!(comments
        .contains(&"Captured other evidence `benchmark.log`: [benchmark] throughput".to_owned()));
    assert!(comments.contains(&"Captured log evidence `log.txt`: standard kind".to_owned()));
    let media = db::TaskMediaRepo::list_active_media_for_task(&*fixture.db, &fixture.task_id)
        .await
        .expect("media lists");
    assert_eq!(media.len(), 3);
}

#[tokio::test]
async fn native_task_evidence_preserves_unknown_kind_in_stored_filename() {
    let fixture = native_plan_fixture("worker", "worker").await;
    fixture
        .provider
        .set_media_root(fixture._temp.path().join("media"));
    let result = fixture
            .provider
            .execute_task_evidence_capture(
                &fixture.agent_id,
                &fixture.runtime_session_id,
                &fixture.scope,
                &serde_json::from_value(
                    json!({"action":"capture", "kind": "test", "caption": "focused tests", "content": "OK"}),
                )
                .unwrap(),
            )
            .await
            .expect("unknown kind captures");

    assert_eq!(result["kind"], "other");
    assert_eq!(result["caption"], "[test] focused tests");
    let media = db::TaskMediaRepo::list_active_media_for_task(&*fixture.db, &fixture.task_id)
        .await
        .expect("media lists");
    assert_eq!(media.len(), 1);
    assert_eq!(media[0].display_filename, "[test] other.txt");
}

#[tokio::test]
async fn execution_outbox_ingests_pretty_printed_and_concatenated_objects() {
    let fixture = native_plan_fixture("worker", "worker").await;
    fixture
        .provider
        .set_media_root(fixture._temp.path().join("media"));
    let outbox = executors::prepare_execution_outbox(&fixture.worktree, &fixture.execution_id)
        .expect("outbox creates");
    std::fs::write(outbox.join("plan.md"), "# Missing checklist\n").expect("invalid plan writes");
    std::fs::write(
        outbox.join(executors::OUTBOX_WORKLOG_FILE),
        format!(
            "{}\n\n{}{}\n",
            serde_json::to_string_pretty(&json!({"kind": "progress", "summary": "approach"}))
                .expect("pretty worklog serializes"),
            json!({"kind": "decision", "summary": "implementation"}),
            json!({"kind": "validation", "summary": "verification"}),
        ),
    )
    .expect("worklog writes");
    std::fs::write(
        outbox.join(executors::OUTBOX_EVIDENCE_FILE),
        format!(
            "{}\n{}{}\n",
            serde_json::to_string_pretty(
                &json!({"kind": "log", "caption": "first", "content": "one"})
            )
            .expect("pretty evidence serializes"),
            json!({"kind": "log", "caption": "second", "content": "two"}),
            json!({"kind": "log", "caption": "third", "content": "three"}),
        ),
    )
    .expect("evidence writes");
    let worktree_path = fixture.worktree.to_string_lossy().into_owned();
    let input = ExecutionOutboxInput {
        task_id: &fixture.task_id,
        execution_id: &fixture.execution_id,
        agent_id: &fixture.agent_id,
        role: Some("worker"),
        worktree_path: &worktree_path,
    };
    for _ in 0..2 {
        let report = fixture.provider.ingest_execution_outbox(&input).await;
        assert_eq!(report.worklog_entries, 3, "{report:?}");
        assert_eq!(report.evidence_items, 3, "{report:?}");
        assert_eq!(report.rejected.len(), 1, "{report:?}");
        assert!(report.rejected[0].starts_with("plan.md:"));
    }
    let comments: Vec<(String, String)> =
        sqlx::query_as("SELECT idempotency_key, content FROM task_comment WHERE task_id = ?")
            .bind(&fixture.task_id)
            .fetch_all(fixture.db.pool())
            .await
            .expect("comments list");
    assert_eq!(
        comments.len(),
        6,
        "each streamed object has a distinct receipt"
    );
    for summary in ["approach", "implementation", "verification"] {
        assert!(comments.iter().any(|(_, content)| content == summary));
    }
    assert!(comments
        .iter()
        .any(|(key, _)| key == &format!("outbox:{}:worklog:1", fixture.execution_id)));
    let media = db::TaskMediaRepo::list_active_media_for_task(&*fixture.db, &fixture.task_id)
        .await
        .expect("media lists");
    assert_eq!(
        media.len(),
        3,
        "replay does not duplicate concatenated artifacts"
    );
}

#[tokio::test]
async fn concurrent_outbox_worklog_and_evidence_replays_are_already_ingested() {
    let fixture = native_plan_fixture("worker", "worker").await;
    fixture
        .provider
        .set_media_root(fixture._temp.path().join("media"));
    let outbox = executors::prepare_execution_outbox(&fixture.worktree, &fixture.execution_id)
        .expect("outbox creates");
    let worktree_path = fixture.worktree.to_string_lossy().into_owned();
    let input = ExecutionOutboxInput {
        task_id: &fixture.task_id,
        execution_id: &fixture.execution_id,
        agent_id: &fixture.agent_id,
        role: Some("worker"),
        worktree_path: &worktree_path,
    };
    let key = format!("outbox:{}:worklog:1", fixture.execution_id);
    let (first, replay) = tokio::join!(
        fixture
            .provider
            .append_outbox_worklog(&input, "worker", "validation", "OK", key.clone()),
        fixture
            .provider
            .append_outbox_worklog(&input, "worker", "validation", "OK", key),
    );
    first.expect("first append succeeds");
    replay.expect("concurrent replay succeeds");
    let evidence = json!({"kind": "log", "caption": "focused tests", "content": "OK"});
    let (first, replay) = tokio::join!(
        fixture
            .provider
            .ingest_local_outbox_evidence(&input, "worker", &outbox, "1", &evidence),
        fixture
            .provider
            .ingest_local_outbox_evidence(&input, "worker", &outbox, "1", &evidence),
    );
    first.expect("first evidence capture succeeds");
    replay.expect("concurrent evidence replay succeeds");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_comment WHERE task_id = ?")
        .bind(&fixture.task_id)
        .fetch_one(fixture.db.pool())
        .await
        .expect("comments count");
    assert_eq!(count, 2, "one worklog entry and one evidence caption");
    let media = db::TaskMediaRepo::list_active_media_for_task(&*fixture.db, &fixture.task_id)
        .await
        .expect("media lists");
    assert_eq!(media.len(), 1);
}

#[tokio::test]
async fn execution_outbox_ingests_valid_entries_and_reports_the_rest() {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(db::SqliteDb::new(pool));
    let now = db::now_rfc3339();
    let project = db::ProjectRepo::create(
        &*db,
        db::CreateProject {
            id: db::new_uuid_v4(),
            name: "outbox".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    let task = db::TaskRepo::create(
        &*db,
        db::CreateTask {
            id: db::new_uuid_v4(),
            project_id: project.id.clone(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "outbox".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_owned(),
            is_automation: false,
            priority: 0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates");
    let now = db::now_rfc3339();
    db::ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: "exec-1".to_owned(),
            task_id: task.id.clone(),
            agent_id: None,
            role: "reviewer".to_owned(),
            status: db::ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates");

    let temp = tempfile::tempdir().expect("temp dir");
    let worktree = temp.path().join(&task.id).join("repo");
    std::fs::create_dir_all(&worktree).expect("worktree");
    std::fs::write(worktree.join("run.log"), "20 tests OK\n").expect("artifact");
    let outbox = executors::execution_outbox_path(&worktree, "exec-1").expect("outbox path");
    std::fs::create_dir_all(&outbox).expect("outbox");
    std::fs::write(outbox.join("plan.md"), "- [ ] reviewer must not publish\n")
        .expect("reviewer plan");
    std::fs::write(outbox.join("shot.png"), [0x89, b'P', b'N', b'G']).expect("shot");
    std::fs::write(
        outbox.join(executors::OUTBOX_WORKLOG_FILE),
        concat!(
            r#"{"kind":"validation","summary":"unittest: 20 OK"}"#,
            "\n\n",
            r#"{"kind":"chatter","summary":"not a kind"}"#,
            "\nnot json\n",
            r#"{"kind":"progress","summary":"valid entry after malformed content"}"#,
            "\n",
        ),
    )
    .expect("worklog");
    std::fs::write(
        outbox.join(executors::OUTBOX_EVIDENCE_FILE),
        format!(
            "{}\n{}\n{}\n{}\n",
            r#"{"kind":"log","caption":"inline","content":"OK"}"#,
            r#"{"kind":"log","caption":"worktree file","path":"run.log"}"#,
            serde_json::json!({
                "kind": "screenshot",
                "caption": "saved in the outbox",
                "path": outbox.join("shot.png"),
            }),
            r#"{"kind":"log","caption":"escape","path":"/etc/hosts"}"#,
        ),
    )
    .expect("evidence");

    let provider = CoordinationToolProvider::new(Arc::clone(&db));
    provider.set_media_root(temp.path().join("media"));
    let report = provider
        .ingest_execution_outbox(&ExecutionOutboxInput {
            task_id: &task.id,
            execution_id: "exec-1",
            agent_id: "agent-1",
            role: Some("reviewer"),
            worktree_path: &worktree.to_string_lossy(),
        })
        .await;

    assert_eq!(report.worklog_entries, 2, "{report:?}");
    assert_eq!(report.evidence_items, 3, "{report:?}");
    assert_eq!(report.rejected.len(), 3, "{report:?}");
    assert!(report.rejected[0].starts_with("worklog.jsonl:3: kind must be"));
    assert!(report.rejected[1].starts_with("worklog.jsonl:4: "));
    assert!(report.rejected[2].contains("evidence.jsonl:4: an absolute evidence path"));
    assert!(!outbox.exists(), "the outbox is consumed");
    assert!(
        !worktree
            .parent()
            .expect("task dir")
            .join("plan.md")
            .exists(),
        "a reviewer cannot publish a plan"
    );

    let comments = db::TaskCommentRepo::list_comments(
        &*db,
        &task.id,
        db::PageRequest {
            cursor: None,
            limit: 50,
            include_total: false,
            sort_by: db::SortBy::CreatedAt,
            sort_order: db::SortOrder::Asc,
        },
    )
    .await
    .expect("comments list")
    .items;
    assert_eq!(comments.len(), 5);
    assert!(comments.iter().all(|comment| {
        comment.execution_id.as_deref() == Some("exec-1")
            && comment.role.as_deref() == Some("reviewer")
            && comment.author_id.as_deref() == Some("agent-1")
    }));
    assert_eq!(comments[0].worklog_kind.as_deref(), Some("validation"));
    assert_eq!(comments[0].content, "unittest: 20 OK");
    assert!(comments.iter().any(|comment| {
        comment.content == "valid entry after malformed content"
            && comment.worklog_kind.as_deref() == Some("progress")
    }));
    assert!(comments
        .iter()
        .any(|comment| comment.content.contains("`shot.png`: saved in the outbox")));

    // A second completion pass finds nothing left to ingest.
    let replay = provider
        .ingest_execution_outbox(&ExecutionOutboxInput {
            task_id: &task.id,
            execution_id: "exec-1",
            agent_id: "agent-1",
            role: Some("reviewer"),
            worktree_path: &worktree.to_string_lossy(),
        })
        .await;
    assert_eq!(replay, ExecutionOutboxReport::default());

    let planner_outbox =
        executors::execution_outbox_path(&worktree, "planner-exec").expect("planner outbox");
    std::fs::create_dir_all(&planner_outbox).expect("planner outbox creates");
    std::fs::write(planner_outbox.join("plan.md"), "- [ ] implement\n").expect("planner plan");
    let planner_report = provider
        .ingest_execution_outbox(&ExecutionOutboxInput {
            task_id: &task.id,
            execution_id: "planner-exec",
            agent_id: "agent-1",
            role: Some("planner"),
            worktree_path: &worktree.to_string_lossy(),
        })
        .await;
    assert!(planner_report.plan_candidate, "{planner_report:?}");
    assert!(
        !planner_outbox.exists(),
        "the agent-writable outbox is removed after staging"
    );
    assert!(
        crate::plan_artifact::publish_staged_execution_plan(&worktree, "planner-exec")
            .expect("planner plan publishes")
    );
    crate::plan_artifact::discard_staged_execution_plan(&worktree, "planner-exec")
        .expect("settled planner stage removes");
    assert_eq!(
        std::fs::read_to_string(worktree.parent().expect("task dir").join("plan.md"))
            .expect("canonical plan reads"),
        "- [ ] implement\n"
    );

    let coder_outbox =
        executors::execution_outbox_path(&worktree, "coder-exec").expect("coder outbox");
    std::fs::create_dir_all(&coder_outbox).expect("coder outbox creates");
    std::fs::write(coder_outbox.join("plan.md"), "- [x] implement\n").expect("coder plan");
    let coder_report = provider
        .ingest_execution_outbox(&ExecutionOutboxInput {
            task_id: &task.id,
            execution_id: "coder-exec",
            agent_id: "agent-1",
            role: Some("coder"),
            worktree_path: &worktree.to_string_lossy(),
        })
        .await;
    assert!(coder_report.plan_candidate, "{coder_report:?}");
    assert!(
        crate::plan_artifact::publish_staged_execution_plan(&worktree, "coder-exec")
            .expect("coder plan publishes")
    );
    assert_eq!(
        std::fs::read_to_string(worktree.parent().expect("task dir").join("plan.md"))
            .expect("updated plan reads"),
        "- [x] implement\n"
    );
}
#[test]
fn outbox_entry_reader_reports_malformed_and_non_object_content() {
    let temp = tempfile::tempdir().expect("temp dir");
    std::fs::write(
        temp.path().join(executors::OUTBOX_WORKLOG_FILE),
        concat!(
            "not json\n",
            "{\n\"kind\":\"progress\",\n\"summary\":\"valid\"\n}\n",
            "[]\n",
            "{\"kind\":\"validation\",\"summary\":",
        ),
    )
    .expect("worklog writes");
    let mut report = ExecutionOutboxReport::default();
    let entries = read_outbox_entries(temp.path(), executors::OUTBOX_WORKLOG_FILE, &mut report);
    assert!(
        report.rejected.is_empty(),
        "entry errors stay in file order"
    );
    assert_eq!(entries.len(), 4);
    assert_eq!(entries[0].0, "1");
    assert!(entries[0].1.is_err());
    assert_eq!(entries[1].0, "2");
    assert_eq!(
        entries[1].1.as_ref().expect("pretty object parses")["summary"],
        "valid"
    );
    assert_eq!(entries[2].0, "6");
    assert_eq!(
        entries[2].1.as_ref().expect_err("array is not an entry"),
        "entry must be a JSON object"
    );
    assert_eq!(entries[3].0, "7");
    assert!(entries[3]
        .1
        .as_ref()
        .expect_err("truncated object is malformed")
        .contains("EOF"));
}

#[test]
fn outbox_entry_reader_limits_objects_rather_than_pretty_printed_lines() {
    let temp = tempfile::tempdir().expect("temp dir");
    let entry = json!({
        "kind": "progress",
        "summary": "large pretty object",
        "details": vec![0; MAX_OUTBOX_ENTRIES + 1],
    });
    let pretty = serde_json::to_string_pretty(&entry).expect("pretty object serializes");
    assert!(pretty.lines().count() > MAX_OUTBOX_ENTRIES);
    let path = temp.path().join(executors::OUTBOX_WORKLOG_FILE);
    std::fs::write(&path, &pretty).expect("pretty object writes");
    let mut report = ExecutionOutboxReport::default();
    let entries = read_outbox_entries(temp.path(), executors::OUTBOX_WORKLOG_FILE, &mut report);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1.as_ref().expect("pretty object parses"), &entry);
    assert!(report.rejected.is_empty());

    std::fs::write(&path, "{}".repeat(MAX_OUTBOX_ENTRIES + 1)).expect("object stream writes");
    let entries = read_outbox_entries(temp.path(), executors::OUTBOX_WORKLOG_FILE, &mut report);
    assert_eq!(entries.len(), MAX_OUTBOX_ENTRIES);
    assert_eq!(report.rejected.len(), 1);
    assert!(report.rejected[0].contains("only the first 200 entries"));
    let positions: BTreeSet<_> = entries.iter().map(|(position, _)| position).collect();
    assert_eq!(positions.len(), MAX_OUTBOX_ENTRIES);
}

#[cfg(unix)]
#[test]
fn outbox_entry_reader_rejects_symlinks_and_hard_links() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().expect("temp dir");
    let outside = tempfile::tempdir().expect("outside dir");
    let outbox = temp.path().join("outbox");
    fs::create_dir(&outbox).expect("outbox creates");
    let outside_log = outside.path().join("worklog.jsonl");
    fs::write(
        &outside_log,
        "{\"kind\":\"progress\",\"summary\":\"secret\"}\n",
    )
    .expect("outside log writes");

    symlink(&outside_log, outbox.join(executors::OUTBOX_WORKLOG_FILE))
        .expect("log symlink creates");
    let mut symlink_report = ExecutionOutboxReport::default();
    assert!(
        read_outbox_entries(&outbox, executors::OUTBOX_WORKLOG_FILE, &mut symlink_report)
            .is_empty()
    );
    assert!(symlink_report
        .rejected
        .iter()
        .any(|reason| reason.contains("not a regular file")));

    fs::remove_file(outbox.join(executors::OUTBOX_WORKLOG_FILE)).expect("log symlink removes");
    fs::hard_link(&outside_log, outbox.join(executors::OUTBOX_WORKLOG_FILE))
        .expect("log hard link creates");
    let mut hard_link_report = ExecutionOutboxReport::default();
    assert!(read_outbox_entries(
        &outbox,
        executors::OUTBOX_WORKLOG_FILE,
        &mut hard_link_report
    )
    .is_empty());
    assert!(hard_link_report
        .rejected
        .iter()
        .any(|reason| reason.contains("hard link")));
}

#[cfg(unix)]
#[test]
fn workspace_artifact_reader_rejects_leaf_symlinks() {
    use std::os::unix::fs::symlink;

    let workspace = tempfile::tempdir().expect("workspace dir");
    let outside = tempfile::tempdir().expect("outside dir");
    fs::write(outside.path().join("secret"), "secret").expect("outside file writes");
    symlink(
        outside.path().join("secret"),
        workspace.path().join("capture"),
    )
    .expect("capture symlink creates");

    let error = resolve_workspace_artifact(workspace.path(), "capture")
        .expect_err("leaf symlink fails closed");

    assert_eq!(
        error,
        "captured artifact must be a regular file, not a symlink"
    );
}

#[test]
fn outbox_artifact_rejection_preserves_plain_reason() {
    let workspace = tempfile::tempdir().unwrap();
    let outbox = workspace.path().join("outbox");
    let error = resolve_outbox_artifact(workspace.path(), &outbox, "../secret").unwrap_err();
    assert_eq!(error, "captured artifact path escapes the Task workspace");
}
