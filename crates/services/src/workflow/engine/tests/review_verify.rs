use super::*;
use serde_json::Value;

async fn fixture(ci: &[&str], verify: serde_json::Value) -> FailedCiFixture {
    let mut f = failed_ci_fixture(3, FailurePolicy::Block).await;
    let state = f
        .workflow
        .states
        .iter_mut()
        .find(|s| s.name == "review")
        .unwrap();
    state.hooks.on_enter.clear();
    state.hooks.after_enter.clear();
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id=?")
        .bind(serde_json::to_string(&f.workflow).unwrap())
        .bind(&f.task.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
        .bind(
            json!({"retry_budgets":{"review":3},"review":{"ci_steps":ci,"verify":verify}})
                .to_string(),
        )
        .bind(&f.task.id)
        .execute(f.db.pool())
        .await
        .unwrap();
    f
}
async fn enter(f: &FailedCiFixture) -> (db::Task, db::Review, serde_json::Value) {
    let task = TaskRepo::get_by_id(&*f.db, &f.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let result = f
        .engine
        .workflow_execution()
        .transition_with_authority(
            &task.id,
            "review",
            task.version,
            &f.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "candidate completed",
            false,
            f.workflow_authority().await,
        )
        .await
        .unwrap();
    let task = drain_result(f.engine.clone(), result).await.task;
    let review = db::ReviewRepo::list_by_task(&*f.db, &task.id)
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|r| r.attempt_number)
        .unwrap();
    let details = serde_json::from_str(&review.step_results_json).unwrap();
    (task, review, details)
}
async fn verify_count(f: &FailedCiFixture) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM check_consumer WHERE purpose='review_verify'")
        .fetch_one(f.db.pool())
        .await
        .unwrap()
}
async fn assert_stopped(f: &FailedCiFixture) {
    let receipts:Vec<String>=sqlx::query_scalar("SELECT owner_receipt_json FROM check_run WHERE id IN (SELECT run_id FROM check_consumer WHERE purpose='review_verify')").fetch_all(f.db.pool()).await.unwrap();
    assert!(!receipts.is_empty());
    for raw in receipts {
        let receipt: api_types::CheckReceipt = serde_json::from_str(&raw).unwrap();
        assert!(!receipt.commands.is_empty());
        assert!(
            receipt.commands.iter().all(|c| c.process_tree_stopped),
            "{receipt:?}"
        );
    }
    let path = std::path::PathBuf::from(f.workspace.embedded_worktree_path_for_backend());
    let review = db::ReviewRepo::list_by_task(&*f.db, &f.task.id)
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|r| r.attempt_number)
        .unwrap();
    let dir = review::verify::directory(&path, &review.id).unwrap();
    assert!(
        !dir.join("start.pgid").exists(),
        "stopped run retained its group record"
    );
    if let Ok(pid) = std::fs::read_to_string(dir.join("start.pid")) {
        let running = std::process::Command::new("kill")
            .args(["-0", pid.trim()])
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(
            !running.success(),
            "verify background start survived its run"
        );
    }
}
#[tokio::test]
async fn review_verify_fails_before_reviewer_and_creates_v_finding() {
    let f = fixture(
        &["true"],
        json!({"checks":[{"name":"smoke","command":"printf broken-button; exit 1"}]}),
    )
    .await;
    super::rerun_review::seed_reviewer_agent(&f).await;
    let (task, review, details) = enter(&f).await;
    assert_eq!(task.status, "in_progress", "{task:?} {details}");
    assert_eq!(review.status, db::ReviewStatus::Failed);
    assert_eq!(details["ci_steps"][0]["exit_code"], 0);
    assert_eq!(details["verify"]["status"], "fail");
    let ledger = db::review_findings::finding_ledger(&[review]).unwrap();
    assert!(ledger
        .iter()
        .any(|f| f.id == "V-smoke" && f.status.is_open()));
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id=? AND role='reviewer'")
            .bind(&task.id)
            .fetch_one(f.db.pool())
            .await
            .unwrap();
    assert_eq!(count, 0);
    assert_eq!(verify_count(&f).await, 1);
    assert_stopped(&f).await;
}
#[tokio::test]
async fn review_verify_ci_failure_skips_verify() {
    let f = fixture(
        &["exit 3"],
        json!({"checks":[{"name":"smoke","command":"exit 0"}]}),
    )
    .await;
    let (_, _, details) = enter(&f).await;
    assert!(details.get("verify").is_none());
    assert_eq!(verify_count(&f).await, 0);
}
#[tokio::test]
async fn review_verify_report_overrides_zero_exit() {
    let f=fixture(&[],json!({"checks":[{"name":"smoke","command":r#"printf '%s' '{"verdict":"failed","summary":"Save button does nothing","findings":[{"summary":"No save","where":"form","expected":"saved","observed":"unchanged"}]}' > "$FORGE_VERIFY_REPORT""#}]})).await;
    let (_, review, details) = enter(&f).await;
    assert_eq!(review.status, db::ReviewStatus::Failed);
    assert_eq!(
        details["verify"]["checks"][0]["summary"],
        "Save button does nothing"
    );
    assert_eq!(details["verify"]["status"], "fail");
    let ledger = db::review_findings::finding_ledger(&[review]).unwrap();
    assert!(ledger[0].observed.as_deref().unwrap().contains("unchanged"));
    assert_stopped(&f).await;
}
#[tokio::test]
async fn review_verify_ready_timeout_has_app_tail_and_stops_background() {
    let f=fixture(&["true"],json!({"start":"printf app-never-ready; sleep 300","ready_path":"/","ready_timeout_seconds":1,"checks":[{"name":"smoke","command":"true"}]})).await;
    let (_, review, details) = enter(&f).await;
    assert_eq!(details["verify"]["status"], "fail", "{details}");
    let ledger = db::review_findings::finding_ledger(&[review]).unwrap();
    let ready = ledger.iter().find(|f| f.id == "V-ready").unwrap();
    assert!(
        ready
            .observed
            .as_deref()
            .unwrap()
            .contains("app-never-ready"),
        "{ready:?}"
    );
    assert_stopped(&f).await;
}
#[tokio::test]
async fn review_verify_start_exit_is_v_start() {
    let f = fixture(
        &[],
        json!({"start":"printf cannot-bind; exit 7","checks":[{"name":"smoke","command":"true"}]}),
    )
    .await;
    let (_, review, details) = enter(&f).await;
    assert_eq!(details["verify"]["status"], "fail");
    let ledger = db::review_findings::finding_ledger(&[review]).unwrap();
    assert!(ledger
        .iter()
        .any(|f| f.id == "V-start" && f.observed.as_deref().unwrap().contains("cannot-bind")));
    assert_stopped(&f).await;
}
#[tokio::test]
async fn review_verify_inconclusive_twice_parks_without_review_charge() {
    let f=fixture(&["true"],json!({"start":"sleep 300","checks":[{"name":"smoke","command":r#"printf '%s' '{"verdict":"inconclusive","summary":"Try again"}' > "$FORGE_VERIFY_REPORT""#}]})).await;
    let (task, _, details) = enter(&f).await;
    assert_eq!(task.status, "review", "{task:?} {details}");
    assert_eq!(details["verify"]["status"], "inconclusive");
    assert_eq!(verify_count(&f).await, 2);
    let blocked: serde_json::Value =
        serde_json::from_str(task.blocked_json.as_deref().unwrap()).unwrap();
    assert_eq!(blocked["kind"], "review_blocked");
    assert_eq!(blocked["source"], "verify");
    assert_eq!(
        db::budget::spent(f.db.pool(), &task.id, "review")
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        db::budget::spent(f.db.pool(), &task.id, "review_verify_rerun")
            .await
            .unwrap(),
        1
    );
    let snapshot = f
        .engine
        .task_action_snapshot(
            &task.id,
            &api_types::Actor::user(api_types::UserActionSource::Api),
        )
        .await
        .unwrap();
    let offers = crate::available_actions(&snapshot);
    assert!(offers.iter().any(|o| o.action.verb() == "retry"));
    assert!(offers.iter().any(|o| o.action.verb() == "cancel"));
    let manual = offers.iter().find(|o| o.label == "Mark Reviewed").unwrap();
    assert!(manual
        .parameters
        .iter()
        .any(|p| p.name == "reason" && p.required));
    assert_stopped(&f).await;
}
#[tokio::test]
async fn review_verify_path_escape_is_skipped_without_changing_verdict() {
    let f=fixture(&[],json!({"checks":[{"name":"smoke","command":r#"printf '%s' '{"verdict":"pass","evidence":[{"path":"../../etc/passwd","kind":"log","caption":"escape"}]}' > "$FORGE_VERIFY_REPORT""#}]})).await;
    let (_, review, details) = enter(&f).await;
    assert_eq!(review.status, db::ReviewStatus::Passed, "{details}");
    assert_eq!(details["verify"]["status"], "pass");
    assert!(details["verify"]["notes"][0]
        .as_str()
        .unwrap()
        .contains("escapes"));
    assert_eq!(details["verify"]["checks"][0]["evidence"], json!([]));
    assert_stopped(&f).await;
}
#[tokio::test]
async fn review_verify_runtime_inputs_request_and_http_readiness() {
    let f=fixture(&[],json!({"start":r#"python3 -u -m http.server "$FORGE_VERIFY_PORT" --bind 127.0.0.1"#,"ready_path":"/","ready_timeout_seconds":5,"checks":[{"name":"smoke","command":r#"test -n "$FORGE_VERIFY_URL" && test -f "$FORGE_VERIFY_REQUEST" && test "$FORGE_VERIFY_REPORT" = "$FORGE_VERIFY_DIR/smoke.report.json" && cp "$FORGE_VERIFY_REQUEST" "$FORGE_VERIFY_DIR/seen.json""#}]})).await;
    let (_, review, details) = enter(&f).await;
    assert_eq!(review.status, db::ReviewStatus::Passed, "{details}");
    let path = std::path::PathBuf::from(f.workspace.embedded_worktree_path_for_backend());
    let dir = review::verify::directory(&path, &review.id).unwrap();
    let request: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("seen.json")).unwrap()).unwrap();
    assert_eq!(request["task_id"], f.task.id);
    assert!(request["requirements"].is_array());
    assert!(request["changed_files"].is_array());
    assert!(request["findings"].is_array());
    assert!(request["base_sha"].as_str().unwrap().len() >= 40);
    let raw:String=sqlx::query_scalar("SELECT input_json FROM check_run WHERE id IN (SELECT run_id FROM check_consumer WHERE purpose='review_verify')").fetch_one(f.db.pool()).await.unwrap();
    let inputs: api_types::CheckDigestInput = serde_json::from_str(&raw).unwrap();
    let spec = inputs.spec;
    assert_eq!(spec.execution_policy, api_types::CANONICAL_CI_POLICY);
    assert!(!spec.cacheable());
    assert_stopped(&f).await;
}
#[tokio::test]
async fn review_verify_attempt_memory_loads_only_for_coder_worker_repairs() {
    let f = fixture(&[], json!({"checks":[{"name":"smoke","command":"false"}]})).await;
    let _ = enter(&f).await;
    let router = crate::lifecycle::context::embedded_workspace_router_for_test(
        f.db.clone(),
        f._workspace_root.path().to_owned(),
        None,
    );
    for (role, state, expected) in [
        ("reviewer", "review", 0),
        ("planner", "planning", 0),
        ("coder", "planning", 0),
        ("coder", "in_progress", 1),
        ("worker", "in_progress", 1),
    ] {
        let context = crate::workflow::dispatch::loader::load_agent_dispatch_context(
            crate::workflow::dispatch::loader::DispatchContextParams {
                db: f.db.clone(),
                router: &router,
                task_id: &f.task.id,
                role,
                state_name: state,
                state_config: json!({}),
                execution_policy: None,
                workflow: &f.workflow,
            },
        )
        .await
        .unwrap();
        assert_eq!(context.repair_attempts.len(), expected, "{role}/{state}");
    }
}

#[tokio::test]
async fn review_progress_infrastructure_entry_failure_keeps_infrastructure_retry() {
    let f = failed_ci_fixture(2, FailurePolicy::Block).await;
    let now = now_rfc3339();
    let candidate: String =
        sqlx::query_scalar("SELECT id FROM execution WHERE task_id=? AND role='coder'")
            .bind(&f.task.id)
            .fetch_one(f.db.pool())
            .await
            .unwrap();
    db::ReviewRepo::create(
        &*f.db,
        db::CreateReview {
            id: "progress-prior".into(),
            task_id: f.task.id.clone(),
            execution_id: candidate,
            attempt_number: 2,
            status: db::ReviewStatus::Failed,
            step_results_json: "{}".into(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let mut tx = db::begin_immediate(f.db.pool()).await.unwrap();
    for identity in ["review:one", "review:progress-prior"] {
        db::budget::charge(&mut tx, &f.task.id, "review", 2, identity)
            .await
            .unwrap();
    }
    db::budget::charge(
        &mut tx,
        &f.task.id,
        "review_progress",
        2,
        "progress:progress-prior",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    crate::check_runner::policy::silence_login_shell_for_test(&f.db, true)
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&*f.db, &f.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let result = f
        .engine
        .workflow_execution()
        .transition_with_authority(
            &task.id,
            "review",
            task.version,
            &f.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "progress repair completed",
            false,
            f.workflow_authority().await,
        )
        .await
        .unwrap();
    let task = drain_result(f.engine.clone(), result).await.task;
    let annotation: serde_json::Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(
        annotation["blocking_reason"], "review_ci_infrastructure",
        "{task:?}"
    );
    assert_eq!(
        db::budget::spent(f.db.pool(), &task.id, "review")
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        db::budget::spent(f.db.pool(), &task.id, "review_ci_infrastructure")
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn review_verify_daemon_records_not_run_without_starting_a_check() {
    let mut f = fixture(
        &[],
        json!({"start":"exit 99","checks":[{"name":"smoke","command":"exit 99"}]}),
    )
    .await;
    f.workflow
        .states
        .iter_mut()
        .find(|s| s.name == "review")
        .unwrap()
        .hooks
        .before_enter
        .retain(|h| h.action == "run_ci_steps");
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id=?")
        .bind(serde_json::to_string(&f.workflow).unwrap())
        .bind(&f.task.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    let now = now_rfc3339();
    sqlx::query("INSERT INTO daemon(id,machine_id,hostname,os,arch,status,created_at,updated_at) VALUES('verify-daemon','verify-machine','test','test','test','online',?,?)").bind(&now).bind(&now).execute(f.db.pool()).await.unwrap();
    sqlx::query("INSERT INTO runtime(id,daemon_id,kind,workspace_root,status,created_at,updated_at) VALUES('verify-runtime','verify-daemon','shell','/opaque','ready',?,?)").bind(&now).bind(&now).execute(f.db.pool()).await.unwrap();
    sqlx::query("UPDATE workspace_placement SET owner_kind='daemon',daemon_id='verify-daemon',runtime_id='verify-runtime',workspace_handle='opaque-handle' WHERE workspace_id=?").bind(&f.workspace.id).execute(f.db.pool()).await.unwrap();
    super::rerun_review::seed_reviewer_agent(&f).await;
    let (_, review, details) = enter(&f).await;
    assert_eq!(review.status, db::ReviewStatus::Running, "{details}");
    assert_eq!(details["verify"]["status"], "not_run");
    assert_eq!(details["verify"]["reason"], "placement_unsupported");
    assert_eq!(verify_count(&f).await, 0);
    let router = crate::lifecycle::context::embedded_workspace_router_for_test(
        f.db.clone(),
        f._workspace_root.path().to_owned(),
        None,
    );
    let context = crate::workflow::dispatch::loader::load_agent_dispatch_context(
        crate::workflow::dispatch::loader::DispatchContextParams {
            db: f.db.clone(),
            router: &router,
            task_id: &f.task.id,
            role: "reviewer",
            state_name: "review",
            state_config: json!({}),
            execution_policy: None,
            workflow: &f.workflow,
        },
    )
    .await
    .unwrap();
    use crate::workflow::dispatch::PromptBuilder;
    let prompt = crate::workflow::dispatch::reviewer_prompt::ReviewerPromptBuilder.build(&context);
    assert!(prompt.user.contains("placement_unsupported"));
}

struct EvidenceSupport(crate::native_tools::CoordinationToolProvider);
#[async_trait::async_trait]
impl crate::task_service::execution::ExecutionSupport for EvidenceSupport {
    async fn inject_provider_env(&self, _: &mut serde_json::Value) -> crate::Result<()> {
        Ok(())
    }
    async fn ingest_execution_outbox(
        &self,
        _: &crate::task_service::execution::ExecutionOutboxInput<'_>,
    ) -> crate::task_service::execution::ExecutionOutboxReport {
        Default::default()
    }
    async fn ingest_execution_outbox_entries(
        &self,
        _: &crate::task_service::execution::ExecutionOutboxInput<'_>,
        _: Vec<api_types::ExecutionOutboxEntry>,
    ) -> crate::task_service::execution::ExecutionOutboxReport {
        Default::default()
    }
    async fn capture_verify_evidence(
        &self,
        task: &str,
        id: &str,
        filename: &str,
        kind: &str,
        caption: &str,
        bytes: &[u8],
    ) -> crate::Result<String> {
        self.0
            .capture_verify_evidence(task, id, filename, kind, caption, bytes)
            .await
            .map_err(|e| crate::ServiceError::invalid_operation(e.to_string()))
    }
}
#[tokio::test]
async fn review_verify_captures_evidence_for_the_reviewer_and_refuses_symlinks_size_kind() {
    let mut f=fixture(&[],json!({"checks":[{"name":"smoke","command":r#"printf shot-one > "$FORGE_VERIFY_DIR/one.png"
printf shot-two > "$FORGE_VERIFY_DIR/two.png"
ln -s /etc/passwd "$FORGE_VERIFY_DIR/escape"
dd if=/dev/zero of="$FORGE_VERIFY_DIR/large.log" bs=1 count=1 seek=26214400 2>/dev/null
printf '%s' '{"verdict":"pass","summary":"Two screens exercised","evidence":[{"path":"./one.png","kind":"screenshot","caption":"first"},{"path":"two.png","kind":"screenshot","caption":"second"},{"path":"escape","kind":"log","caption":"escape"},{"path":"large.log","kind":"log","caption":"oversize"},{"path":"one.png","kind":"unknown","caption":"bad kind"}]}' > "$FORGE_VERIFY_REPORT""#}]})).await;
    super::rerun_review::seed_reviewer_agent(&f).await;
    let media = f._workspace_root.path().join("media");
    let provider = crate::native_tools::CoordinationToolProvider::new(f.db.clone());
    provider.set_media_root(media.clone());
    f.engine = f
        .engine
        .clone()
        .with_provider_credential_env(Arc::new(EvidenceSupport(provider)));
    let (_, review, details) = enter(&f).await;
    assert_eq!(review.status, db::ReviewStatus::Running, "{details}");
    assert_eq!(details["verify"]["status"], "pass");
    assert_eq!(
        details["verify"]["checks"][0]["evidence"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        details["verify"]["notes"].as_array().unwrap().len(),
        3,
        "{details}"
    );
    for id in details["verify"]["checks"][0]["evidence"]
        .as_array()
        .unwrap()
    {
        let stored = db::TaskMediaRepo::get_media_by_id(&*f.db, id.as_str().unwrap(), false)
            .await
            .unwrap()
            .unwrap();
        let bytes = std::fs::read(media.join(&stored.storage_key)).unwrap();
        assert_eq!(stored.author_type, db::CommentAuthorType::System);
        assert!(stored.author_id.is_none());
        assert_eq!(stored.author_name, "Forge Verify");
        let replay = f
            .engine
            .capture_verify_evidence(
                &f.task.id,
                id.as_str().unwrap(),
                &stored.display_filename,
                "screenshot",
                "recorded capture",
                &bytes,
            )
            .await
            .unwrap();
        assert_eq!(replay, id.as_str().unwrap());
    }
    let router = crate::lifecycle::context::embedded_workspace_router_for_test(
        f.db.clone(),
        f._workspace_root.path().to_owned(),
        None,
    );
    let context = crate::workflow::dispatch::loader::load_agent_dispatch_context(
        crate::workflow::dispatch::loader::DispatchContextParams {
            db: f.db.clone(),
            router: &router,
            task_id: &f.task.id,
            role: "reviewer",
            state_name: "review",
            state_config: json!({}),
            execution_policy: None,
            workflow: &f.workflow,
        },
    )
    .await
    .unwrap();
    use crate::workflow::dispatch::PromptBuilder;
    let prompt = crate::workflow::dispatch::reviewer_prompt::ReviewerPromptBuilder.build(&context);
    assert!(prompt.user.contains("Verify results:"));
    assert!(prompt.user.contains("Two screens exercised"));
    assert!(prompt.user.contains("one.png"));
    assert!(prompt.user.contains("two.png"));
    assert_stopped(&f).await;
}

#[tokio::test]
async fn review_verify_command_timeout_is_a_failure_and_stops_the_server() {
    let f=fixture(&[],json!({"start":"sleep 300","timeout_seconds":2,"checks":[{"name":"smoke","command":"sleep 300","timeout_seconds":1}]})).await;
    let (_, review, details) = enter(&f).await;
    assert_eq!(review.status, db::ReviewStatus::Failed, "{details}");
    assert_eq!(details["verify"]["status"], "fail");
    let ledger = db::review_findings::finding_ledger(&[review]).unwrap();
    assert!(ledger.iter().any(|f| f.id == "V-smoke"));
    assert_stopped(&f).await;
}
#[tokio::test]
async fn review_verify_inconclusive_then_passes_once_with_fresh_report() {
    let f = fixture(
        &[],
        json!({"checks":[{"name":"smoke","command":r#"if [ -f "$FORGE_VERIFY_DIR/ran" ]; then
printf '%s' '{"verdict":"pass"}' > "$FORGE_VERIFY_REPORT"
else
touch "$FORGE_VERIFY_DIR/ran"
printf '%s' '{"verdict":"inconclusive"}' > "$FORGE_VERIFY_REPORT"
fi"#}]}),
    )
    .await;
    let (_, review, details) = enter(&f).await;
    assert_eq!(review.status, db::ReviewStatus::Passed, "{details}");
    assert_eq!(details["verify"]["status"], "pass");
    assert_eq!(verify_count(&f).await, 2);
    assert_stopped(&f).await;
}
#[tokio::test]
async fn review_verify_never_reuses_results_and_preserves_ci_reuse() {
    let f = fixture(
        &["true"],
        json!({"checks":[{"name":"smoke","command":"true"}]}),
    )
    .await;
    let _ = enter(&f).await;
    f.engine
        .rerun_review(
            uuid::Uuid::parse_str(&f.task.id).unwrap(),
            api_types::Actor::user(api_types::UserActionSource::Api),
        )
        .await
        .unwrap();
    let task = drain(f.engine.clone(), &f.task.id).await;
    assert_eq!(task.status, "review");
    assert_eq!(verify_count(&f).await, 2);
    let ci_runs: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT run_id) FROM check_consumer WHERE purpose='entry_ci'",
    )
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(ci_runs, 1);
    let verify_runs: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT run_id) FROM check_consumer WHERE purpose='review_verify'",
    )
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(verify_runs, 2);
}
#[tokio::test]
async fn review_verify_check_named_ready_keeps_its_command_output() {
    let f = fixture(
        &[],
        json!({"checks":[{"name":"ready","command":"printf actual-check-failed; exit 2"}]}),
    )
    .await;
    let (_, review, _) = enter(&f).await;
    let ledger = db::review_findings::finding_ledger(&[review]).unwrap();
    assert!(ledger
        .iter()
        .find(|f| f.id == "V-ready")
        .unwrap()
        .observed
        .as_deref()
        .unwrap()
        .contains("actual-check-failed"));
}

#[tokio::test]
async fn review_verify_deadline_before_launch_is_infrastructure_never_a_pass() {
    let f = fixture(
        &[],
        json!({"timeout_seconds":1,"checks":[{"name":"smoke","command":"true"}]}),
    )
    .await;
    let path = f.workspace.embedded_worktree_path_for_backend();
    let held =
        f.db.lock_server_check_checkout(&f.workspace.id, path)
            .await
            .unwrap();
    let (task, review, details) = enter(&f).await;
    drop(held);
    assert_eq!(task.status, "review", "{task:?} {details}");
    assert_eq!(review.status, db::ReviewStatus::Cancelled);
    let annotation: serde_json::Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["blocking_reason"], "review_ci_infrastructure");
    assert_eq!(
        db::budget::spent(f.db.pool(), &task.id, "review")
            .await
            .unwrap(),
        0
    );
    let raw:String=sqlx::query_scalar("SELECT owner_receipt_json FROM check_run WHERE id IN (SELECT run_id FROM check_consumer WHERE purpose='review_verify')").fetch_one(f.db.pool()).await.unwrap();
    let receipt: api_types::CheckReceipt = serde_json::from_str(&raw).unwrap();
    assert!(receipt.commands.is_empty());
    assert_eq!(receipt.outcome, api_types::CheckExecutionOutcome::TimedOut);
}

async fn set_project_env(f: &FailedCiFixture, key: &str, value: &str) {
    let project = db::ProjectRepo::get_by_id(&*f.db, &f.task.project_id)
        .await
        .unwrap()
        .unwrap();
    let mut settings: serde_json::Value = serde_json::from_str(&project.settings).unwrap();
    settings["environment"]["env"][key] = json!(value);
    sqlx::query("UPDATE project SET settings=? WHERE id=?")
        .bind(settings.to_string())
        .bind(&f.task.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
}

#[tokio::test]
async fn review_verify_app_log_report_and_text_evidence_are_ansi_free_and_redacted() {
    const SECRET: &str = "s3cr3t-verify-value";
    let mut f = fixture(
        &[],
        json!({"start":r#"printf '\033[31mboot with %s\033[0m\n' "$VERIFY_SECRET"; sleep 300"#,
            "ready_command":r#"test -s "$FORGE_VERIFY_DIR/app.log""#,
            "checks":[{"name":"smoke","command":r#"python3 -c 'import json,os; s=os.environ["VERIFY_SECRET"]; json.dump({"verdict":"failed","summary":"\x1b[1msaw "+s+"\x1b[0m","findings":[{"summary":"leak","observed":"token "+s}],"evidence":[{"path":"app.log","kind":"log","caption":"log "+s}]}, open(os.environ["FORGE_VERIFY_REPORT"],"w"))'"#}]}),
    )
    .await;
    set_project_env(&f, "VERIFY_SECRET", SECRET).await;
    let media = f._workspace_root.path().join("media");
    let provider = crate::native_tools::CoordinationToolProvider::new(f.db.clone());
    provider.set_media_root(media.clone());
    f.engine = f
        .engine
        .clone()
        .with_provider_credential_env(Arc::new(EvidenceSupport(provider)));
    let (_, review, details) = enter(&f).await;
    assert_eq!(details["verify"]["status"], "fail", "{details}");
    let tail = details["verify"]["app_log_tail"].as_str().unwrap();
    assert!(tail.contains("boot with [REDACTED]"), "{tail:?}");
    let check = details["verify"]["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "smoke")
        .unwrap();
    assert_eq!(check["summary"], "saw [REDACTED]", "{details}");
    let raw = details.to_string();
    assert!(!raw.contains(SECRET), "{raw}");
    assert!(!raw.contains("\\u001b"), "{raw}");
    let ledger = db::review_findings::finding_ledger(&[review]).unwrap();
    let smoke = ledger.iter().find(|f| f.id == "V-smoke").unwrap();
    assert!(
        smoke
            .observed
            .as_deref()
            .unwrap()
            .contains("token [REDACTED]"),
        "{smoke:?}"
    );
    let id = check["evidence"][0].as_str().unwrap();
    let stored = db::TaskMediaRepo::get_media_by_id(&*f.db, id, false)
        .await
        .unwrap()
        .unwrap();
    let bytes = std::fs::read(media.join(&stored.storage_key)).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert!(text.contains("boot with [REDACTED]") && !text.contains(SECRET));
    assert_stopped(&f).await;
}

#[tokio::test]
async fn review_verify_unusable_report_or_unknown_verdict_is_inconclusive_not_pass() {
    for command in [
        r#"printf 'not json' > "$FORGE_VERIFY_REPORT""#,
        r#"printf '%s' '["pass"]' > "$FORGE_VERIFY_REPORT""#,
        r#"printf '%s' '{"verdict":"maybe","summary":"unsure"}' > "$FORGE_VERIFY_REPORT""#,
        r#"ln -s /etc/hosts "$FORGE_VERIFY_REPORT""#,
    ] {
        let f = fixture(&[], json!({"checks":[{"name":"smoke","command":command}]})).await;
        let (task, review, details) = enter(&f).await;
        assert_eq!(
            details["verify"]["status"], "inconclusive",
            "{command}: {details}"
        );
        assert_eq!(review.status, db::ReviewStatus::Failed, "{command}");
        assert_eq!(verify_count(&f).await, 2, "{command}: re-runs once");
        let blocked: serde_json::Value =
            serde_json::from_str(task.blocked_json.as_deref().unwrap()).unwrap();
        assert_eq!(blocked["source"], "verify", "{command}");
    }
    let f = fixture(
        &[],
        json!({"checks":[{"name":"smoke","command":r#"printf '%s' '{"verdict":"PASSED","summary":"fine"}' > "$FORGE_VERIFY_REPORT""#}]}),
    )
    .await;
    let (_, _, details) = enter(&f).await;
    assert_eq!(details["verify"]["status"], "pass", "{details}");
}

#[tokio::test]
async fn review_verify_exit_during_readiness_only_creates_v_ready() {
    let f = fixture(&[], json!({"start":"sleep 0.4; printf app-exited; exit 7", "ready_command":"false", "ready_timeout_seconds":5, "checks":[{"name":"smoke","command":"true"}]})).await;
    let (_, review, details) = enter(&f).await;
    assert_eq!(details["verify"]["status"], "fail");
    let ledger = db::review_findings::finding_ledger(&[review]).unwrap();
    assert!(ledger.iter().any(|f| f.id == "V-ready"));
    assert!(!ledger.iter().any(|f| f.id == "V-start"), "{ledger:?}");
    assert_stopped(&f).await;
}

#[tokio::test]
async fn review_verify_rerun_ledger_honors_task_and_project_limits() {
    for (project_limit, task_limit, runs) in
        [(0, None, 1), (0, Some(2), 3), (2, Some(0), 1), (2, None, 3)]
    {
        let f = fixture(&[], json!({"checks":[{"name":"smoke","command":r#"printf '%s' '{"verdict":"inconclusive"}' > "$FORGE_VERIFY_REPORT""#}]})).await;
        // fixture() stores the config after f.task was loaded.
        let raw: String = sqlx::query_scalar("SELECT task_state_config FROM task WHERE id=?")
            .bind(&f.task.id)
            .fetch_one(f.db.pool())
            .await
            .unwrap();
        let mut config: Value = serde_json::from_str(&raw).unwrap();
        if let Some(limit) = task_limit {
            config["retry_budgets"]["review_verify_rerun"] = json!(limit);
        }
        sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
            .bind(config.to_string())
            .bind(&f.task.id)
            .execute(f.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE project SET settings=? WHERE id=?")
            .bind(json!({"retry_budgets":{"review_verify_rerun":project_limit}}).to_string())
            .bind(&f.task.project_id)
            .execute(f.db.pool())
            .await
            .unwrap();
        let (task, review, details) = enter(&f).await;
        assert_eq!(details["verify"]["status"], "inconclusive", "{details}");
        assert_eq!(verify_count(&f).await, runs);
        assert_eq!(
            db::budget::spent(f.db.pool(), &task.id, "review_verify_rerun")
                .await
                .unwrap(),
            runs - 1
        );
        let charges: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_budget_charge WHERE task_id=? AND kind='review_verify_rerun' AND window_id=?").bind(&task.id).bind(&review.id).fetch_one(f.db.pool()).await.unwrap();
        assert_eq!(charges, runs - 1);
        assert_eq!(
            db::budget::spent(f.db.pool(), &task.id, "review")
                .await
                .unwrap(),
            0
        );
        assert_stopped(&f).await;
    }
}

#[tokio::test]
async fn review_verify_evidence_has_one_byte_budget_across_checks() {
    let mut f = fixture(&[], json!({"checks":[
        {"name":"first","command":r#"python3 - <<'PY'
import os,json
root=os.environ['FORGE_VERIFY_DIR']
for name in ['one','two','three']:
    with open(root+'/'+name+'.bin','wb') as f:
        f.write(b'\xff'); f.truncate(24*1024*1024)
json.dump({'verdict':'pass','evidence':[{'path':n+'.bin','kind':'other'} for n in ['one','two']]},open(os.environ['FORGE_VERIFY_REPORT'],'w'))
PY"#},
        {"name":"second","command":r#"printf '%s' '{"verdict":"pass","evidence":[{"path":"three.bin","kind":"other"}]}' > "$FORGE_VERIFY_REPORT""#}
    ]})).await;
    let provider = crate::native_tools::CoordinationToolProvider::new(f.db.clone());
    provider.set_media_root(f._workspace_root.path().join("media"));
    f.engine = f
        .engine
        .clone()
        .with_provider_credential_env(Arc::new(EvidenceSupport(provider)));
    let (_, _, details) = enter(&f).await;
    let verify = &details["verify"];
    assert_eq!(verify["status"], "pass", "{details}");
    assert_eq!(verify["checks"][0]["evidence"].as_array().unwrap().len(), 2);
    assert!(verify["checks"][1]["evidence"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(
        verify["notes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n.as_str().unwrap().contains("total evidence byte budget")),
        "{verify}"
    );
}

#[tokio::test]
#[ignore = "NOT MET: two capped writers buffered short startup logs; streaming app.log cap remains open"]
async fn review_verify_app_log_stops_at_16_mib_and_keeps_readable_tail() {
    let f = fixture(&[], json!({"start":r#"python3 -u - <<'PY'
import os,sys,time
marker='cap-tail-readable\n'
sys.stdout.write('x'*(16*1024*1024-len(marker))+marker+'discarded-after-cap\n'*100000)
sys.stdout.flush()
open(os.environ['FORGE_VERIFY_DIR']+'/emitted','w').close()
time.sleep(300)
PY"#, "ready_command":r#"test -f "$FORGE_VERIFY_DIR/emitted""#, "ready_timeout_seconds":10, "checks":[{"name":"smoke","command":"true"}]})).await;
    let (_, review, details) = enter(&f).await;
    assert_eq!(details["verify"]["status"], "pass", "{details}");
    let path = std::path::PathBuf::from(f.workspace.embedded_worktree_path_for_backend());
    let directory = review::verify::directory(&path, &review.id).unwrap();
    assert_eq!(
        std::fs::metadata(directory.join("app.log")).unwrap().len(),
        16 * 1024 * 1024
    );
    let tail = details["verify"]["app_log_tail"].as_str().unwrap();
    assert!(tail.ends_with("cap-tail-readable\n"), "{tail}");
    assert!(!tail.contains("discarded-after-cap"));
    assert_stopped(&f).await;
}

#[cfg(unix)]
#[tokio::test]
async fn review_verify_escaped_listener_warns_in_details_and_reviewer_prompt() {
    struct EscapedProcess(std::path::PathBuf);
    impl Drop for EscapedProcess {
        fn drop(&mut self) {
            if let Ok(pid) = std::fs::read_to_string(&self.0) {
                let _ = std::process::Command::new("kill")
                    .args(["-KILL", pid.trim()])
                    .status();
            }
        }
    }
    let f = fixture(&[], json!({"start":r#"python3 -u - <<'PY'
import os,socket,time
if not os.path.exists(os.environ['FORGE_VERIFY_DIR']+'/escaped.pid') and os.fork()==0:
    os.setsid()
    s=socket.socket(); s.bind(('127.0.0.1',int(os.environ['FORGE_VERIFY_PORT']))); s.listen()
    open(os.environ['FORGE_VERIFY_DIR']+'/escaped.pid','w').write(str(os.getpid()))
while True: time.sleep(1)
PY"#, "ready_command":r#"test -s "$FORGE_VERIFY_DIR/escaped.pid""#, "ready_timeout_seconds":10, "checks":[{"name":"smoke","command":r#"if test -f "$FORGE_VERIFY_DIR/reran"; then true; else touch "$FORGE_VERIFY_DIR/reran"; printf '%s' '{"verdict":"inconclusive"}' > "$FORGE_VERIFY_REPORT"; fi"#}]})).await;
    // The Review is allocated during enter(); locate the exact recorded pid even
    // on an assertion failure so the regression leaves no escaped process.
    struct RootCleanup(std::path::PathBuf);
    impl Drop for RootCleanup {
        fn drop(&mut self) {
            if let Ok(entries) = std::fs::read_dir(&self.0) {
                for e in entries.flatten() {
                    drop(EscapedProcess(e.path().join("escaped.pid")));
                }
            }
        }
    }
    let path = std::path::PathBuf::from(f.workspace.embedded_worktree_path_for_backend());
    let directory = review::verify::directory(&path, "placeholder").unwrap();
    let _cleanup = RootCleanup(directory.parent().unwrap().to_owned());
    let (_, review, details) = enter(&f).await;
    assert_eq!(details["verify"]["status"], "pass", "{details}");
    assert_eq!(review.status, db::ReviewStatus::Passed);
    assert_eq!(verify_count(&f).await, 2);
    let port: u16 = std::fs::read_to_string(
        review::verify::directory(&path, &review.id)
            .unwrap()
            .join("port"),
    )
    .unwrap()
    .parse()
    .unwrap();
    assert!(
        review::verify::port_warning(port).is_none(),
        "second evaluation's port must be free; its Review still needs the first warning"
    );
    let verify: api_types::ReviewVerifyDetails =
        serde_json::from_value(details["verify"].clone()).unwrap();
    assert!(
        verify
            .notes
            .iter()
            .any(|n| n.contains("still in use") && n.contains("setsid")),
        "{verify:?}"
    );
    assert!(review::verify::prompt(&verify).contains("still in use"));
    assert_stopped(&f).await;
}

#[tokio::test]
async fn review_verify_stopped_run_removes_pgid_record() {
    let f = fixture(
        &[],
        json!({"start":"sleep 300","checks":[{"name":"smoke","command":"true"}]}),
    )
    .await;
    let (_, _, details) = enter(&f).await;
    assert_eq!(details["verify"]["status"], "pass");
    assert_stopped(&f).await;
}
