//! Workflow engine review infrastructure tests.

use super::*;

#[tokio::test]
async fn system_review_ci_placement_error_retries_without_review_rejection() {
    use crate::workspace_backend as ws;
    struct UnreachableOnce(std::sync::atomic::AtomicBool, Arc<dyn ws::WorkspaceBackend>);
    #[async_trait::async_trait]
    impl ws::WorkspaceBackend for UnreachableOnce {
        async fn prepare(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::PrepareSpec,
        ) -> ws::Result<ws::PreparedWorkspace> {
            unreachable!()
        }
        // Review-entry CI is asked of the check runner; the hook itself only
        // reads the checkout's HEAD from its owner, so the outage is met there.
        async fn describe(&self, p: &db::WorkspacePlacement) -> ws::Result<ws::WorkspaceState> {
            if self.0.swap(false, std::sync::atomic::Ordering::SeqCst) {
                Err(ws::WorkspaceBackendError::OwnerUnreachable {
                    daemon_id: "review-owner".into(),
                })
            } else {
                self.1.describe(p).await
            }
        }
        async fn run(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::RunSpec,
        ) -> ws::Result<ws::RunResult> {
            unreachable!()
        }
        async fn diff(&self, _: &db::WorkspacePlacement, _: &ws::DiffSpec) -> ws::Result<ws::Diff> {
            unreachable!()
        }
        async fn read(&self, _: &db::WorkspacePlacement, _: &str, _: u64) -> ws::Result<Vec<u8>> {
            unreachable!()
        }

        async fn reset(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::ResetSpec,
        ) -> ws::Result<ws::PreparedWorkspace> {
            unreachable!()
        }
        async fn cleanup(&self, _: &db::WorkspacePlacement) -> ws::Result<ws::CleanupAck> {
            unreachable!()
        }
        async fn harvest_outbox(
            &self,
            _: &db::WorkspacePlacement,
            _: &str,
        ) -> ws::Result<ws::OutboxHarvest> {
            unreachable!()
        }
        async fn consume_outbox(&self, _: &db::WorkspacePlacement, _: &str) -> ws::Result<()> {
            unreachable!()
        }
    }
    // Budget 1 would immediately block if this outage were a CI rejection.
    let mut fixture = failed_ci_fixture(1, FailurePolicy::Block).await;
    fixture
        .workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap()
        .gate_config
        .as_mut()
        .unwrap()
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&fixture.workflow).unwrap())
        .bind(&fixture.task.project_id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    // The reconnected owner answers for the real checkout, where the check
    // runner then runs a passing step.
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(json!({"retry_budgets":{"review":1},"review":{"ci_steps":["true"]}}).to_string())
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let owner = fixture
        .engine
        .workspace_backend_router
        .resolve(&fixture.db, &fixture.workspace)
        .await
        .unwrap()
        .backend;
    fixture.engine.workspace_backend_router = Arc::new(ws::WorkspaceBackendRouter::new(Arc::new(
        UnreachableOnce(std::sync::atomic::AtomicBool::new(true), owner),
    )));
    let result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            fixture.task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "worker completed",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    let result = drain_result(fixture.engine.clone(), result).await;
    assert_eq!(result.task.status, "review");
    assert!(result.review.is_none());
    let annotation: serde_json::Value =
        serde_json::from_str(result.task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["blocking_reason"], "review_ci_infrastructure");
    assert!(annotation["message"]
        .as_str()
        .unwrap()
        .contains("owner_unreachable"));
    let entries = TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(
        crate::task_diagnostics::audit_gate_rejections_since_boundary(&entries, "review"),
        0
    );
    let retried = fixture
        .engine
        .workflow_execution()
        .retry_entry_barrier_with_authority(
            &fixture.task.id,
            result.task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
            "owner reconnected",
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    let retried = drain_result(fixture.engine.clone(), retried).await;
    assert!(retried.task.error_annotation.is_none());
    assert!(retried.task.entry_barrier_json.is_none());
    assert_eq!(
        retried.review.unwrap().status,
        db::ReviewStatus::AwaitingHuman
    );
    let entries = TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(
        crate::task_diagnostics::audit_gate_rejections_since_boundary(&entries, "review"),
        0
    );
}

#[tokio::test]
async fn system_review_ci_infrastructure_retry_is_capped_and_does_not_create_attempts() {
    use crate::workspace_backend as ws;
    struct ReviewCiFault {
        db: Arc<db::SqliteDb>,
        task_id: String,
        authority_loss: bool,
    }
    impl ReviewCiFault {
        async fn outage(&self) -> ws::Result<ws::RunResult> {
            if self.authority_loss {
                sqlx::query("UPDATE task SET version = version + 1 WHERE id = ?")
                    .bind(&self.task_id)
                    .execute(self.db.pool())
                    .await
                    .unwrap();
                Ok(ws::RunResult {
                    exit_code: 0,
                    stdout_tail: String::new(),
                    stderr_tail: String::new(),
                    duration_ms: 1,
                })
            } else {
                sqlx::query("UPDATE task SET metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.concurrent_ci_note', 'retained') WHERE id = ?")
                    .bind(&self.task_id).execute(self.db.pool()).await.unwrap();
                Err(ws::WorkspaceBackendError::OwnerUnreachable {
                    daemon_id: "review-owner".into(),
                })
            }
        }
    }
    #[async_trait::async_trait]
    impl ws::WorkspaceBackend for ReviewCiFault {
        async fn prepare(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::PrepareSpec,
        ) -> ws::Result<ws::PreparedWorkspace> {
            unreachable!()
        }
        // Review-entry CI is asked of the check runner; the hook itself only
        // reads the checkout's HEAD from its owner, so the outage is met there.
        async fn describe(&self, _: &db::WorkspacePlacement) -> ws::Result<ws::WorkspaceState> {
            self.outage().await?;
            unreachable!("the injected owner never answers")
        }
        async fn run(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::RunSpec,
        ) -> ws::Result<ws::RunResult> {
            unreachable!()
        }
        async fn diff(&self, _: &db::WorkspacePlacement, _: &ws::DiffSpec) -> ws::Result<ws::Diff> {
            unreachable!()
        }
        async fn read(&self, _: &db::WorkspacePlacement, _: &str, _: u64) -> ws::Result<Vec<u8>> {
            unreachable!()
        }

        async fn reset(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::ResetSpec,
        ) -> ws::Result<ws::PreparedWorkspace> {
            unreachable!()
        }
        async fn cleanup(&self, _: &db::WorkspacePlacement) -> ws::Result<ws::CleanupAck> {
            unreachable!()
        }
        async fn harvest_outbox(
            &self,
            _: &db::WorkspacePlacement,
            _: &str,
        ) -> ws::Result<ws::OutboxHarvest> {
            unreachable!()
        }
        async fn consume_outbox(&self, _: &db::WorkspacePlacement, _: &str) -> ws::Result<()> {
            unreachable!()
        }
    }

    let mut fixture = failed_ci_fixture(1, FailurePolicy::Block).await;
    fixture.engine.workspace_backend_router =
        Arc::new(ws::WorkspaceBackendRouter::new(Arc::new(ReviewCiFault {
            db: fixture.db.clone(),
            task_id: fixture.task.id.clone(),
            authority_loss: false,
        })));
    let mut result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            fixture.task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "worker completed",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    result = drain_result(fixture.engine.clone(), result).await;
    let placement = db::WorkspacePlacementRepo::get_for_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE workspace_placement SET state = 'disconnected' WHERE id = ?")
        .bind(&placement.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    for _ in 0..6 {
        result = fixture
            .engine
            .workflow_execution()
            .retry_entry_barrier_with_authority(
                &fixture.task.id,
                result.task.version,
                &fixture.workflow,
                &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
                "same owner remains offline",
                fixture.workflow_authority().await,
            )
            .await
            .unwrap();
        result = drain_result(fixture.engine.clone(), result).await;
        assert!(
            result.task.blocked_json.is_none(),
            "an owner outage waits for max_disconnect"
        );
        let barrier: serde_json::Value =
            serde_json::from_str(result.task.entry_barrier_json.as_deref().unwrap()).unwrap();
        assert_eq!(barrier["status"], "blocked");
        assert_eq!(
            db::budget::spent(
                fixture.db.pool(),
                &fixture.task.id,
                db::budget::Kind::ReviewCiInfrastructure.key()
            )
            .await
            .unwrap(),
            1,
            "disconnected attempts do not spend the infrastructure cap"
        );
    }
    sqlx::query("UPDATE workspace_placement SET state = 'ready' WHERE id = ?")
        .bind(&placement.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    for attempt in 1..=5 {
        let barrier: serde_json::Value =
            serde_json::from_str(result.task.entry_barrier_json.as_deref().unwrap()).unwrap();
        assert_eq!(barrier["status"], "blocked");
        assert_eq!(
            db::budget::spent(
                fixture.db.pool(),
                &fixture.task.id,
                db::budget::Kind::ReviewCiInfrastructure.key()
            )
            .await
            .unwrap(),
            attempt
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM review WHERE task_id = ?")
            .bind(&fixture.task.id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "an unreachable owner never ran CI");
        if attempt == 5 {
            break;
        }
        let deferred = crate::deferred_dispatch::pending_until(&result.task).unwrap();
        let delay = chrono::DateTime::parse_from_rfc3339(&deferred.not_before)
            .unwrap()
            .with_timezone(&chrono::Utc)
            - chrono::Utc::now();
        assert!(delay.num_milliseconds() > (5 * (1_i64 << (attempt - 1)) - 1) * 1000);
        result = fixture
            .engine
            .workflow_execution()
            .retry_entry_barrier_with_authority(
                &fixture.task.id,
                result.task.version,
                &fixture.workflow,
                &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
                "retry owner",
                fixture.workflow_authority().await,
            )
            .await
            .unwrap();
        result = drain_result(fixture.engine.clone(), result).await;
    }
    let annotation: serde_json::Value =
        serde_json::from_str(result.task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(
        annotation["blocking_reason"],
        "review_ci_infrastructure_exhausted"
    );
    let blocked: api_types::InterruptionMetadata =
        serde_json::from_str(result.task.blocked_json.as_deref().unwrap()).unwrap();
    assert_eq!(
        blocked.kind,
        Some(api_types::FailureKind::BeforeWorkHookFailed)
    );
    assert!(!blocked.reason.is_empty());
    assert!(!blocked.created_at.is_empty());
    assert!(crate::deferred_dispatch::pending_until(&result.task).is_none());
    let metadata: serde_json::Value =
        serde_json::from_str(result.task.metadata_json.as_deref().unwrap()).unwrap();
    assert_eq!(metadata["concurrent_ci_note"], "retained");
    let feed =
        crate::AttentionService::new(fixture.db.clone(), crate::test_support::noop_wake_turns())
            .mission_control_home("test-user", None, 50)
            .await
            .unwrap();
    let item = feed
        .needs_attention
        .iter()
        .find(|item| item.dedupe_key == format!("review-ci:{}", fixture.task.id))
        .expect("review CI park is visible in Mission Control");
    assert_eq!(item.category, api_types::AttentionCategory::ExecutionFailed);
    assert!(item.details["cause"]
        .as_str()
        .unwrap()
        .contains("review_ci_infrastructure_exhausted"));
    let entries = TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(
        crate::task_diagnostics::audit_gate_rejections_since_boundary(&entries, "review"),
        0
    );
}

// The Review row changes while review-entry CI runs on the check runner and
// its hooks step is suspended. The woken step settles under the authority it
// asked with (carried in the suspension record), so the write is refused
// where the inline run refused it and the attempt is cancelled.
#[tokio::test]
async fn system_review_ci_authority_loss_keeps_base_cancellation_routing() {
    let fixture = failed_ci_fixture(1, FailurePolicy::Block).await;
    let gate = TempDir::new().unwrap();
    let started = gate.path().join("started");
    let release = gate.path().join("release");
    let quote = |p: &std::path::Path| format!("\"{}\"", p.to_str().unwrap());
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
        .bind(
            json!({"retry_budgets":{"review":1},"review":{"ci_steps":[format!(
                "touch {}; while [ ! -f {} ]; do sleep 0.02; done",
                quote(&started),
                quote(&release)
            )]}})
            .to_string(),
        )
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            fixture.task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "worker completed",
            false,
            fixture.workflow_authority().await,
        )
        .await;
    let result = result.unwrap();
    let draining = tokio::spawn(drain_result(fixture.engine.clone(), result));
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while !started.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("review-entry CI runs on the check runner");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_step WHERE task_id=? AND kind='hooks' AND status='suspended'"
        )
        .bind(&fixture.task.id)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap(),
        1,
        "the asking step waits suspended"
    );
    let changed = sqlx::query(
        "UPDATE review SET updated_at='2099-01-01T00:00:00Z' WHERE task_id=? AND status='running'",
    )
    .bind(&fixture.task.id)
    .execute(fixture.db.pool())
    .await
    .unwrap()
    .rows_affected();
    assert_eq!(changed, 1, "the Review row changes while CI runs");
    std::fs::write(&release, "release").unwrap();
    let result = draining.await.unwrap();
    assert_eq!(result.task.status, "in_progress");
    assert!(result.task.entry_barrier_json.is_none());
    let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(!task
        .error_annotation
        .as_deref()
        .is_some_and(|raw| raw.contains("review_ci_")));
    assert!(crate::deferred_dispatch::pending_until(&task).is_none());
    let status: String = sqlx::query_scalar(
        "SELECT status FROM review WHERE task_id = ? ORDER BY attempt_number DESC LIMIT 1",
    )
    .bind(&fixture.task.id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    assert_eq!(status, "cancelled");
}

/// Audit 2.3a: with no ci_steps and no reviewer (the default workflow's
/// "unconfigured review"), one target-moved refresh round repeats
/// review -> merging inside one chain without creating a Review row, so the
/// loop detector parks a legitimate integration retry. The old depth guard
/// allowed two such rounds.
#[tokio::test]
async fn audit_23a_unconfigured_review_target_moved_round_keeps_running() {
    use db::TaskStepRepo;
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(256));
    let id = "audit-unconfigured-refresh";
    seed_project_repo_and_task(&db, id, default_states::IN_PROGRESS).await;
    let workflow = default_workflow::default_workflow();
    let driver = engine(Arc::clone(&db), Arc::clone(&bus));

    // Real producer: unconfigured review cascades review -> merging (step 1).
    let result = driver
        .workflow_execution()
        .transition(
            id,
            default_states::REVIEW,
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::General),
            "agent completed",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.pending_steps, 1);
    let merging = drain(driver.clone(), id).await;
    assert_eq!(merging.status, default_states::MERGING);
    let step1 = db.task_steps(id).await.unwrap().last().cloned().unwrap();
    assert_eq!(step1.status, "done");

    // RunMerge's TargetMoved outcome (target_moved_result) returns exactly
    // this cascade from the step-1 execution; enqueue it the way the engine
    // does, with step 1 as parent.
    let reason = "main advanced; rebased onto main, re-review required".to_owned();
    let input = driver
        .workflow_execution()
        .cascade_step_input(
            &merging,
            &workflow,
            default_states::MERGE_FAILED.into(),
            reason,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::TargetMovedRebase),
            false,
            false,
            None,
            Some(&step1),
            "audit:target-moved".into(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(input.chain_id, step1.chain_id);
    db.enqueue_step(&input).await.unwrap();
    db.ready_step(&input.id).await.unwrap();

    // Real producers from here: merge_failed bridge -> review, then the
    // unconfigured review cascades review -> merging again.
    let settled = drain(driver.clone(), id).await;
    let steps = db.task_steps(id).await.unwrap();
    println!(
        "audit steps: {:?}",
        steps
            .iter()
            .map(|s| (
                s.expected_status.as_str(),
                s.status.as_str(),
                s.chain_position
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(settled.status, default_states::MERGING);
    assert_eq!(steps.last().unwrap().status, "done");
    assert!(settled.blocked_json.is_none());
    let snapshot = driver
        .task_action_snapshot(
            id,
            &api_types::Actor::user(api_types::UserActionSource::Test),
        )
        .await
        .unwrap();
    let offers = crate::available_actions(&snapshot);
    println!(
        "audit offers: {:?} annotation: {:?}",
        offers
            .iter()
            .map(|o| (o.reason.clone(), o.label.clone()))
            .collect::<Vec<_>>(),
        settled.error_annotation
    );
}

// A new review entry starts a new CI-infrastructure interruption episode.
#[tokio::test]
async fn review_ci_infrastructure_second_entry_gets_full_allowance() {
    use crate::workspace_backend as ws;
    struct ReviewCiFault {
        db: Arc<db::SqliteDb>,
        task_id: String,
        authority_loss: bool,
    }
    impl ReviewCiFault {
        async fn outage(&self) -> ws::Result<ws::RunResult> {
            if self.authority_loss {
                sqlx::query("UPDATE task SET version = version + 1 WHERE id = ?")
                    .bind(&self.task_id)
                    .execute(self.db.pool())
                    .await
                    .unwrap();
                Ok(ws::RunResult {
                    exit_code: 0,
                    stdout_tail: String::new(),
                    stderr_tail: String::new(),
                    duration_ms: 1,
                })
            } else {
                sqlx::query("UPDATE task SET metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.concurrent_ci_note', 'retained') WHERE id = ?")
                    .bind(&self.task_id).execute(self.db.pool()).await.unwrap();
                Err(ws::WorkspaceBackendError::OwnerUnreachable {
                    daemon_id: "review-owner".into(),
                })
            }
        }
    }
    #[async_trait::async_trait]
    impl ws::WorkspaceBackend for ReviewCiFault {
        async fn prepare(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::PrepareSpec,
        ) -> ws::Result<ws::PreparedWorkspace> {
            unreachable!()
        }
        // Review-entry CI is asked of the check runner; the hook itself only
        // reads the checkout's HEAD from its owner, so the outage is met there.
        async fn describe(&self, _: &db::WorkspacePlacement) -> ws::Result<ws::WorkspaceState> {
            self.outage().await?;
            unreachable!("the injected owner never answers")
        }
        async fn run(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::RunSpec,
        ) -> ws::Result<ws::RunResult> {
            unreachable!()
        }
        async fn diff(&self, _: &db::WorkspacePlacement, _: &ws::DiffSpec) -> ws::Result<ws::Diff> {
            unreachable!()
        }
        async fn read(&self, _: &db::WorkspacePlacement, _: &str, _: u64) -> ws::Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn reset(
            &self,
            _: &db::WorkspacePlacement,
            _: &ws::ResetSpec,
        ) -> ws::Result<ws::PreparedWorkspace> {
            unreachable!()
        }
        async fn cleanup(&self, _: &db::WorkspacePlacement) -> ws::Result<ws::CleanupAck> {
            unreachable!()
        }
        async fn harvest_outbox(
            &self,
            _: &db::WorkspacePlacement,
            _: &str,
        ) -> ws::Result<ws::OutboxHarvest> {
            unreachable!()
        }
        async fn consume_outbox(&self, _: &db::WorkspacePlacement, _: &str) -> ws::Result<()> {
            unreachable!()
        }
    }

    let mut fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    fixture.engine.workspace_backend_router =
        Arc::new(ws::WorkspaceBackendRouter::new(Arc::new(ReviewCiFault {
            db: fixture.db.clone(),
            task_id: fixture.task.id.clone(),
            authority_loss: false,
        })));
    let system = api_types::Actor::system(api_types::SystemComponent::Workflow);
    let dispatcher = api_types::Actor::system(api_types::SystemComponent::TaskDispatcher);
    // Episode 1: two connected infrastructure failures, then the Task leaves review.
    let mut result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            fixture.task.version,
            &fixture.workflow,
            &system,
            "worker completed",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    result = drain_result(fixture.engine.clone(), result).await;
    result = fixture
        .engine
        .workflow_execution()
        .retry_entry_barrier_with_authority(
            &fixture.task.id,
            result.task.version,
            &fixture.workflow,
            &dispatcher,
            "retry owner",
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    result = drain_result(fixture.engine.clone(), result).await;
    assert!(result.task.blocked_json.is_none());
    // Owner moves the Task back to work; the engine's status update drops the barrier.
    let owner = api_types::Actor::user(api_types::UserActionSource::Board).into_override();
    result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "in_progress",
            result.task.version,
            &fixture.workflow,
            &owner,
            "owner moves back",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    result = drain_result(fixture.engine.clone(), result).await;
    assert_eq!(result.task.status, "in_progress");
    sqlx::query("UPDATE task SET blocked_json = NULL, error_annotation = NULL, metadata_json = '{}' WHERE id = ?")
        .bind(&fixture.task.id).execute(fixture.db.pool()).await.unwrap();
    // Episode 2: a fresh review entry; count connected failures until the park.
    let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            task.version,
            &fixture.workflow,
            &system,
            "worker completed again",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    result = drain_result(fixture.engine.clone(), result).await;
    let mut failures = 1;
    while result.task.blocked_json.is_none() && failures < 10 {
        result = fixture
            .engine
            .workflow_execution()
            .retry_entry_barrier_with_authority(
                &fixture.task.id,
                result.task.version,
                &fixture.workflow,
                &dispatcher,
                "retry owner",
                fixture.workflow_authority().await,
            )
            .await
            .unwrap();
        result = drain_result(fixture.engine.clone(), result).await;
        failures += 1;
    }
    assert_eq!(
        failures, 5,
        "a new review entry gets the full five-failure allowance"
    );
}
#[tokio::test]
async fn review_comments_ci_only_failure_and_routing_replay_are_idempotent() {
    for budget in [1, 3] {
        let fixture = failed_ci_fixture(budget, FailurePolicy::Block).await;
        let result = fixture
            .engine
            .workflow_execution()
            .transition_with_authority(
                &fixture.task.id,
                "review",
                fixture.task.version,
                &fixture.workflow,
                &api_types::Actor::system(api_types::SystemComponent::Workflow),
                "review comment coverage",
                false,
                fixture.workflow_authority().await,
            )
            .await
            .unwrap();
        let result = drain_result(fixture.engine.clone(), result).await;
        let review = result.review.unwrap();
        fixture
            .engine
            .publish_finished_review_comment(&review, None)
            .await
            .unwrap();
        fixture
            .engine
            .publish_finished_review_comment(&review, None)
            .await
            .unwrap();
        let comments = db::TaskCommentRepo::list_comments(
            &*fixture.db,
            &fixture.task.id,
            db::PageRequest {
                cursor: None,
                limit: 100,
                sort_by: db::SortBy::CreatedAt,
                sort_order: db::SortOrder::Asc,
                include_total: false,
            },
        )
        .await
        .unwrap()
        .items;
        let results: Vec<_> = comments
            .iter()
            .filter(|comment| comment.content.starts_with("Review failed (attempt"))
            .collect();
        assert_eq!(results.len(), 1, "{comments:?}");
        assert!(results[0].content.contains("- [ ] CI-0 New:"));
        assert!(results[0]
            .content
            .contains("Full review: Review tab, attempt 1"));
        let prefix = if budget == 1 {
            "Task parked after review"
        } else {
            "Repair round"
        };
        let routing: Vec<_> = comments
            .iter()
            .filter(|comment| comment.content.starts_with(prefix))
            .collect();
        assert_eq!(routing.len(), 1, "{comments:?}");
        assert!(routing[0].content.contains("CI-0"));
    }
}
