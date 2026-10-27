//! Workflow engine review entry budgets tests.

use super::*;

#[tokio::test]
async fn system_review_ci_failure_routes_to_coder_and_spends_review_budget() {
    // Log-policy hooks used by existing Projects must settle the same verdict
    // as the current blocking entry hook. Also exercise deleted-tree recovery.
    for (policy, requires_approval) in [(FailurePolicy::Block, false), (FailurePolicy::Log, true)] {
        let mut fixture = failed_ci_fixture(3, policy).await;
        fixture
            .workflow
            .states
            .iter_mut()
            .find(|state| state.name == "review")
            .unwrap()
            .gate_config
            .as_mut()
            .unwrap()
            .requires_user_approval = Some(requires_approval);
        sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
            .bind(serde_json::to_string(&fixture.workflow).unwrap())
            .bind(&fixture.task.project_id)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        std::fs::remove_dir_all(fixture.workspace.embedded_worktree_path_for_backend()).unwrap();
        let result = fixture
            .engine
            .workflow_execution()
            .transition_with_authority(
                &fixture.task.id,
                "review",
                fixture.task.version,
                &fixture.workflow,
                &api_types::Actor::system(api_types::SystemComponent::Workflow),
                "user action",
                false,
                fixture.workflow_authority().await,
            )
            .await
            .unwrap();
        let result = drain_result(fixture.engine.clone(), result).await;
        assert_eq!(result.task.status, "in_progress");
        assert!(result.task.entry_barrier_json.is_none());
        assert!(result.task.blocked_json.is_none());
        let review = result.review.unwrap();
        assert_eq!(review.status, db::ReviewStatus::Failed);
        let details: serde_json::Value = serde_json::from_str(&review.step_results_json).unwrap();
        assert_eq!(details["ci_steps"][0]["exit_code"], 101);
        let entries = TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
            .await
            .unwrap();
        let rejection = entries
            .iter()
            .find(|entry| entry.from_state == "review" && entry.rejection)
            .unwrap();
        assert_eq!(rejection.to_state, "in_progress");
        // CI diagnostics use the stored zero-based step index. Check the
        // stable prefix, including the command, without depending on output.
        assert!(
            rejection.trigger_reason.starts_with(
                "CI step 0 failed (exit 101): `echo 'entry-ci-failed [conflict-handoff]'; exit 101`"
            ),
            "unexpected CI rejection reason: {}",
            rejection.trigger_reason,
        );
        assert_eq!(
            crate::task_diagnostics::audit_gate_rejections_since_boundary(&entries, "review"),
            1
        );
        assert_eq!(
            db::budget::spent(
                fixture.db.pool(),
                &fixture.task.id,
                db::budget::Kind::Review.key()
            )
            .await
            .unwrap(),
            1,
            "the failed CI verdict spends one Review attempt"
        );
        assert!(
            std::path::Path::new(&fixture.workspace.embedded_worktree_path_for_backend()).exists()
        );
    }
}

#[tokio::test]
async fn system_review_ci_failure_records_review_budget_exhausted_blocker() {
    let fixture = failed_ci_fixture(1, FailurePolicy::Block).await;
    let result = fixture
        .engine
        .workflow_execution()
        .transition_with_authority(
            &fixture.task.id,
            "review",
            fixture.task.version,
            &fixture.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "repair completed",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    let result = drain_result(fixture.engine.clone(), result).await;
    assert_eq!(result.task.status, "review");
    assert!(result.task.entry_barrier_json.is_none());
    assert!(result.task.blocked_json.is_some());
    let annotation: serde_json::Value =
        serde_json::from_str(result.task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "review_budget_exhausted");
    let review = result.review.unwrap();
    assert_eq!(review.status, db::ReviewStatus::Failed);
    let details: serde_json::Value = serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(details["ci_steps"][0]["exit_code"], 101);
}

#[tokio::test]
async fn review_gate_exhausted_budget_defers_blocking_until_review_failure() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let mut review = state("review", StateKind::Gate, None, StateHooks::default());
    review.gate_config = Some(api_types::GateConfig {
        reject_target: Some("coding".to_owned()),
        max_rejections: Some(1),
        approve_label: None,
        reject_label: None,
        requires_user_approval: Some(false),
        optional_when_unassigned: Some(false),
    });
    review.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "shipped".to_owned(),
            dispatch: None,
        },
    );
    review.triggers.insert(
        WorkflowTrigger::Reject,
        WorkflowTriggerDefinition {
            to: "coding".to_owned(),
            dispatch: None,
        },
    );
    review.triggers.insert(
        WorkflowTrigger::Fail,
        WorkflowTriggerDefinition {
            to: "needs_help".to_owned(),
            dispatch: None,
        },
    );
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("start", StateKind::Initial, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "coding",
            ),
            with_trigger(
                state("coding", StateKind::Active, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "review",
            ),
            review,
            state("needs_help", StateKind::Custom, None, StateHooks::default()),
            state("shipped", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    seed_custom_workflow_task(&db, &task_id, "start", &workflow).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "coding",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "start work",
            false,
            Default::default(),
        )
        .await
        .expect("start → coding");
    assert_eq!(result.task.status, "coding");

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "review",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request review",
            false,
            Default::default(),
        )
        .await
        .expect("coding → review");
    let result = drain_result(eng.clone(), result).await;
    assert_eq!(result.task.status, "review");

    let transition_logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    let payload = transition_logs
        .last()
        .and_then(|entry| entry.hook_results_json.as_deref())
        .expect("hook results are written");
    let results: Vec<HookResultEntry> =
        serde_json::from_str(payload).expect("hook results deserialize");
    assert!(results
        .iter()
        .any(|entry| { entry.action == "check_retry_budget" && entry.phase == "after_enter" }));

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            "coding",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate rejected",
            true,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateRejected),
        )
        .await
        .expect("review → coding (reject)");
    assert_eq!(result.task.status, "coding");

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    eng.workflow_execution()
        .transition(
            &task_id,
            "review",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request review again",
            false,
            Default::default(),
        )
        .await
        .expect("review budget resolves on re-entry");

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current_task.status, "review");
    assert!(
        current_task.blocked_json.is_none(),
        "review entry defers retry exhaustion enforcement until a review failure is recorded"
    );

    let transition_logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    assert!(!transition_logs
        .iter()
        .any(|entry| { entry.from_state == "review" && entry.to_state == "needs_help" }));
}

#[tokio::test]
async fn review_entry_failure_without_failed_review_bounces_max_minus_one() {
    for max in [2, 3, 4] {
        let outcomes = entry_failure_outcomes(
            max,
            None,
            api_types::Actor::system(api_types::SystemComponent::Workflow),
        )
        .await;
        let mut expected = vec!["bounce".to_owned(); (max - 1) as usize];
        expected.push("block".to_owned());
        assert_eq!(outcomes, expected, "max_rejections={max}");
    }
}

// Allowed change 1: the same entry failure honours the Task's review override
// over the gate cap (base parked on the second failure here).
#[tokio::test]
async fn review_entry_failure_without_failed_review_honors_task_override() {
    let outcomes = entry_failure_outcomes(
        2,
        Some(5),
        api_types::Actor::system(api_types::SystemComponent::Workflow),
    )
    .await;
    assert_eq!(outcomes, ["bounce", "bounce", "bounce", "bounce", "block"]);
}

// Exhausting the cancelled-review entry cap (max_rejections - 1) parks the Task.
#[tokio::test]
async fn cancelled_review_entry_cap_exhaustion_parks() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(64));
    let task_id = new_uuid_v4();
    let mut workflow =
        user_approval_review_workflow(hook("run_ci_steps", FailurePolicy::Block), Vec::new());
    workflow
        .states
        .iter_mut()
        .find(|s| s.name == "review")
        .unwrap()
        .gate_config
        .as_mut()
        .unwrap()
        .max_rejections = Some(3);
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(r#"{"review":{"ci_steps":"not-an-array"}}"#)
        .bind(&task_id)
        .execute(db.pool())
        .await
        .unwrap();
    let now = now_rfc3339();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('audit-e',?,'coder','completed',?,?)")
        .bind(&task_id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('audit-r',?,'audit-e',1,'cancelled','{}',?,?,?)")
        .bind(&task_id).bind(&now).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let actor = api_types::Actor::system(api_types::SystemComponent::Workflow);
    let mut outcomes = Vec::new();
    for _ in 0..4 {
        let task = TaskRepo::get_by_id(&*db, &task_id, false)
            .await
            .unwrap()
            .unwrap();
        if task.status != "working" {
            break;
        }
        let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
            .workflow_execution()
            .transition(
                &task_id,
                "review",
                task.version,
                &workflow,
                &actor,
                "submit",
                false,
                Default::default(),
            )
            .await
            .unwrap();
        let result = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;
        let barrier = result.task.entry_barrier_json.clone().unwrap_or_default();
        outcomes.push(if result.task.status == "working" {
            "bounce".to_owned()
        } else if barrier.contains("review retry budget exhausted") {
            "block".to_owned()
        } else {
            format!(
                "other:{}:barrier={}:blocked={:?}:annotation={:?}",
                result.task.status, barrier, result.task.blocked_json, result.task.error_annotation
            )
        });
    }
    assert_eq!(outcomes, vec!["bounce", "bounce", "block"]);
}

#[tokio::test]
async fn review_progress_configuration_entry_failure_parks_without_infrastructure_retry() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(64));
    let task_id = new_uuid_v4();
    let workflow =
        user_approval_review_workflow(hook("run_ci_steps", FailurePolicy::Block), Vec::new());
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
        .bind(
            json!({"retry_budgets":{"review":2}, "review":{"ci_steps":"not-an-array"}}).to_string(),
        )
        .bind(&task_id)
        .execute(db.pool())
        .await
        .unwrap();
    // A granted progress round returns after the ordinary budget is spent.
    let reviewed_at = now_rfc3339();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('prior-coder',?,'coder','completed',?,?)")
        .bind(&task_id).bind(&reviewed_at).bind(&reviewed_at).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('progress-review',?,'prior-coder',2,'failed','{}',?,?,?)")
        .bind(&task_id).bind(&reviewed_at).bind(&reviewed_at).bind(&reviewed_at).execute(db.pool()).await.unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    for identity in ["review:one", "review:progress-review"] {
        db::budget::charge(&mut tx, &task_id, "review", 2, identity)
            .await
            .unwrap();
    }
    db::budget::charge(
        &mut tx,
        &task_id,
        "review_progress",
        2,
        "progress:progress-review",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let service = engine(Arc::clone(&db), Arc::clone(&event_bus));
    let transition = service
        .workflow_execution()
        .transition(
            &task_id,
            "review",
            task.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "progress repair completed",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    let result = drain_result(service, transition).await;
    assert_eq!(result.task.status, "review");
    let annotation: serde_json::Value = serde_json::from_str(
        result
            .task
            .error_annotation
            .as_deref()
            .expect("infrastructure annotation"),
    )
    .unwrap();
    assert_ne!(annotation["blocking_reason"], "review_ci_infrastructure");
    assert!(
        result.task.entry_barrier_json.is_some(),
        "configuration errors remain parked"
    );
    assert_eq!(
        db::budget::spent(db.pool(), &task_id, "review")
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        db::budget::spent(db.pool(), &task_id, "review_progress")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db::budget::spent(db.pool(), &task_id, "review_ci_infrastructure")
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        db::ReviewRepo::list_by_task(&*db, &task_id)
            .await
            .unwrap()
            .len(),
        1
    );
}

/// Entry failures and cancelled attempts share the built-in gate's bounded
/// bounce/park behavior, including the final retry barrier.
#[tokio::test]
async fn renamed_review_gate_failed_and_cancelled_entries_match_default() {
    for cancelled in [false, true] {
        for gate_name in ["review", "qa_review"] {
            let db = Arc::new(sqlite_db().await);
            let event_bus = Arc::new(EventBus::new(64));
            let task_id = new_uuid_v4();
            let mut workflow = user_approval_review_workflow(
                hook("run_ci_steps", FailurePolicy::Block),
                Vec::new(),
            );
            for state in &mut workflow.states {
                for trigger in state.triggers.values_mut() {
                    if trigger.to == "review" {
                        trigger.to = gate_name.into();
                    }
                }
                if state.name == "review" {
                    state.name = gate_name.into();
                    state.role = Some("reviewer".into());
                    state.canonical_phase = Some(api_types::CanonicalPhase::Review);
                    state.config = json!({"ci_steps":"not-an-array"});
                }
            }
            seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
            if cancelled {
                let now = now_rfc3339();
                sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('cancelled-coder',?,'coder','completed',?,?)")
                    .bind(&task_id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
                sqlx::query("INSERT INTO review(id,task_id,execution_id,attempt_number,status,step_results_json,started_at,created_at,updated_at) VALUES('cancelled-review',?,'cancelled-coder',1,'cancelled','{}',?,?,?)")
                    .bind(&task_id).bind(&now).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
            }
            let service = engine(Arc::clone(&db), Arc::clone(&event_bus));
            for attempt in 1..=3 {
                let task = TaskRepo::get_by_id(&*db, &task_id, false)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(task.status, "working");
                let result = service
                    .workflow_execution()
                    .transition(
                        &task_id,
                        gate_name,
                        task.version,
                        &workflow,
                        &api_types::Actor::system(api_types::SystemComponent::Workflow),
                        "submit",
                        false,
                        Default::default(),
                    )
                    .await
                    .unwrap();
                let result = drain_result(service.clone(), result).await;
                assert_eq!(
                    result.task.status,
                    if attempt < 3 { "working" } else { gate_name },
                    "cancelled={cancelled}, gate={gate_name}"
                );
                if attempt == 3 {
                    let barrier: serde_json::Value = serde_json::from_str(
                        result
                            .task
                            .entry_barrier_json
                            .as_deref()
                            .expect("exhausted gate parks"),
                    )
                    .unwrap();
                    assert_eq!(barrier["state"], gate_name);
                    assert_eq!(barrier["status"], "blocked");
                    assert!(barrier["blocking_reason"]
                        .as_str()
                        .unwrap()
                        .contains("review retry budget exhausted"));
                }
            }
            assert_eq!(
                db::budget::spent(db.pool(), &task_id, "review")
                    .await
                    .unwrap(),
                if cancelled { 2 } else { 3 }
            );
            if cancelled {
                assert_eq!(
                    db::ReviewRepo::list_by_task(&*db, &task_id).await.unwrap()[0].status,
                    db::ReviewStatus::Cancelled
                );
            }
        }
    }
}
