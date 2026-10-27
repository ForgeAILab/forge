//! Workflow engine cascades tests.

use super::*;

#[tokio::test]
async fn long_legitimate_cascade_returns_own_commit_then_drains_without_parking() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(256));
    let mut events = bus.subscribe();
    let id = "long-chain";
    seed_project_repo_and_task(&db, id, "start").await;
    // 40 distinct edges, over four times the hotfix's nine-hop carry chain.
    let workflow = cascade_chain_workflow(40);
    let result = engine(db.clone(), bus.clone())
        .workflow_execution()
        .transition(
            id,
            "step_0",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.task.status, "step_0");
    assert_eq!(result.pending_steps, 1);
    assert_eq!(
        TaskRepo::get_by_id(&*db, id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "step_0"
    );
    let settled = drain(engine(db.clone(), bus), id).await;
    assert_eq!(settled.status, "done");
    assert!(settled.blocked_json.is_none());
    assert_eq!(
        TransitionLogRepo::list_by_task(&*db, id)
            .await
            .unwrap()
            .len(),
        41
    );
    assert!(db::TaskStepRepo::task_steps(&*db, id)
        .await
        .unwrap()
        .iter()
        .all(|s| s.status == "done"));
    assert!(!drain_events(&mut events)
        .iter()
        .any(|e| e.event_type == "transition.loop_detected"));
}

#[tokio::test]
async fn automatic_chain_bound_parks_and_publishes_loop_event() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(512));
    let mut events = bus.subscribe();
    let id = "bounded-chain";
    seed_project_repo_and_task(&db, id, "start").await;
    let workflow = cascade_chain_workflow(66);
    engine(db.clone(), bus.clone())
        .workflow_execution()
        .transition(
            id,
            "step_0",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    let settled = drain(engine(db.clone(), bus), id).await;
    assert_eq!(settled.status, "step_64");
    assert!(settled
        .blocked_json
        .as_deref()
        .unwrap()
        .contains("limit 64"));
    let steps = db::TaskStepRepo::task_steps(&*db, id).await.unwrap();
    assert_eq!(steps.last().unwrap().status, "parked");
    assert_eq!(steps.last().unwrap().chain_position, 65);
    assert_eq!(
        drain_events(&mut events)
            .iter()
            .filter(|e| e.event_type == "transition.loop_detected")
            .count(),
        1
    );
}

#[tokio::test]
async fn review_carry_two_refresh_rounds_complete_without_parking() {
    use db::{ReviewRepo, TaskStepRepo};
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(128));
    let id = "review-carry-chain";
    seed_project_repo_and_task(&db, id, "review").await;
    let mut merging = with_trigger(
        state("merging", StateKind::Gate, None, StateHooks::default()),
        WorkflowTrigger::Accept,
        "done",
    );
    merging.triggers.insert(
        WorkflowTrigger::Reject,
        WorkflowTriggerDefinition {
            to: "merge_failed".into(),
            dispatch: None,
        },
    );
    let workflow = WorkflowDefinition {
        roles: vec![],
        configuration: vec![],
        cancellation_state: None,
        states: vec![
            with_trigger(
                state("review", StateKind::Gate, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "merging",
            ),
            merging,
            with_trigger(
                state("merge_failed", StateKind::Gate, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "review",
            ),
            state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
    };
    let driver = engine(db.clone(), bus);
    let mut parent: Option<db::TaskStep> = None;
    let mut previous_chain = None;
    // Each rebase/CI result creates a new Review, exactly the carry producer's
    // new evidence boundary. A repeated edge without that result parks.
    for (index, target) in [
        "merging",
        "merge_failed",
        "review",
        "merging",
        "merge_failed",
        "review",
        "merging",
        "done",
    ]
    .iter()
    .enumerate()
    {
        if index == 3 || index == 6 {
            let now = now_rfc3339();
            let execution_id = new_uuid_v4();
            sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES (?,?,'coder','completed',?,?)")
                .bind(&execution_id).bind(id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
            ReviewRepo::create(
                &*db,
                db::CreateReview {
                    id: new_uuid_v4(),
                    task_id: id.into(),
                    execution_id,
                    attempt_number: index as i64,
                    status: db::ReviewStatus::Passed,
                    step_results_json: "{}".into(),
                    started_at: now.clone(),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await
            .unwrap();
        }
        let task = TaskRepo::get_by_id(&*db, id, false).await.unwrap().unwrap();
        let input = driver
            .workflow_execution()
            .cascade_step_input(
                &task,
                &workflow,
                (*target).into(),
                "review carry fixture".into(),
                Default::default(),
                false,
                false,
                None,
                parent.as_ref(),
                format!("carry:{index}"),
                None,
            )
            .await
            .unwrap();
        if index == 3 || index == 6 {
            assert_ne!(previous_chain.as_ref(), Some(&input.chain_id));
            assert_eq!(input.chain_position, 1);
        }
        previous_chain = Some(input.chain_id.clone());
        db.enqueue_step(&input).await.unwrap();
        db.ready_step(&input.id).await.unwrap();
        let settled = drain(driver.clone(), id).await;
        assert_eq!(settled.status, *target);
        assert!(settled.blocked_json.is_none());
        parent = db.task_steps(id).await.unwrap().last().cloned();
    }
    assert_eq!(
        TransitionLogRepo::list_by_task(&*db, id)
            .await
            .unwrap()
            .len(),
        8
    );
    assert!(db
        .task_steps(id)
        .await
        .unwrap()
        .iter()
        .all(|s| s.status == "done"));
}

#[tokio::test]
async fn repeated_edge_parks_instead_of_transitioning_again() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(64));
    let id = "loop-chain";
    seed_project_repo_and_task(&db, id, "start").await;
    let mut workflow = cascade_chain_workflow(2);
    workflow.states[2]
        .triggers
        .get_mut(&WorkflowTrigger::Accept)
        .unwrap()
        .to = "step_0".into();
    workflow.states[2].hooks.on_enter =
        vec![hook("auto_cascade_on_unassigned_role", FailurePolicy::Log)];
    engine(db.clone(), bus.clone())
        .workflow_execution()
        .transition(
            id,
            "step_0",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    let settled = drain(engine(db.clone(), bus), id).await;
    assert_eq!(settled.status, "step_0");
    assert!(settled.blocked_json.is_some());
    assert_eq!(
        TransitionLogRepo::list_by_task(&*db, id)
            .await
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        db::TaskStepRepo::task_steps(&*db, id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .status,
        "parked"
    );
}

#[tokio::test]
async fn moved_task_supersedes_pending_cascade() {
    let db = Arc::new(sqlite_db().await);
    let bus = Arc::new(EventBus::new(64));
    let id = "superseded-chain";
    seed_project_repo_and_task(&db, id, "start").await;
    let workflow = cascade_chain_workflow(2);
    engine(db.clone(), bus.clone())
        .workflow_execution()
        .transition(
            id,
            "step_0",
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "start",
            false,
            Default::default(),
        )
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status='done',version=version+1 WHERE id=?")
        .bind(id)
        .execute(db.pool())
        .await
        .unwrap();
    let settled = drain(engine(db.clone(), bus), id).await;
    assert_eq!(settled.status, "done");
    assert_eq!(
        TransitionLogRepo::list_by_task(&*db, id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db::TaskStepRepo::task_steps(&*db, id)
            .await
            .unwrap()
            .into_iter()
            .find(|step| step.kind == "hooks")
            .unwrap()
            .status,
        "superseded"
    );
}

#[test]
fn optional_gate_cascade_uses_typed_skip_instead_of_reason_words() {
    let mut source = state("approval", StateKind::Gate, None, StateHooks::default());
    source.gate_config = Some(GateConfig {
        requires_user_approval: Some(true),
        optional_when_unassigned: Some(true),
        reject_target: None,
        max_rejections: None,
        approve_label: None,
        reject_label: None,
    });
    assert!(WorkflowEngine::cascade_allowed(
        &source,
        &api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateSkipped),
        false
    ));
    assert!(!WorkflowEngine::cascade_allowed(
        &source,
        &api_types::TransitionBridge::default(),
        false
    ));
}
