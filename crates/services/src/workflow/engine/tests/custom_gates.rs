//! Workflow engine custom gates tests.

use super::*;

#[tokio::test]
async fn review_refresh_bridge_skips_merge_repair_entry_hooks() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();

    let mut merging = with_trigger(
        state(
            default_states::MERGING,
            StateKind::Gate,
            None,
            StateHooks::default(),
        ),
        WorkflowTrigger::Reject,
        default_states::MERGE_FAILED,
    );
    merging.canonical_phase = Some(api_types::CanonicalPhase::Review);

    let mut merge_failed = with_trigger(
        state(
            default_states::MERGE_FAILED,
            StateKind::Active,
            Some(default_roles::CODER),
            StateHooks {
                before_enter: vec![hook("run_before_work_hooks", FailurePolicy::Block)],
                on_enter: vec![hook("dispatch_role_agent", FailurePolicy::Log)],
                ..StateHooks::default()
            },
        ),
        WorkflowTrigger::Accept,
        default_states::REVIEW,
    );
    merge_failed.canonical_phase = Some(api_types::CanonicalPhase::Review);

    let mut review = state(
        default_states::REVIEW,
        StateKind::Gate,
        Some(default_roles::REVIEWER),
        StateHooks::default(),
    );
    review.canonical_phase = Some(api_types::CanonicalPhase::Review);

    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![merging, merge_failed, review],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let project_id =
        seed_custom_workflow_task(&db, &task_id, default_states::MERGING, &workflow).await;

    // If the intermediate repair state's entry hook runs, this deliberately
    // invalid settings payload makes it block before the review cascade.
    sqlx::query("UPDATE project SET settings = ? WHERE id = ?")
        .bind("not-json")
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project settings become invalid");

    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            &task_id,
            default_states::MERGE_FAILED,
            1,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "Reviewed commit changed; fresh review required",
            true,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::ReviewRefresh),
        )
        .await
        .expect("review refresh crosses the repair bridge");

    let result = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;

    assert_eq!(result.task.status, default_states::REVIEW);
    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    let bridge = logs
        .iter()
        .find(|entry| {
            entry.from_state == default_states::MERGING
                && entry.to_state == default_states::MERGE_FAILED
        })
        .expect("review-refresh bridge transition exists");
    assert!(
        !bridge.rejection,
        "review-refresh bridges are not retry rejections"
    );
    let results: Vec<HookResultEntry> = serde_json::from_str(
        bridge
            .hook_results_json
            .as_deref()
            .expect("bridge hook results are recorded"),
    )
    .expect("bridge hook results deserialize");
    assert!(
        results.iter().all(|entry| entry.phase != "before_enter"),
        "repair entry hooks must not run for the review-refresh bridge: {results:?}"
    );
    assert!(results.iter().any(|entry| {
        entry.phase == "on_enter"
            && entry.action == "dispatch_role_agent"
            && entry.outcome == "cascade"
    }));
}

#[tokio::test]
async fn custom_workflow_renamed_states_lifecycle() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("pending", StateKind::Initial, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "working",
            ),
            with_trigger(
                state("working", StateKind::Active, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "checking",
            ),
            with_trigger(
                state("checking", StateKind::Gate, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "shipped",
            ),
            state("shipped", StateKind::Terminal, None, StateHooks::default()),
            state(
                "abandoned",
                StateKind::Terminal,
                None,
                StateHooks::default(),
            ),
        ],
        configuration: Vec::new(),
        cancellation_state: Some("abandoned".to_owned()),
    };
    seed_custom_workflow_task(&db, &task_id, "pending", &workflow).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let r1 = eng
        .workflow_execution()
        .transition(
            &task_id,
            "working",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "claim",
            false,
            Default::default(),
        )
        .await
        .expect("pending → working");
    assert_eq!(r1.task.status, "working");

    let r2 = eng
        .workflow_execution()
        .transition(
            &task_id,
            "checking",
            r1.task.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::General),
            "completed",
            false,
            Default::default(),
        )
        .await
        .expect("working → checking");
    assert_eq!(r2.task.status, "checking");

    let r3 = eng
        .workflow_execution()
        .transition(
            &task_id,
            "shipped",
            r2.task.version,
            &workflow,
            &api_types::Actor::system(api_types::SystemComponent::General),
            "approved",
            false,
            Default::default(),
        )
        .await
        .expect("checking → shipped");
    assert_eq!(r3.task.status, "shipped");
}

#[tokio::test]
async fn implicit_accept_transition_moves_to_next_declared_state() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            state("pending", StateKind::Initial, None, StateHooks::default()),
            state("working", StateKind::Active, None, StateHooks::default()),
            state("shipped", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    seed_custom_workflow_task(&db, &task_id, "pending", &workflow).await;

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            &task_id,
            "working",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "accept",
            false,
            Default::default(),
        )
        .await
        .expect("pending implicitly accepts to working");

    assert_eq!(result.task.status, "working");
}

#[tokio::test]
async fn gate_approve_reject_on_custom_gate_states() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let mut start_state = state("start", StateKind::Initial, None, StateHooks::default());
    start_state.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "coding".to_owned(),
            dispatch: None,
        },
    );
    let mut coding_state = state("coding", StateKind::Active, None, StateHooks::default());
    coding_state.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "qa".to_owned(),
            dispatch: None,
        },
    );
    let mut qa_state = state("qa", StateKind::Gate, None, StateHooks::default());
    qa_state.gate_config = Some(api_types::GateConfig {
        reject_target: Some("coding".to_owned()),
        max_rejections: Some(2),
        approve_label: None,
        reject_label: None,
        requires_user_approval: Some(false),
        optional_when_unassigned: Some(false),
    });
    qa_state.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "released".to_owned(),
            dispatch: None,
        },
    );
    qa_state.triggers.insert(
        WorkflowTrigger::Reject,
        WorkflowTriggerDefinition {
            to: "coding".to_owned(),
            dispatch: None,
        },
    );
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            start_state,
            coding_state,
            qa_state,
            state("released", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let task_a_id = new_uuid_v4();
    let task_b_id = new_uuid_v4();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "Custom".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: serde_json::to_string(&workflow).unwrap(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            remote_url: Some("https://example.com/repo.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    for (task_id, title) in [
        (task_a_id.as_str(), "custom gate approve"),
        (task_b_id.as_str(), "custom gate reject"),
    ] {
        TaskRepo::create(
            &*db,
            CreateTask {
                id: task_id.to_owned(),
                project_id: project_id.clone(),
                parent_task_id: None,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: title.to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "start".to_owned(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("task creates");
    }
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let current_task = TaskRepo::get_by_id(&*db, &task_a_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_a_id,
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

    let current_task = TaskRepo::get_by_id(&*db, &task_a_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_a_id,
            "qa",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request qa",
            false,
            Default::default(),
        )
        .await
        .expect("coding → qa");
    assert_eq!(result.task.status, "qa");

    let current_task = TaskRepo::get_by_id(&*db, &task_a_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_a_id,
            "released",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate approved",
            false,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateApproved),
        )
        .await
        .expect("qa → released");
    assert_eq!(result.task.status, "released");

    let current_task = TaskRepo::get_by_id(&*db, &task_b_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_b_id,
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

    let current_task = TaskRepo::get_by_id(&*db, &task_b_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_b_id,
            "qa",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request qa",
            false,
            Default::default(),
        )
        .await
        .expect("coding → qa");
    assert_eq!(result.task.status, "qa");

    let current_task = TaskRepo::get_by_id(&*db, &task_b_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_b_id,
            "coding",
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate rejected",
            true,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateRejected),
        )
        .await
        .expect("qa → coding (reject)");
    assert_eq!(result.task.status, "coding");
}

#[tokio::test]
async fn user_approval_gate_failed_blocking_before_enter_cascades_to_reject_target() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow =
        user_approval_review_workflow(hook("run_ci_steps", FailurePolicy::Block), Vec::new());
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(r#"{"review":{"ci_steps":"not-an-array"}}"#)
        .bind(&task_id)
        .execute(db.pool())
        .await
        .expect("task review config updates");

    let result = engine(Arc::clone(&db), Arc::clone(&event_bus))
        .workflow_execution()
        .transition(
            &task_id,
            "review",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "submit for validation",
            false,
            Default::default(),
        )
        .await
        .expect("failed validation cascades back to working");

    let result = drain_result(engine(Arc::clone(&db), Arc::clone(&event_bus)), result).await;

    assert_eq!(result.task.status, "working");
    assert!(result.task.entry_barrier_json.is_none());
    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    assert!(
        logs.iter().any(|entry| {
            entry.from_state == "review" && entry.to_state == "working" && entry.rejection
        }),
        "validation rejection should be recorded on the automatic reject cascade"
    );
}

#[tokio::test]
async fn user_review_ci_success_still_waits_for_human() {
    let fixture = failed_ci_fixture(3, FailurePolicy::Block).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(r#"{"review":{"ci_steps":["test -d ."]}}"#)
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
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "request human review",
            false,
            fixture.workflow_authority().await,
        )
        .await
        .unwrap();
    let result = drain_result(fixture.engine.clone(), result).await;
    assert_eq!(result.task.status, "review");
    assert!(result.task.entry_barrier_json.is_none());
    assert!(
        !db::TaskStepRepo::task_steps(&*fixture.db, &fixture.task.id)
            .await
            .unwrap()
            .iter()
            .any(|s| s.kind == "cascade")
    );
    let review = result.review.unwrap();
    assert_eq!(review.status, db::ReviewStatus::AwaitingHuman);
    let details: serde_json::Value = serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(details["ci_steps"][0]["exit_code"], 0);
}

#[tokio::test]
async fn user_approval_gate_passing_hooks_pauses_forward_cascade_for_human() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = user_approval_review_workflow(
        hook("check_retry_budget", FailurePolicy::Block),
        vec![hook("auto_cascade_on_completion", FailurePolicy::Log)],
    );
    seed_custom_workflow_task(&db, &task_id, "working", &workflow).await;

    let result = engine(Arc::clone(&db), event_bus)
        .workflow_execution()
        .transition(
            &task_id,
            "review",
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "submit for validation",
            false,
            Default::default(),
        )
        .await
        .expect("passing validation pauses for human approval");

    assert_eq!(result.task.status, "review");
    assert!(result.task.entry_barrier_json.is_none());
    assert!(!db::TaskStepRepo::task_steps(&*db, &task_id)
        .await
        .unwrap()
        .iter()
        .any(|s| s.kind == "cascade"));
    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition logs load");
    assert!(!logs.iter().any(|entry| entry.rejection));
}

#[tokio::test]
async fn planning_gate_reject_back_to_itself() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let workflow = default_workflow::default_workflow();
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();

    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "Default".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: serde_json::to_string(&workflow).unwrap(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "repo".to_owned(),
            remote_url: Some("https://example.com/repo.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    TaskRepo::create(
        &*db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.clone(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "planning reject".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: default_states::TODO.to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("task creates");
    assign_user_role(&db, &task_id, default_roles::PLANNER).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "enter planning",
            false,
            Default::default(),
        )
        .await
        .expect("todo → planning");
    assert_eq!(result.task.status, default_states::PLANNING);

    let current_task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            default_states::PLANNING,
            current_task.version,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate rejected",
            true,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateRejected),
        )
        .await
        .expect("planning rejects back to itself");
    assert_eq!(result.task.status, default_states::PLANNING);
}

#[tokio::test]
async fn gate_reject_back_to_itself() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let task_id = new_uuid_v4();
    let mut planning = state("planning", StateKind::Gate, None, StateHooks::default());
    planning.gate_config = Some(api_types::GateConfig {
        reject_target: Some("planning".to_owned()),
        max_rejections: Some(3),
        approve_label: None,
        reject_label: None,
        requires_user_approval: Some(false),
        optional_when_unassigned: Some(false),
    });
    planning.triggers.insert(
        WorkflowTrigger::Reject,
        WorkflowTriggerDefinition {
            to: "planning".to_owned(),
            dispatch: None,
        },
    );
    planning.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "working".to_owned(),
            dispatch: None,
        },
    );
    let workflow = WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            with_trigger(
                state("todo", StateKind::Initial, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "planning",
            ),
            planning,
            with_trigger(
                state("working", StateKind::Active, None, StateHooks::default()),
                WorkflowTrigger::Accept,
                "done",
            ),
            state("done", StateKind::Terminal, None, StateHooks::default()),
        ],
        configuration: Vec::new(),
        cancellation_state: None,
    };
    seed_custom_workflow_task(&db, &task_id, "planning", &workflow).await;
    let eng = engine(Arc::clone(&db), Arc::clone(&event_bus));

    let reject_target = workflow
        .gate_reject_target("planning")
        .expect("planning has reject_target");
    assert_eq!(reject_target, "planning");

    let result = eng
        .workflow_execution()
        .transition(
            &task_id,
            reject_target,
            1,
            &workflow,
            &api_types::Actor::user(api_types::UserActionSource::Test),
            "gate rejected: needs more detail",
            true,
            api_types::TransitionBridge::new(api_types::TransitionBridgeKind::GateRejected),
        )
        .await
        .expect("planning → planning (reject)");
    assert_eq!(result.task.status, "planning");

    let logs = TransitionLogRepo::list_by_task(&*db, &task_id)
        .await
        .expect("transition log loads");
    assert!(
        logs.iter().any(|entry| entry.from_state == "planning"
            && entry.to_state == "planning"
            && entry.rejection),
        "transition log should record the rejection"
    );
}
