//! Conflict and check-failure handoffs to the task workflow.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_back_hands_a_conflict_to_the_worker_as_the_merge_hook_does() {
    let world = World::new().await;
    let a = world.add_task("t", "f", "one\n").await;
    let mut events = world.bus.subscribe();
    let asked = world.place(&a.id, "rebasing", 2).await;
    world.ask(&asked, IntegrationStepAction::SendBack).await;
    world
        .set(
            &a.id,
            "state='ejected',conflict_paths_json='[\"f\",\"dir/g\"]',failure_message='conflict in f'",
        )
        .await;
    eventually("the Task was handed back", || async {
        world.task("t").await.status == "merge_failed"
    })
    .await;
    let details = "rebased onto main; conflicts were committed with markers in: f, dir/g";
    assert_eq!(
        world.comments("t").await,
        vec![format!(
            "Merge conflict handed back to the Worker: {details}"
        )]
    );
    let transitions = world.transitions("t").await;
    assert_eq!(
        transitions[0],
        (
            "merging".to_owned(),
            "merge_failed".to_owned(),
            details.to_owned(),
            Some("conflict_handoff".to_owned()),
            "system:workflow".to_owned(),
            0
        )
    );
    let payload: String = world
        .scalar(
            "SELECT bridge_payload FROM transition_log WHERE task_id=? AND bridge_kind='conflict_handoff'",
            "t",
        )
        .await;
    assert_eq!(
        serde_json::from_str::<Value>(&payload).unwrap()["paths"],
        json!(["f", "dir/g"])
    );
    let task = world.task("t").await;
    assert!(task.review_passed_at.is_none(), "the approval is void");
    let annotation: Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "merge_conflict");
    let mut failed = Vec::new();
    while let Ok(event) = events.try_recv() {
        if event.event_type == "merge.failed" {
            failed.push(event);
        }
    }
    assert_eq!(failed.len(), 1);
    let charged: i64 = world
        .scalar(
            "SELECT COALESCE((SELECT spent FROM task_budget WHERE task_id=? AND kind='conflict_handoff'),0)",
            "t",
        )
        .await;
    assert_eq!(charged, 1);
    assert!(ack(&world.attempt(&a.id).await, IntegrationStepAction::SendBack).is_some());
    assert_eq!(
        world.step("t", IntegrationStepAction::SendBack).await[0].status,
        "done"
    );
}

/// A `send_back` that stops after its comment, annotation and event but
/// before its transaction (a crash, a lost lease) is delivered again: the
/// Task ends with exactly what one delivery leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_redelivered_send_back_repeats_no_comment_charge_or_transition() {
    let world = World::new().await;
    for (task, budget, set, comment) in [
        (
            "redelivered-conflict",
            vec![("conflict_handoff".to_owned(), 1)],
            "state='ejected',conflict_paths_json='[\"f\"]',failure_message='conflict in f'",
            "Merge conflict handed back to the Worker: rebased onto main; conflicts were committed with markers in: f",
        ),
        (
            "redelivered-red",
            // One rejection out of the `merging` gate and one merge fix.
            vec![("gate:merging".to_owned(), 1), ("merge_fix".to_owned(), 1)],
            "state='ejected',failure_kind='candidate_check_failed',failure_message='make test exited 2'",
            "Merge checks failed: checks failed after the rebase onto main: make test exited 2",
        ),
    ] {
        let a = world.add_task(task, task, "one\n").await;
        let asked = world.place(&a.id, "rebasing", 2).await;
        world.set(&a.id, set).await;
        test_faults::interrupt_once(task);
        world.ask(&asked, IntegrationStepAction::SendBack).await;
        eventually("the Task was handed back", || async {
            world.task(task).await.status == "merge_failed"
        })
        .await;
        let step = &world.step(task, IntegrationStepAction::SendBack).await[0];
        assert_eq!(step.status, "done");
        assert!(step.attempts >= 2, "the step was delivered twice");
        assert_eq!(world.comments(task).await, vec![comment.to_owned()], "{task}");
        let transitions = world.transitions(task).await;
        assert_eq!(
            transitions
                .iter()
                .filter(|row| row.0 == "merging" && row.1 == "merge_failed")
                .count(),
            1,
            "{task}: {transitions:?}"
        );
        let charged: Vec<(String, i64)> =
            sqlx::query_as("SELECT kind,spent FROM task_budget WHERE task_id=? AND spent>0 ORDER BY kind")
                .bind(task)
                .fetch_all(world.db.pool())
                .await
                .unwrap();
        assert_eq!(charged, budget, "{task}");
        let cascades: i64 = world
            .scalar(
                "SELECT COUNT(*) FROM task_step WHERE task_id=? AND kind='cascade' AND causation_key LIKE 'integration:%'",
                task,
            )
            .await;
        assert_eq!(cascades, 1, "{task}");
        assert!(ack(&world.attempt(&a.id).await, IntegrationStepAction::SendBack).is_some());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_root_conflict_can_reopen_for_a_corrective_subtask() {
    let world = World::new().await;
    let a = world.add_task("root-repair", "f", "one\n").await;
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO task(id,project_id,parent_task_id,title,status,created_at,updated_at) VALUES('finished-child','p','root-repair','child','done',?,?)")
        .bind(&now).bind(&now).execute(world.db.pool()).await.unwrap();
    let asked = world.place(&a.id, "rebasing", 2).await;
    world
        .set(
            &a.id,
            "state='ejected',conflict_paths_json='[\"f\"]',failure_message='conflict in f'",
        )
        .await;
    world.ask(&asked, IntegrationStepAction::SendBack).await;
    settled(
        &world,
        "root-repair",
        IntegrationStepAction::SendBack,
        "done",
    )
    .await;
    let offers = world
        .service
        .task_action_offers(
            "root-repair",
            &api_types::Actor::user(api_types::UserActionSource::Api),
        )
        .await
        .unwrap();
    let send_back = offers
        .available_actions
        .iter()
        .find(|offer| offer.reason == "root_review_reopen")
        .expect("a root conflict has a corrective-work action");
    assert_eq!(send_back.action.verb(), "send_back");
    world
        .service
        .perform_task_action(
            "root-repair",
            api_types::TaskAction::SendBack {
                guidance: "resolve f in a corrective subtask".into(),
            },
            offers.version,
        )
        .await
        .unwrap();
    let reopened = world.task("root-repair").await;
    assert_eq!(reopened.status, "merge_failed");
    assert!(reopened.condition.integration_wait().is_none());
    // The ejected attempt keeps its row and its statement (the lineage the
    // next admission links to) and holds no place in the queue; nothing was
    // admitted or merged by the send-back.
    assert_eq!(
        reopened
            .condition
            .integration_reason()
            .map(|reason| reason.attempt_id().as_str().to_owned()),
        Some(a.id.clone())
    );
    assert!(reopened.condition.integration_handoff_ready());
    assert_eq!(
        world
            .scalar::<String>(
                "SELECT group_concat(state) FROM integration_attempt WHERE task_ref=?",
                "root-repair"
            )
            .await,
        "ejected"
    );
    assert!(matches!(
        reopened.condition,
        db::TaskCondition::Parked {
            primary: db::ParkReason::Held { .. },
            ..
        }
    ));
    let workflow = crate::workflow::default_workflow::default_workflow();
    crate::task_hierarchy::ensure_parent_accepts_subtasks(&reopened, &workflow).unwrap();
    assert_eq!(
        world
            .scalar::<i64>(
                "SELECT COUNT(*) FROM execution WHERE task_id=? AND status='running'",
                "root-repair"
            )
            .await,
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_manually_repaired_root_hands_integration_ownership_to_fresh_review() {
    let world = World::new().await;
    let a = world
        .add_task(
            "manual-root",
            "f",
            "<<<<<<< ours\none\n=======\ntwo\n>>>>>>> theirs\n",
        )
        .await;
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO task(id,project_id,parent_task_id,title,status,created_at,updated_at) VALUES('manual-child','p','manual-root','child','done',?,?)")
        .bind(&now).bind(&now).execute(world.db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES('manual-reviewer','manual-root','reviewer','user','owner',?,?)")
        .bind(&now).bind(&now).execute(world.db.pool()).await.unwrap();
    let asked = world.place(&a.id, "rebasing", 2).await;
    world
        .set(
            &a.id,
            "state='ejected',conflict_paths_json='[\"f\"]',failure_message='conflict in f'",
        )
        .await;
    world.ask(&asked, IntegrationStepAction::SendBack).await;
    settled(
        &world,
        "manual-root",
        IntegrationStepAction::SendBack,
        "done",
    )
    .await;
    std::fs::write(world.tree("manual-root").join("f"), "resolved\n").unwrap();
    git::commit_all(&world.tree("manual-root"), "manual repair")
        .await
        .unwrap();
    let offers = world
        .service
        .task_action_offers(
            "manual-root",
            &api_types::Actor::user(api_types::UserActionSource::Api),
        )
        .await
        .unwrap();
    assert!(offers
        .available_actions
        .iter()
        .any(|offer| offer.reason == "manual_merge_repair"));
    world
        .service
        .perform_task_action(
            "manual-root",
            api_types::TaskAction::retry(),
            offers.version,
        )
        .await
        .unwrap();
    // The action is accepted into recovery; the real dispatcher, not the
    // Task-step worker alone, issues its replay command.
    sqlx::query("UPDATE project SET primary_repo_id='r' WHERE id='p'")
        .execute(world.db.pool())
        .await
        .unwrap();
    let dispatcher = crate::TaskDispatcher::new(
        world.db.clone(),
        world.bus.clone(),
        Arc::new(world.service.clone()),
    );
    eventually(
        "manual repair reaches review and gives ownership away",
        || async {
            dispatcher.reconcile_all().await.unwrap();
            let task = world.task("manual-root").await;
            task.status == "review"
                && task.condition.integration_wait().is_none()
                && task.condition.integration_handoff_ready()
        },
    )
    .await;
    assert_eq!(
        world
            .scalar::<i64>(
                "SELECT COUNT(*) FROM execution WHERE task_id=? AND status='running'",
                "manual-root"
            )
            .await,
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_back_routes_a_red_check_and_a_lost_review_through_merge_failed() {
    let world = World::new().await;
    // A red check on the rebased commit: the coder's, under the merge-fix
    // budget, as any failed integration.
    let red = world.add_task("red", "f", "one\n").await;
    let asked = world.place(&red.id, "checking", 2).await;
    world.ask(&asked, IntegrationStepAction::SendBack).await;
    world
        .set(
            &red.id,
            "state='ejected',failure_kind='candidate_check_failed',failure_message='`make test` exited 2: boom'",
        )
        .await;
    eventually("the red Task went to merge_failed", || async {
        world.task("red").await.status == "merge_failed"
    })
    .await;
    let transitions = world.transitions("red").await;
    assert_eq!(
        (
            transitions[0].1.as_str(),
            &transitions[0].3,
            transitions[0].5
        ),
        ("merge_failed", &None, 1),
        "a plain rejection out of the merge gate"
    );
    assert!(transitions[0]
        .2
        .contains("checks failed after the rebase onto main"));
    let annotation: Value = serde_json::from_str(
        world
            .task("red")
            .await
            .error_annotation
            .as_deref()
            .unwrap_or("{}"),
    )
    .unwrap_or_default();
    let comments = world.comments("red").await;
    assert!(
        comments[0].starts_with("Merge checks failed:"),
        "{comments:?} {annotation}"
    );

    // Review authority lost: today's review refresh.
    let stale = world.add_task("stale", "g", "two\n").await;
    let asked = world.place(&stale.id, "awaiting_task_step", 2).await;
    world.ask(&asked, IntegrationStepAction::SendBack).await;
    world
        .set(
            &stale.id,
            "state='needs_review',failure_message='review acceptance is stale; fresh review required'",
        )
        .await;
    eventually("the stale Task left merging", || async {
        !world.transitions("stale").await.is_empty()
    })
    .await;
    let first = world.transitions("stale").await[0].clone();
    assert_eq!(
        (
            first.0.as_str(),
            first.1.as_str(),
            first.3.as_deref(),
            first.5
        ),
        ("merging", "merge_failed", Some("review_refresh"), 0)
    );
    assert_eq!(
        first.2,
        "conformance review required: review acceptance is stale; fresh review required"
    );
    eventually("review authority was cleared", || async {
        world.task("stale").await.review_passed_at.is_none()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_back_merge_fix_brief_carries_command_and_output() {
    use crate::workflow::dispatch::{
        loader, resolve_prompt_builder, BUILDER_ID_CODER_MERGE_FIX_V2,
        BUILDER_ID_WORKER_MERGE_FIX_V1,
    };
    let world = World::new().await;
    let attempt = world.add_task("brief-red", "f", "one\n").await;
    let asked = world.place(&attempt.id, "checking", 2).await;
    let failure = format!(
        "`cargo test` exited 101: parser::empty FAILED\n{}\n+5 more\n{}TAIL-END",
        (0..20)
            .map(|i| format!("binary::case_{i}"))
            .collect::<Vec<_>>()
            .join("\n"),
        "x".repeat(2040)
    );
    world
        .set(
            &attempt.id,
            &format!(
                "state='ejected',failure_kind='candidate_check_failed',failure_message='{failure}'"
            ),
        )
        .await;
    world.ask(&asked, IntegrationStepAction::SendBack).await;
    eventually("red check reaches repair", || async {
        world.task("brief-red").await.status == "merge_failed"
    })
    .await;
    let workflow = crate::workflow::default_workflow::default_workflow();
    for (builder, role) in [
        (BUILDER_ID_CODER_MERGE_FIX_V2, "coder"),
        (BUILDER_ID_WORKER_MERGE_FIX_V1, "worker"),
    ] {
        let context = loader::load_agent_dispatch_context(loader::DispatchContextParams {
            db: world.db.clone(),
            router: &world.service.workspace_backend_router,
            task_id: "brief-red",
            role,
            state_name: "merge_failed",
            state_config: json!({}),
            execution_policy: None,
            workflow: &workflow,
        })
        .await
        .unwrap();
        let text = resolve_prompt_builder(builder).build(&context).user;
        for expected in [
            "MERGE-CI",
            "main",
            "cargo test",
            "parser::empty FAILED",
            "binary::case_19",
            "+5 more",
            "TAIL-END",
            "merge_fix budget: 1 of 1 spent; 0 remaining",
        ] {
            assert!(text.contains(expected), "{text}");
        }
        assert!(!text.contains("manual task-worktree repair"), "{text}");
        let window = world.db.review_window("brief-red").await.unwrap();
        assert!(db::review_findings::finding_ledger(&window.reviews)
            .unwrap()
            .is_empty());
    }
}
