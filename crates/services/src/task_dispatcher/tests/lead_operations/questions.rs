//! Tests for questions in lead operations.

use super::*;
/// The request-input wait. The lead asks, its run ends, and the root waits
/// in planning under a typed park with one Attention item: no relaunch
/// across ten passes and a restart, subtasks held. The owner's answer ends
/// the wait, the lead runs again with the answer in its input, and no
/// re-plan is charged.
#[tokio::test]
async fn a_lead_question_parks_the_root_until_the_owner_answers() {
    let mut ops = ops_fixture(true).await;
    let (root, child) = root_with_child(&ops.lead, true).await;
    ops.script(vec![
        vec![Call::Raw(
            "lead.request_input",
            json!({"question": "Which database should the service use?"}),
        )],
        vec![],
    ]);
    {
        let lead = &ops.lead;
        until(lead, "the root waits for the answer", async || {
            executions(&lead.db, &root.id).await
                == vec![(PLANNER.to_owned(), "completed".to_owned())]
                && park(&lead.db, &root.id).await.contains("HumanWork")
        })
        .await;
    }
    let lead = &ops.lead;
    let results = ops.results();
    assert!(results[0].1.is_ok(), "{results:?}");
    let waiting = reload(&lead.db, &root.id).await;
    assert_eq!(waiting.status, "planning");
    let condition = serde_json::to_string(&waiting.condition.public()).unwrap();
    assert!(
        condition.contains("human_decision") && condition.contains("lead_input"),
        "{condition}"
    );
    let asked = comments(lead, &root.id).await;
    let question = asked
        .iter()
        .find(|comment| comment.worklog_kind.as_deref() == Some("request_input"))
        .expect("the question is a comment on the root");
    assert_eq!(question.content, "Which database should the service use?");
    assert_eq!(question.author_id.as_deref(), Some(lead.planner.as_str()));
    assert!(question.execution_id.is_some());
    let (item, action, status, version): (String, String, String, i64) = sqlx::query_as(
        "SELECT id, recommended_action, status, version FROM attention_projection WHERE dedupe_key=?",
    )
    .bind(format!("lead-input:{}:{}", root.id, question.id))
    .fetch_one(lead.db.pool())
    .await
    .unwrap();
    assert_eq!(
        (action.as_str(), status.as_str()),
        ("answer_lead_input", "open")
    );
    let held = park(&lead.db, &child.id).await;
    assert!(
        held.contains("ParentWait") && held.contains("planning"),
        "{held}"
    );

    // Ten passes and a restart: the lead is not run again and nothing moves.
    passes(lead, 10).await;
    let restarted = TaskDispatcher::with_check_interval(
        Arc::clone(&lead.db),
        Arc::new(EventBus::new(64)),
        Arc::clone(&ops.service),
        Duration::from_millis(10),
    );
    for _ in 0..3 {
        restarted.check_once_and_drain().await.unwrap();
    }
    assert_eq!(
        executions(&lead.db, &root.id).await,
        vec![(PLANNER.to_owned(), "completed".to_owned())]
    );
    assert_eq!(reload(&lead.db, &root.id).await.status, "planning");
    assert!(executions(&lead.db, &child.id).await.is_empty());

    // The item is a question: it is resolved with an answer and not without.
    let attention = crate::attention_service::AttentionService::new(
        Arc::clone(&lead.db),
        crate::test_support::noop_wake_turns(),
    );
    assert!(attention.resolve("owner", &item, version).await.is_err());
    assert_eq!(reload(&lead.db, &root.id).await.status, "planning");
    let resolved = attention
        .resolve_with_answer("owner", &item, version, Some("Postgres, version 16"))
        .await
        .unwrap();
    assert_eq!(resolved.status, "resolved");
    until(
        lead,
        "the lead ran again and the root implements",
        async || {
            executions(&lead.db, &root.id).await
                == vec![
                    (PLANNER.to_owned(), "completed".to_owned()),
                    (PLANNER.to_owned(), "completed".to_owned()),
                ]
                && reload(&lead.db, &root.id).await.status == "in_progress"
                && running(lead, &child.id, CODER).await
        },
    )
    .await;
    assert_eq!(
        spent(lead, &root.id, "replan").await,
        0,
        "an answer is not a re-plan"
    );
    let condition =
        serde_json::to_string(&reload(&lead.db, &root.id).await.condition.public()).unwrap();
    assert!(!condition.contains("lead_input"), "{condition}");
    let mut inputs = Vec::new();
    while let Ok(ctx) = ops.lead.rx.try_recv() {
        if ctx.task_id == root.id {
            inputs.push(ctx.description);
        }
    }
    assert_eq!(inputs.len(), 2);
    assert!(!inputs[0].contains("Postgres"));
    assert!(
        inputs[1].contains("Which database should the service use?")
            && inputs[1].contains("Answer to the lead's question: Postgres, version 16"),
        "the second run reads the answer: {}",
        inputs[1]
    );
}

/// A question is for the planning run on the root: a second one while the
/// root already waits is refused, typed.
#[tokio::test]
async fn a_second_question_while_the_root_waits_is_refused() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root, _) = root_with_child(lead, true).await;
    until(lead, "the root plans", async || {
        running(lead, &root.id, PLANNER).await
    })
    .await;
    let first = ops
        .call(
            &root.id,
            "lead.request_input",
            json!({"question": "A or B?"}),
            "q1",
        )
        .await
        .unwrap();
    // The same request again answers the same question.
    let replay = ops
        .call(
            &root.id,
            "lead.request_input",
            json!({"question": "A or B?"}),
            "q1",
        )
        .await
        .unwrap();
    assert_eq!(first["comment_id"], replay["comment_id"]);
    let refused = ops
        .call(
            &root.id,
            "lead.request_input",
            json!({"question": "and C?"}),
            "q2",
        )
        .await
        .unwrap_err();
    assert!(refused.contains("lead_input_pending"), "{refused}");
    let items: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM attention_projection WHERE recommended_action='answer_lead_input'",
    )
    .fetch_one(lead.db.pool())
    .await
    .unwrap();
    assert_eq!(items, 1);
}

/// A step check is a verdict on one subtask and nothing else. Its run is the
/// lead's Agent in the lead's role, but it leads nothing: the server derives
/// no root for it, no `lead.*` operation is in its scope, and the breakdown
/// is not changed by it. (The planning run on the root is the one place the
/// lead changes the breakdown; a step it cannot pass ends there by itself.)
#[tokio::test]
async fn a_step_check_run_has_no_lead_operation() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let root = seed_task(&lead.db, &lead.project, "root", "in_progress", 1).await;
    let checked = seed_subtask(
        &lead.db,
        &root,
        "first step",
        db::inherited_subtask::STEP_CHECK_STATE,
        0,
    )
    .await;
    let next = seed_subtask(&lead.db, &root, "second step", "todo", 1).await;
    seed_completed_coder_execution(&lead.db, &checked.id).await;
    assign_role(&lead.db, &root.id, CODER, &lead.coder).await;
    assign_role(&lead.db, &root.id, PLANNER, &lead.planner).await;
    until(lead, "the step check runs", async || {
        running(lead, &checked.id, PLANNER).await
    })
    .await;
    let runtime = session(&lead.db, &lead.planner, &checked.id).await;
    let principal = lead
        .db
        .resolve_task_effective_authority(&lead.planner, None, &checked.id, "task_read", &runtime)
        .await
        .unwrap()
        .principal;
    assert!(
        matches!(
            principal,
            operation_registry::authority::Principal::TaskAgent {
                lead_root_id: None,
                ..
            }
        ),
        "{principal:?}"
    );
    let waiting = reload(&lead.db, &next.id).await;
    for (operation, payload) in [
        ("lead.read", json!({})),
        (
            "lead.subtask",
            json!({"action": "update", "task_id": waiting.id, "version": waiting.version, "description": "rewritten by a step check"}),
        ),
        (
            "lead.subtask",
            json!({"action": "create", "title": "third step"}),
        ),
        (
            "lead.subtask",
            json!({"action": "cancel", "task_id": waiting.id, "version": waiting.version, "reason": "no"}),
        ),
        (
            "lead.subtask",
            json!({"action": "assign", "task_id": waiting.id, "agent_id": lead.planner}),
        ),
        (
            "lead.recover",
            json!({"task_id": waiting.id, "action": {"verb": "cancel"}, "version": waiting.version}),
        ),
        (
            "lead.handoff",
            json!({"task_id": waiting.id, "note": "note"}),
        ),
        (
            "lead.request_input",
            json!({"question": "is this good enough?"}),
        ),
    ] {
        let denied = ops
            .call(
                &checked.id,
                operation,
                payload.clone(),
                &format!("{operation}:{payload}"),
            )
            .await
            .unwrap_err();
        assert!(
            denied.contains("OperationNotInScope") || denied.contains("operation_not_in_scope"),
            "{operation}: {denied}"
        );
    }
    // The command boundary says the same to a caller that got past the
    // registry (a principal resolved before the rule, a direct service call).
    let execution: String = sqlx::query_scalar(
        "SELECT id FROM execution WHERE task_id=? AND role='planner' AND status='running'",
    )
    .bind(&checked.id)
    .fetch_one(lead.db.pool())
    .await
    .unwrap();
    let refused = ops
        .service
        .lead_create_subtask(
            &crate::LeadRun {
                identity_id: lead.planner.clone(),
                execution_id: execution,
                task_id: checked.id.clone(),
                root_id: root.id.clone(),
            },
            "third step".to_owned(),
            None,
            None,
            "direct",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            refused,
            crate::ServiceError::LeadOperationRefused {
                refusal: crate::LeadRefusal::NotLeading,
                ..
            }
        ),
        "{refused:?}"
    );
    assert_eq!(children(lead, &root.id).await.len(), 2);
    let untouched = reload(&lead.db, &next.id).await;
    assert_eq!(untouched.version, waiting.version);
    assert_eq!(untouched.status, "todo");
    assert_eq!(reload(&lead.db, &root.id).await.status, "in_progress");
}

/// Two questions sent at once by one run: one is asked, the other is
/// refused, and there is one comment, one Attention item and one wait. (The
/// comment, the item and the wait are one transaction.)
#[tokio::test]
async fn two_questions_at_once_ask_one() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root, _) = root_with_child(lead, true).await;
    until(lead, "the root plans", async || {
        running(lead, &root.id, PLANNER).await
    })
    .await;
    // The session exists before the two calls race.
    session(&lead.db, &lead.planner, &root.id).await;
    let (first, second) = tokio::join!(
        ops.call(
            &root.id,
            "lead.request_input",
            json!({"question": "A or B?"}),
            "q1",
        ),
        ops.call(
            &root.id,
            "lead.request_input",
            json!({"question": "C or D?"}),
            "q2",
        ),
    );
    let (asked, refused) = match (first, second) {
        (Ok(asked), Err(refused)) | (Err(refused), Ok(asked)) => (asked, refused),
        other => panic!("exactly one question is asked: {other:?}"),
    };
    assert!(refused.contains("lead_input_pending"), "{refused}");
    let questions = comments(lead, &root.id)
        .await
        .into_iter()
        .filter(|comment| comment.worklog_kind.as_deref() == Some("request_input"))
        .collect::<Vec<_>>();
    assert_eq!(questions.len(), 1, "the refused question left no comment");
    assert_eq!(asked["comment_id"], questions[0].id.as_str());
    let items: Vec<String> = sqlx::query_scalar(
        "SELECT dedupe_key FROM attention_projection WHERE recommended_action='answer_lead_input' AND status='open'",
    )
    .fetch_all(lead.db.pool())
    .await
    .unwrap();
    assert_eq!(
        items,
        vec![format!("lead-input:{}:{}", root.id, questions[0].id)]
    );
    let waiting = reload(&lead.db, &root.id).await;
    let metadata: Value = serde_json::from_str(waiting.metadata_json.as_deref().unwrap()).unwrap();
    assert_eq!(
        metadata["awaiting_human_marker_id"],
        questions[0].id.as_str()
    );
}
