//! Tests for subtask updates in lead operations.

use super::*;
/// The failing-first test of stage 4. The planning run on root A may not
/// touch a subtask of root B: a typed scope refusal and no write. The same
/// call on its own subtask that has not started succeeds and the record
/// names the lead and its execution.
#[tokio::test]
async fn a_lead_updates_its_own_subtask_and_never_another_roots() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root_a, a1) = root_with_child(lead, true).await;
    let (root_b, b1) = other_root(lead).await;
    until(lead, "both roots plan", async || {
        running(lead, &root_a.id, PLANNER).await && running(lead, &root_b.id, PLANNER).await
    })
    .await;
    let update = |task: &Task| json!({"action": "update", "task_id": task.id, "version": task.version, "description": "narrowed by the lead"});

    let foreign = reload(&lead.db, &b1.id).await;
    let refused = ops
        .call(&root_a.id, "lead.subtask", update(&foreign), "escape")
        .await
        .unwrap_err();
    assert!(refused.contains("outside_lead_subtree"), "{refused}");
    let after = reload(&lead.db, &b1.id).await;
    assert_eq!(
        (after.description.clone(), after.version),
        (foreign.description.clone(), foreign.version),
        "nothing was written to the other root's subtask"
    );
    assert!(comments(lead, &b1.id).await.is_empty());

    let own = reload(&lead.db, &a1.id).await;
    let updated = ops
        .call(&root_a.id, "lead.subtask", update(&own), "own")
        .await
        .unwrap();
    assert_eq!(updated["task_id"], own.id.as_str(), "{updated}");
    let now = reload(&lead.db, &a1.id).await;
    assert_eq!(now.description.as_deref(), Some("narrowed by the lead"));
    assert!(now.version > own.version);
    let execution: String = sqlx::query_scalar(
        "SELECT id FROM execution WHERE task_id=? AND role='planner' AND status='running'",
    )
    .bind(&root_a.id)
    .fetch_one(lead.db.pool())
    .await
    .unwrap();
    let record = comments(lead, &a1.id).await;
    assert_eq!(record.len(), 1, "{record:?}");
    assert_eq!(record[0].author_type, db::CommentAuthorType::Agent);
    assert_eq!(record[0].author_id.as_deref(), Some(lead.planner.as_str()));
    assert_eq!(record[0].execution_id.as_deref(), Some(execution.as_str()));
    assert_eq!(record[0].role.as_deref(), Some(PLANNER));

    // The same refusal for every operation that names a target, whether the
    // target is the other root's subtask, the other root, or no Task at all.
    let foreign = reload(&lead.db, &b1.id).await;
    let escapes = [
        ("lead.read", json!({"task_id": foreign.id})),
        (
            "lead.subtask",
            json!({"action": "cancel", "task_id": foreign.id, "version": foreign.version, "reason": "not mine"}),
        ),
        (
            "lead.subtask",
            json!({"action": "assign", "task_id": foreign.id, "agent_id": lead.coder}),
        ),
        (
            "lead.subtask",
            json!({"action": "reorder", "ordered_task_ids": [foreign.id]}),
        ),
        (
            "lead.subtask",
            json!({"action": "update", "task_id": root_b.id, "version": 1, "title": "mine now"}),
        ),
        (
            "lead.recover",
            json!({"task_id": foreign.id, "action": {"verb": "retry"}, "version": foreign.version}),
        ),
        (
            "lead.recover",
            json!({"task_id": "no-such-task", "action": {"verb": "cancel"}, "version": 1}),
        ),
        (
            "lead.handoff",
            json!({"task_id": foreign.id, "note": "a note for someone else's worker"}),
        ),
        (
            "lead.handoff",
            json!({"task_id": root_b.id, "note": "a note on someone else's root"}),
        ),
    ];
    for (index, (operation, payload)) in escapes.into_iter().enumerate() {
        let refused = ops
            .call(
                &root_a.id,
                operation,
                payload.clone(),
                &format!("escape-{index}"),
            )
            .await
            .unwrap_err();
        assert!(
            refused.contains("outside_lead_subtree"),
            "{operation} {payload}: {refused}"
        );
    }
    let after = reload(&lead.db, &b1.id).await;
    assert_eq!(
        (after.status.as_str(), after.version),
        ("todo", foreign.version)
    );
    assert!(comments(lead, &b1.id).await.is_empty());
    assert!(comments(lead, &root_b.id).await.is_empty());
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_role_assignment WHERE task_id=?")
        .bind(&b1.id)
        .fetch_one(lead.db.pool())
        .await
        .unwrap();
    assert_eq!(rows, 0);
    // The root itself is never a subtask target of its own lead.
    let own_root = reload(&lead.db, &root_a.id).await;
    for (operation, payload) in [
        (
            "lead.subtask",
            json!({"action": "cancel", "task_id": own_root.id, "version": own_root.version, "reason": "give up"}),
        ),
        (
            "lead.recover",
            json!({"task_id": own_root.id, "action": {"verb": "cancel"}, "version": own_root.version}),
        ),
        (
            "lead.handoff",
            json!({"task_id": own_root.id, "note": "to myself"}),
        ),
    ] {
        let refused = ops
            .call(&root_a.id, operation, payload, &format!("root-{operation}"))
            .await
            .unwrap_err();
        assert!(refused.contains("wrong_target"), "{operation}: {refused}");
    }
    assert_eq!(reload(&lead.db, &root_a.id).await.status, "planning");
}

/// A planner run that leads nothing (an ordinary Task in its planning state)
/// is not admitted to any lead operation, and the server's principal says so.
#[tokio::test]
async fn an_ordinary_planner_run_has_no_lead_operation() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let plain = seed_task(&lead.db, &lead.project, "plain", "todo", 1).await;
    assign_role(&lead.db, &plain.id, CODER, &lead.coder).await;
    assign_role(&lead.db, &plain.id, PLANNER, &lead.planner).await;
    let (root, _) = root_with_child(lead, true).await;
    until(lead, "both planners run", async || {
        running(lead, &plain.id, PLANNER).await && running(lead, &root.id, PLANNER).await
    })
    .await;
    for (operation, payload) in [
        ("lead.read", json!({})),
        (
            "lead.subtask",
            json!({"action": "create", "title": "smuggled"}),
        ),
        (
            "lead.recover",
            json!({"task_id": root.id, "action": {"verb": "cancel"}, "version": 1}),
        ),
        ("lead.handoff", json!({"task_id": root.id, "note": "note"})),
        ("lead.request_input", json!({"question": "may I?"})),
    ] {
        let denied = ops
            .call(&plain.id, operation, payload, operation)
            .await
            .unwrap_err();
        assert!(
            denied.contains("OperationNotInScope") || denied.contains("operation_not_in_scope"),
            "{operation}: {denied}"
        );
    }
    assert_eq!(children(lead, &plain.id).await.len(), 0);
    let principal = |task: String| async move {
        let runtime = session(&lead.db, &lead.planner, &task).await;
        lead.db
            .resolve_task_effective_authority(&lead.planner, None, &task, "task_read", &runtime)
            .await
            .unwrap()
            .principal
    };
    assert!(matches!(
        principal(plain.id.clone()).await,
        operation_registry::authority::Principal::TaskAgent {
            lead_root_id: None,
            ..
        }
    ));
    assert!(matches!(
        principal(root.id.clone()).await,
        operation_registry::authority::Principal::TaskAgent { lead_root_id: Some(id), .. } if id == root.id
    ));
    // The lead's own run keeps the lead operations; removing the lead takes
    // them away from the run that is still going.
    assert!(ops
        .call(&root.id, "lead.read", json!({}), "read")
        .await
        .is_ok());
    sqlx::query("DELETE FROM task_role_assignment WHERE task_id=? AND role_name='planner'")
        .bind(&root.id)
        .execute(lead.db.pool())
        .await
        .unwrap();
    assert!(ops
        .call(&root.id, "lead.read", json!({}), "read-2")
        .await
        .is_err());
}

/// What a lead may do to a subtask depends on whether it started. A started
/// one is recovered, reassigned or given a note; one that has not started is
/// updated, reordered or cancelled. Creating appends under the root, once per
/// key, and is bounded per run.
#[tokio::test]
async fn a_started_subtask_is_recovered_not_rewritten_and_creation_is_bounded() {
    started_subtask_and_bounded_creation(false).await;
}

/// The same lead with a slow launch, in a fixed order: the run's launch is
/// held before its final authority check (released at the latest before the
/// sixth extra create), and a write of the root lands in each WorkspaceLease
/// reissue attempt it makes. Lead operations issued before the run reached
/// its executor moved the root under that launch; both reissue attempts then
/// lost to root writes, the run failed before it started ("version
/// conflict") and the sixth create was refused `authority_revoked`. The
/// lead's operations wait for its run, so they never meet the launch.
#[tokio::test]
async fn a_slow_launch_of_the_leads_run_keeps_its_operations() {
    started_subtask_and_bounded_creation(true).await;
}

/// A lead cannot make itself a worker (its runs read the repository; a
/// worker's run writes it), on a subtask, as the root's default worker or on
/// a subtask it creates.
#[tokio::test]
async fn a_lead_never_assigns_itself_as_a_worker() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root, child) = root_with_child(lead, true).await;
    until(lead, "the root plans", async || {
        running(lead, &root.id, PLANNER).await
    })
    .await;
    for (payload, key) in [
        (
            json!({"action": "assign", "task_id": child.id, "agent_id": lead.planner}),
            "self-child",
        ),
        (
            json!({"action": "assign", "agent_id": lead.planner}),
            "self-default",
        ),
        (
            json!({"action": "create", "title": "mine", "assignee_id": lead.planner}),
            "self-create",
        ),
    ] {
        let refused = ops
            .call(&root.id, "lead.subtask", payload, key)
            .await
            .unwrap_err();
        assert!(refused.contains("lead_self_assignment"), "{key}: {refused}");
    }
    let coders: Vec<String> = sqlx::query_scalar(
        "SELECT a.assignee_id FROM task_role_assignment a JOIN task t ON t.id=a.task_id
         WHERE a.role_name='coder' AND (t.id=? OR t.parent_task_id=?)",
    )
    .bind(&root.id)
    .bind(&root.id)
    .fetch_all(lead.db.pool())
    .await
    .unwrap();
    assert!(!coders.contains(&lead.planner), "{coders:?}");
    assert_eq!(children(lead, &root.id).await.len(), 1);
}

/// "Started" is any run on the subtask that is not the step check, whatever
/// the Project's workflow calls its implementation role.
#[tokio::test]
async fn a_subtask_started_under_any_role_name_is_not_rewritten() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root, child) = root_with_child(lead, true).await;
    seed_completed_coder_execution(&lead.db, &child.id).await;
    sqlx::query("UPDATE execution SET role='builder' WHERE task_id=?")
        .bind(&child.id)
        .execute(lead.db.pool())
        .await
        .unwrap();
    until(lead, "the root plans", async || {
        running(lead, &root.id, PLANNER).await
    })
    .await;
    let child = reload(&lead.db, &child.id).await;
    for (payload, key) in [
        (
            json!({"action": "update", "task_id": child.id, "version": child.version, "title": "rewritten"}),
            "update",
        ),
        (
            json!({"action": "cancel", "task_id": child.id, "version": child.version, "reason": "drop"}),
            "cancel",
        ),
    ] {
        let refused = ops
            .call(&root.id, "lead.subtask", payload, key)
            .await
            .unwrap_err();
        assert!(
            refused.contains("subtask_started") || refused.contains("unavailable"),
            "{key}: {refused}"
        );
    }
    let after = reload(&lead.db, &child.id).await;
    assert_eq!((after.title, after.status), (child.title, child.status));
}

/// A question whose root was deleted can still be closed by its owner: the
/// item is resolved and nothing else is written.
#[tokio::test]
async fn a_question_of_a_deleted_root_is_resolved_without_an_answer_target() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root, _) = root_with_child(lead, true).await;
    until(lead, "the root plans", async || {
        running(lead, &root.id, PLANNER).await
    })
    .await;
    ops.call(
        &root.id,
        "lead.request_input",
        json!({"question": "A or B?"}),
        "q1",
    )
    .await
    .unwrap();
    let (item, version): (String, i64) = sqlx::query_as(
        "SELECT id, version FROM attention_projection WHERE recommended_action='answer_lead_input'",
    )
    .fetch_one(lead.db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE task SET deleted_at=? WHERE id=?")
        .bind(now_rfc3339())
        .bind(&root.id)
        .execute(lead.db.pool())
        .await
        .unwrap();
    let owner: String = sqlx::query_scalar("SELECT owner_id FROM project WHERE id=?")
        .bind(&lead.project)
        .fetch_one(lead.db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT OR IGNORE INTO user (id, email, password_hash, created_at, updated_at) VALUES (?, 'owner@example.test', 'unused', ?, ?)")
        .bind(&owner)
        .bind(now_rfc3339())
        .bind(now_rfc3339())
        .execute(lead.db.pool())
        .await
        .unwrap();
    let attention = crate::attention_service::AttentionService::new(
        Arc::clone(&lead.db),
        crate::test_support::noop_wake_turns(),
    );
    let resolved = attention
        .resolve_with_answer(&owner, &item, version, Some("B"))
        .await
        .unwrap();
    assert_eq!(resolved.status, "resolved");
}

/// The recover bound counts recoveries, not keys: the call's key is the
/// lead's own, and sending the same key with each new version of the subtask
/// must not make the second and third recovery free.
#[tokio::test]
async fn a_reused_key_does_not_make_a_recovery_free() {
    let ops = ops_fixture(false).await;
    let lead = &ops.lead;
    let (root, child, _) = root_with_parked_child(lead, true).await;
    until(lead, "the root plans", async || {
        running(lead, &root.id, PLANNER).await
    })
    .await;
    let bound = db::budget::lead_recover_key(&child.id);
    for round in 1..=2_i64 {
        let parked = reload(&lead.db, &child.id).await;
        ops.call(
            &root.id,
            "lead.recover",
            json!({"task_id": child.id, "action": {"verb": "retry"}, "version": parked.version}),
            "same-key",
        )
        .await
        .unwrap();
        assert_eq!(spent(lead, &root.id, &bound).await, round);
        // The subtask is parked again by the same failure.
        TaskRepo::mutate_metadata(
            &*lead.db,
            &child.id,
            None,
            vec![db::TaskMetadataMutation::Remove {
                key: crate::deferred_dispatch::QUEUED_RECOVERY_KEY.to_owned(),
            }],
            &now_rfc3339(),
        )
        .await
        .unwrap();
        super::root_replan::park_child(lead, &child, "the build fails again").await;
        assert!(
            crate::task_hierarchy::subtask_decision_park(&reload(&lead.db, &child.id).await)
                .is_some(),
            "the fixture parks the subtask again"
        );
    }
    let parked = reload(&lead.db, &child.id).await;
    let refused = ops
        .call(
            &root.id,
            "lead.recover",
            json!({"task_id": child.id, "action": {"verb": "retry"}, "version": parked.version}),
            "same-key",
        )
        .await
        .unwrap_err();
    assert!(refused.contains("lead_limit_reached"), "{refused}");
    assert_eq!(spent(lead, &root.id, &bound).await, 2);
}
