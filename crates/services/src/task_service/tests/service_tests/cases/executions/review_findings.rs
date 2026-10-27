//! Review finding routing, owner intervention and deferred follow-ups.
use super::*;

#[tokio::test]
async fn review_finding_routing_owner_parks_without_spending_budget() {
    let (db, service, task, execution, _repo_dir) =
        seed_admitted_review_with(r#"{"retry_budgets":{"execution":3,"review":1}}"#).await;
    let before = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap();
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Forge linked_documents is empty","fixable_by":"owner"}"#,
    )
    .await;
    assert_owner_review_park(
        &db,
        &task.id,
        "fixable by owner: Forge linked_documents is empty",
    )
    .await;
    let after = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap();
    assert_eq!(
        before.len(),
        after.len(),
        "parking creates no rejection transition"
    );
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        json!(
            db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
                .await
                .unwrap()
        ),
        0
    );
    let version = current.version;
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .unwrap();
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap()
            .version,
        version,
        "duplicate completion leaves the owner park unchanged"
    );
}

#[tokio::test]
async fn review_finding_routing_repeat_after_failed_attempt_parks() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    seed_previous_review_attempt(&db, &task.id, ReviewStatus::Failed).await;
    let before = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap();
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Cross-platform measurements remain missing","repeat":true}"#,
    )
    .await;
    assert_owner_review_park(
        &db,
        &task.id,
        "repeated finding: Cross-platform measurements remain missing",
    )
    .await;
    let after = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap();
    assert_eq!(
        before.len(),
        after.len(),
        "repeat park spends no review budget"
    );
    assert_eq!(
        crate::task_diagnostics::audit_gate_rejections_since_boundary(
            &after,
            crate::workflow::default_states::REVIEW,
        ),
        1,
        "only the preceding failure spent budget"
    );
}

#[tokio::test]
async fn review_finding_routing_repeat_requires_previous_reviewer_finding() {
    for details in [
        json!({"auditor":{"verdict":"fail", "reason":"reviewer crashed"}}),
        json!({"ci_steps":[{"exit_code":1}]}),
        json!({"conformance":{"status":"unverified", "contract":null, "checks":[], "reason":"no result"}}),
        json!({"conformance":{"status":"failed", "contract":null,
            "checks":[{"check_id":"ci", "command":"false", "exit_code":1, "output":"failed"}],
            "reason":"CI failed", "assessment":{"result":"fail", "reason":"finding"}}}),
    ] {
        let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
        seed_previous_review_attempt(&db, &task.id, ReviewStatus::Failed).await;
        sqlx::query(
            "UPDATE review SET step_results_json = ? WHERE task_id = ? AND attempt_number = 1",
        )
        .bind(details.to_string())
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
        complete_review_with(
            &db,
            &service,
            &execution,
            r#"{"result":"fail","reason":"Null input crashes","repeat":true}"#,
        )
        .await;
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, crate::workflow::default_states::IN_PROGRESS);
        assert!(current.blocked_json.is_none());
    }
}

#[tokio::test]
async fn review_finding_routing_first_attempt_repeat_returns_to_coder() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Null input still crashes","repeat":true}"#,
    )
    .await;
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, crate::workflow::default_states::IN_PROGRESS);
    assert!(current.blocked_json.is_none());
    assert!(current.error_annotation.is_none());
    let transitions = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap();
    assert_eq!(
        crate::task_diagnostics::audit_gate_rejections_since_boundary(
            &transitions,
            crate::workflow::default_states::REVIEW,
        ),
        1
    );
}

#[tokio::test]
async fn review_finding_routing_repeat_after_cancelled_attempt_returns_to_coder() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    seed_previous_review_attempt(&db, &task.id, ReviewStatus::Cancelled).await;
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Null input crashes","repeat":true}"#,
    )
    .await;
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, crate::workflow::default_states::IN_PROGRESS);
    assert!(current.blocked_json.is_none());
}

#[tokio::test]
async fn review_finding_routing_legacy_fail_returns_to_coder() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Null input crashes"}"#,
    )
    .await;
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, crate::workflow::default_states::IN_PROGRESS);
    assert!(current.blocked_json.is_none());
}

#[tokio::test]
async fn review_finding_routing_forge_checks_override_owner_and_repeat() {
    for checks in [
        r#"{"review":{"setup_steps":["exit 1"]},"retry_budgets":{"review":3}}"#,
        r#"{"review":{"ci_steps":["exit 1"]},"retry_budgets":{"review":3}}"#,
    ] {
        let (db, service, task, execution, _repo_dir) = seed_admitted_review_with(checks).await;
        seed_previous_review_attempt(&db, &task.id, ReviewStatus::Failed).await;
        complete_review_with(&db, &service, &execution,
            r#"{"result":"fail","reason":"Owner needs external credentials","fixable_by":"owner","repeat":true}"#,
        ).await;
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.status, crate::workflow::default_states::IN_PROGRESS);
        assert!(current.blocked_json.is_none());
    }
}

#[tokio::test]
async fn review_finding_routing_settled_owner_failure_reconciles_to_park() {
    let (db, service, task, execution, repo_dir) = seed_admitted_review().await;
    let reply = r#"{"result":"fail","reason":"Hardware measurements needed","fixable_by":"owner"}"#;
    let conformance = ::review::contract::evaluate(&db, &execution.id, repo_dir.path(), reply)
        .await
        .unwrap();
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap()
        .remove(0);
    let now = now_rfc3339();
    ReviewRepo::update_status(
        &*db,
        &review.id,
        ReviewStatus::Failed,
        json!({"ci_steps":[], "conformance":conformance}).to_string(),
        Some(now.clone()),
        &now,
    )
    .await
    .unwrap();
    complete_review_with(&db, &service, &execution, reply).await;
    assert_owner_review_park(
        &db,
        &task.id,
        "fixable by owner: Hardware measurements needed",
    )
    .await;
}

#[tokio::test]
async fn review_finding_routing_owner_manual_pass_clears_the_park() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Hardware measurements needed","fixable_by":"owner"}"#,
    )
    .await;
    let recovered = service
        .test_apply_action(
            task.id.clone(),
            api_types::TaskAction::Approve {
                reason: None,
                override_checks: Some(false),
            },
            Some("Owner verified the hardware measurements".to_owned()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(recovered.status, crate::workflow::default_states::MERGING);
    assert!(recovered.review_passed_at.is_some());
    assert!(recovered.blocked_json.is_none() && recovered.error_annotation.is_none());
    assert_eq!(
        ReviewRepo::list_by_task(&*db, &task.id)
            .await
            .unwrap()
            .last()
            .unwrap()
            .status,
        ReviewStatus::Passed
    );
}

#[tokio::test]
async fn review_finding_routing_defer_follow_up_is_charter_dispatchable() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Owner must collect measurements","fixable_by":"owner"}"#,
    )
    .await;
    let now = now_rfc3339();
    let owner = new_uuid_v4();
    let charter = new_uuid_v4();
    let revision = new_uuid_v4();
    sqlx::query("INSERT INTO user (id, email, password_hash, created_at, updated_at) VALUES (?, ?, 'unused', ?, ?)")
        .bind(&owner).bind(format!("{owner}@example.com")).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE project SET owner_id = ? WHERE id = ?")
        .bind(&owner)
        .bind(&task.project_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO project_charter (id, account_id, project_id, project_mode, maturity, lifecycle, created_at, updated_at)
                 VALUES (?, ?, ?, 'compact', 'prototype', 'attached', ?, ?)")
        .bind(&charter).bind(&owner).bind(&task.project_id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO project_charter_revision (id, charter_id, revision, lifecycle, schema_version, render_version,
                 content_json, rendered_view, author_type, source_refs_json, content_digest, rendered_digest, created_at)
                 VALUES (?, ?, 1, 'approved', 'forge.project-charter/v1', 'forge.project-charter-render/v1', '{}', '# Charter', 'user', '[]', 'content', 'render', ?)")
        .bind(&revision).bind(&charter).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE project_charter SET current_approved_revision_id = ? WHERE id = ?")
        .bind(&revision)
        .bind(&charter)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE project SET current_charter_id = ?, current_charter_revision_id = ?,
                 current_charter_version = 1, charter_status = 'charter_backed', charter_setup_required = 0,
                 version = version + 1 WHERE id = ?")
        .bind(&charter).bind(&revision).bind(&task.project_id).execute(db.pool()).await.unwrap();
    let project = ProjectRepo::get_by_id(&*db, &task.project_id)
        .await
        .unwrap()
        .unwrap();
    let governance = service
        .prepare_task_governance(&project, "task", None)
        .await
        .unwrap()
        .unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    service
        .insert_task_governance(&mut tx, &task.id, &project.id, governance, &now)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    service
        .test_apply_action(
            task.id.clone(),
            api_types::TaskAction::Approve {
                reason: None,
                override_checks: Some(false),
            },
            Some("Schedule the measurements separately".to_owned()),
            None,
        )
        .await
        .unwrap();
    let follow_ups = TaskRepo::list_by_project_with_metadata_key(&*db, &project.id, "follow_up_of")
        .await
        .unwrap();
    assert_eq!(follow_ups.len(), 1);
    let follow_up = &follow_ups[0];
    service
        .ensure_task_runnable(follow_up)
        .await
        .expect("follow-up passes normal Charter dispatch admission");
    let current = ProjectRepo::get_by_id(&*db, &project.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.project_work_epoch, project.project_work_epoch + 1);
    let governance: (String, i64) = sqlx::query_as(
        "SELECT charter_revision_id, runnable FROM project_task_governance WHERE task_id = ?",
    )
    .bind(&follow_up.id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(governance, (revision, 1));
}

#[tokio::test]
async fn review_finding_routing_defer_creates_linked_backlog_and_passes_review() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Required cross-platform measurements","fixable_by":"owner"}"#,
    )
    .await;
    let before = ReviewRepo::list_by_task(&*db, &task.id).await.unwrap();
    for reason in [None, Some("  \n ".to_owned())] {
        let error = service
            .test_apply_action(
                task.id.clone(),
                api_types::TaskAction::Approve {
                    reason: None,
                    override_checks: Some(false),
                },
                reason,
                None,
            )
            .await
            .expect_err("defer requires reason");
        assert!(error.to_string().contains("reason"));
    }
    let mut events = service.event_bus.subscribe();
    let recovered = service
        .test_apply_action(
            task.id.clone(),
            api_types::TaskAction::Approve {
                reason: None,
                override_checks: Some(false),
            },
            Some("macOS/Windows runs need a human".to_owned()),
            None,
        )
        .await
        .expect("defer succeeds");
    assert_eq!(recovered.status, crate::workflow::default_states::MERGING);
    assert!(recovered.review_passed_at.is_some());
    assert!(recovered.blocked_json.is_none() && recovered.error_annotation.is_none());
    let follow_ups =
        TaskRepo::list_by_project_with_metadata_key(&*db, &task.project_id, "follow_up_of")
            .await
            .unwrap();
    assert_eq!(follow_ups.len(), 1);
    let follow_up = &follow_ups[0];
    assert_eq!(follow_up.project_id, task.project_id);
    assert_eq!(follow_up.status, "backlog");
    assert_eq!(
        follow_up.title,
        format!(
            "Follow-up: {} — Required cross-platform measurements",
            task.title
        )
    );
    let metadata: Value =
        serde_json::from_str(follow_up.metadata_json.as_deref().unwrap()).unwrap();
    assert_eq!(metadata["follow_up_of"], task.id);
    let description = follow_up.description.as_deref().unwrap();
    for expected in [
        task.id.as_str(),
        "Required cross-platform measurements",
        "macOS/Windows runs need a human",
    ] {
        assert!(description.contains(expected));
    }
    let reviews = ReviewRepo::list_by_task(&*db, &task.id).await.unwrap();
    assert_eq!(reviews.len(), 2);
    assert_eq!(reviews[0], before[0], "failed assessment remains intact");
    assert_eq!(reviews[1].status, ReviewStatus::Passed);
    let details: Value = serde_json::from_str(&reviews[1].step_results_json).unwrap();
    assert_eq!(details["manual_override"]["action"], "approve");
    let pass_reason = details["manual_override"]["reason"].as_str().unwrap();
    assert!(pass_reason.contains(&follow_up.id) && pass_reason.contains(&follow_up.title));
    assert!(pass_reason.contains("macOS/Windows runs need a human"));
    let transitions = TransitionLogRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap();
    assert_eq!(
        crate::task_diagnostics::audit_gate_rejections_since_boundary(
            &transitions,
            crate::workflow::default_states::REVIEW,
        ),
        0
    );
    let mut created = false;
    let mut updated = false;
    while let Ok(event) = events.try_recv() {
        created |= event.event_type == "task.created" && event.entity_id == follow_up.id;
        updated |= event.event_type == "task.updated" && event.entity_id == task.id;
    }
    assert!(
        created && updated,
        "created/updated events follow the atomic commit"
    );
    assert!(service
        .test_apply_action(
            task.id.clone(),
            api_types::TaskAction::Approve {
                reason: None,
                override_checks: Some(false)
            },
            Some("duplicate defer".to_owned()),
            None
        )
        .await
        .is_err());
    assert_eq!(
        TaskRepo::list_by_project_with_metadata_key(&*db, &task.project_id, "follow_up_of")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn review_finding_routing_defer_uses_workflow_backlog_and_bounds_unicode_title() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    let first_line = "测".repeat(100);
    let finding = format!("{first_line}\nSecond line of the parked finding");
    complete_review_with(
        &db,
        &service,
        &execution,
        &json!({
            "result": "fail", "reason": finding, "fixable_by": "owner",
        })
        .to_string(),
    )
    .await;
    let project = ProjectRepo::get_by_id(&*db, &task.project_id)
        .await
        .unwrap()
        .unwrap();
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    for state in &mut workflow.states {
        if state.name == "backlog" {
            state.name = "waiting".to_owned();
        }
        for trigger in state.triggers.values_mut() {
            if trigger.to == "backlog" {
                trigger.to = "waiting".to_owned();
            }
        }
    }
    sqlx::query("UPDATE project SET workflow_definition = ?, version = version + 1 WHERE id = ?")
        .bind(serde_json::to_string(&workflow).unwrap())
        .bind(&project.id)
        .execute(db.pool())
        .await
        .unwrap();
    service
        .test_apply_action(
            task.id.clone(),
            api_types::TaskAction::Approve {
                reason: None,
                override_checks: Some(false),
            },
            Some("Collect hardware evidence separately".to_owned()),
            None,
        )
        .await
        .unwrap();
    let follow_ups =
        TaskRepo::list_by_project_with_metadata_key(&*db, &task.project_id, "follow_up_of")
            .await
            .unwrap();
    assert_eq!(follow_ups.len(), 1);
    assert_eq!(follow_ups[0].status, "waiting");
    assert_eq!(
        follow_ups[0].title,
        format!("Follow-up: {} — {}…", task.title, "测".repeat(80))
    );
    assert!(follow_ups[0]
        .description
        .as_deref()
        .unwrap()
        .contains(&finding));
}

#[tokio::test]
async fn review_finding_routing_defer_rolls_back_follow_up_when_manual_pass_fails() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    complete_review_with(
        &db,
        &service,
        &execution,
        r#"{"result":"fail","reason":"Hardware measurements needed","fixable_by":"owner"}"#,
    )
    .await;
    let before = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    // Invalidate the candidate so the authority check fails after follow-up insertion.
    sqlx::query("UPDATE execution SET status = 'failed' WHERE id = ?")
        .bind(execution.parent_execution_id.as_deref().unwrap())
        .execute(db.pool())
        .await
        .unwrap();
    let mut events = service.event_bus.subscribe();
    let annotation: api_types::TaskBlockingAnnotation =
        serde_json::from_str(before.error_annotation.as_deref().unwrap()).unwrap();
    service
        .recover_manual_review_pass(
            before.clone(),
            "human follow-up required".to_owned(),
            Some(&annotation),
            api_types::TaskAction::Approve {
                override_checks: Some(false),
                reason: Some("human follow-up required".to_owned()),
            },
        )
        .await
        .expect_err(
            "manual-pass admission must roll back an inserted follow-up for an invalid candidate",
        );

    service
        .test_apply_action(
            task.id.clone(),
            api_types::TaskAction::Approve {
                reason: None,
                override_checks: Some(false),
            },
            Some("human follow-up required".to_owned()),
            None,
        )
        .await
        .expect_err("invalid candidate rejects the manual pass");
    assert!(
        TaskRepo::list_by_project_with_metadata_key(&*db, &task.project_id, "follow_up_of")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    let reviews = ReviewRepo::list_by_task(&*db, &task.id).await.unwrap();
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Failed);
    assert!(
        events.try_recv().is_err(),
        "rolled-back mutations publish no events"
    );
}
