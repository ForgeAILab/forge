//! Review budgets, result comments, progress rounds and anchoring.
use super::*;

#[tokio::test]
async fn execution_budget_is_not_refunded_by_a_review_lap() {
    let db = Arc::new(sqlite_db().await);
    let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(16)));
    let (project_id, _, _repo_dir) = seed_project_repo(&db).await;
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    for state in &mut workflow.states {
        state.hooks = api_types::StateHooks::default();
    }
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id=?")
        .bind(serde_json::to_string(&workflow).unwrap())
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let task = seed_task_with_status(&db, &project_id, "in_progress".into()).await;
    seed_budget_spent(&db, &task.id, db::budget::Kind::Execution, 2).await;
    let review = service
        .transition(
            task.id.clone(),
            "review".to_owned(),
            (task.version, Some("submit".to_owned())),
        )
        .await
        .unwrap()
        .task;
    assert_eq!(
        db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
            .await
            .unwrap(),
        2
    );
    let returned = service
        .transition(
            task.id.clone(),
            "in_progress".to_owned(),
            (review.version, Some("owner retry".to_owned())),
        )
        .await
        .unwrap()
        .task;
    assert_eq!(returned.status, "in_progress");
    assert_eq!(
        db::budget::spent(db.pool(), &task.id, db::budget::Kind::Execution.key())
            .await
            .unwrap(),
        2
    );
    assert_eq!(db::budget::remaining(3, 2), 1);
}

#[tokio::test]
async fn review_comments_cascade_replay_posts_one_result_and_repair() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    complete_review_with(&db, &service, &execution,
        "Full report stays in the Review tab.\n{\"result\":\"fail\",\"reason\":\"empty input crashes\",\"findings\":[{\"summary\":\"Empty input crashes\"}]}").await;
    assert_ne!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "review"
    );
    // Re-enter the pre-cascade state without a new Review or entry hooks. This
    // models redelivery after the result/repair writes, so the second call
    // must execute the settled-review reconciliation instead of its status guard.
    sqlx::query("UPDATE task SET status = 'review', blocked_json = NULL, error_annotation = NULL, failed_json = NULL, version = version + 1 WHERE id = ?")
        .bind(&task.id).execute(db.pool()).await.unwrap();
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .unwrap();
    assert_ne!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "review"
    );
    assert_eq!(
        ReviewRepo::list_by_task(&*db, &task.id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db::budget::spent(db.pool(), &task.id, db::budget::Kind::Review.key())
            .await
            .unwrap(),
        1
    );
    let comments = review_comment_contents(&db, &task.id).await;
    let results: Vec<_> = comments
        .iter()
        .filter(|text| text.starts_with("Review failed (attempt"))
        .collect();
    assert_eq!(results.len(), 1, "{comments:?}");
    assert!(results[0].contains("- [ ] F1 New: Empty input crashes"));
    assert!(results[0].contains("Full review: Review tab, attempt 1"));
    assert!(!results[0].contains("Full report stays"));
    let repairs: Vec<_> = comments
        .iter()
        .filter(|text| text.starts_with("Repair round"))
        .collect();
    assert_eq!(repairs.len(), 1, "{comments:?}");
    assert!(repairs[0].contains("review budget 1 of 3"));
    assert!(repairs[0].contains("F1 open; none resolved"));
}

#[tokio::test]
async fn review_comments_park_and_system_comments_are_idempotent_per_review() {
    let (db, service, task, execution, _repo_dir) = seed_admitted_review().await;
    complete_review_with(&db, &service, &execution,
        r#"{"result":"fail","reason":"credentials required","fixable_by":"owner","findings":[{"summary":"Credentials required"}]}"#).await;
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .unwrap();
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|review| review.attempt_number)
        .unwrap();
    // Exercise the actual durable-key path as well as the cascade's terminal guard.
    let (first, second) = tokio::join!(
        service.publish_review_park_comment(&review, "fixable by owner: credentials required"),
        service.publish_review_park_comment(&review, "fixable by owner: credentials required"),
    );
    first.unwrap();
    second.unwrap();
    service
        .publish_review_repair_comment(&review, 1, 2)
        .await
        .unwrap();
    service
        .publish_review_repair_comment(&review, 1, 2)
        .await
        .unwrap();
    let comments = review_comment_contents(&db, &task.id).await;
    assert_eq!(
        comments
            .iter()
            .filter(|text| text.starts_with("Review failed (attempt"))
            .count(),
        1
    );
    assert_eq!(
        comments
            .iter()
            .filter(|text| text.starts_with("Repair round"))
            .count(),
        1
    );
    let parks: Vec<_> = comments
        .iter()
        .filter(|text| text.starts_with("Task parked after review"))
        .collect();
    assert_eq!(parks.len(), 1, "{comments:?}");
    assert!(parks[0].contains("fixable by owner"));
    assert!(parks[0].contains("Open findings: F1"));
    assert!(parks[0].contains("Owner actions:"));
}

#[tokio::test]
async fn review_comments_reviewer_exit_failure_posts_one_result_on_replay() {
    let (db, service, task, execution, _repo_dir) =
        seed_admitted_review_with(r#"{"retry_budgets":{"execution":0,"review":3}}"#).await;
    sqlx::query("UPDATE execution SET status = 'failed', error = 'reviewer crashed', updated_at = ? WHERE id = ?")
        .bind(now_rfc3339())
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .unwrap();
    // A crash after the result comment, before the executor-failure park:
    // the duplicate terminal delivery reconciles the settled Review.
    sqlx::query("UPDATE task SET blocked_json = NULL, error_annotation = NULL, failed_json = NULL WHERE id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .unwrap();
    let comments = review_comment_contents(&db, &task.id).await;
    let results: Vec<_> = comments
        .iter()
        .filter(|text| text.starts_with("Review failed (attempt 1)"))
        .collect();
    assert_eq!(results.len(), 1, "{comments:?}");
    assert!(results[0].contains("reviewer crashed"), "{comments:?}");
}

#[tokio::test]
async fn review_round_progress_granted_and_self_reported_repeat_ignored() {
    let (db, service, task, execution, _dir) =
        seed_admitted_review_with(r#"{"retry_budgets":{"review":2}}"#).await;
    structured_round_previous(&db, &task.id, 3).await;
    // Anchor the new finding so this case continues to exercise round accounting.
    complete_review_with(&db, &service, &execution,
        r#"{"result":"fail","reason":"one left","repeat":true,"findings":[{"summary":"new issue","kind":"regression"}],"resolved":["F1","F2","F3"]}"#).await;
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.status, "in_progress");
    assert!(task.blocked_json.is_none());
    assert_eq!(round_spending(&db, &task.id).await, (2, 1));
    let comments = db::TaskCommentRepo::list_comments(
        &*db,
        &task.id,
        PageRequest {
            cursor: None,
            limit: 100,
            include_total: false,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Asc,
        },
    )
    .await
    .unwrap();
    assert!(comments
        .items
        .iter()
        .any(|comment| comment.content.contains("review_progress budget 1 of 2")));
}

#[tokio::test]
async fn review_round_no_progress_with_new_findings_exhausts_review() {
    let (db, service, task, execution, _dir) =
        seed_admitted_review_with(r#"{"retry_budgets":{"review":2}}"#).await;
    structured_round_previous(&db, &task.id, 1).await;
    // Anchor the new finding so this case continues to exercise round accounting.
    complete_review_with(&db, &service, &execution,
        r#"{"result":"fail","reason":"new issue","findings":[{"summary":"new issue","kind":"regression"}],"resolved":["F1"]}"#).await;
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.status, "review");
    let annotation: Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "review_budget_exhausted");
    assert_eq!(round_spending(&db, &task.id).await, (2, 0));
}

#[tokio::test]
async fn review_round_carried_with_progress_gets_one_more_round() {
    let (db, service, task, execution, _dir) =
        seed_admitted_review_with(r#"{"retry_budgets":{"review":2}}"#).await;
    structured_round_previous(&db, &task.id, 3).await;
    complete_review_with(&db, &service, &execution,
        r#"{"result":"fail","reason":"one left","findings":[{"id":"F1","summary":"issue 1"}],"resolved":["F2","F3"]}"#).await;
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.status, "in_progress");
    assert_eq!(round_spending(&db, &task.id).await, (2, 1));
}

#[tokio::test]
async fn review_round_third_appearance_parks_even_with_progress_without_spending() {
    let (db, service, task, execution, _dir) =
        seed_admitted_review_with(r#"{"retry_budgets":{"review":4}}"#).await;
    structured_round_previous(&db, &task.id, 3).await;
    let current = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.attempt_number == 2)
        .unwrap();
    sqlx::query("UPDATE review SET attempt_number=3 WHERE id=?")
        .bind(&current.id)
        .execute(db.pool())
        .await
        .unwrap();
    ReviewRepo::create(&*db, db::CreateReview {
        id:new_uuid_v4(),task_id:task.id.clone(),execution_id:current.execution_id.clone(),attempt_number:2,status:ReviewStatus::Failed,
        step_results_json:json!({"conformance":{"status":"failed","checks":[],"reason":"two left","assessment":{"result":"fail","reason":"two left","findings":[{"id":"F1","summary":"issue 1"},{"id":"F2","summary":"issue 2"}],"resolved":["F3"]}}}).to_string(),
        started_at:current.started_at.clone(),created_at:current.created_at.clone(),updated_at:current.updated_at.clone(),
    }).await.unwrap();
    let before = round_spending(&db, &task.id).await;
    complete_review_with(&db, &service, &execution,
        r#"{"result":"fail","reason":"one left","findings":[{"id":"F1","summary":"issue 1"}],"resolved":["F2"]}"#).await;
    assert_owner_review_park(&db, &task.id, "repeated finding: F1 issue 1").await;
    assert_eq!(round_spending(&db, &task.id).await, before);
}

#[tokio::test]
async fn review_round_repeat_without_progress_parks_without_spending() {
    let (db, service, task, execution, _dir) =
        seed_admitted_review_with(r#"{"retry_budgets":{"review":4}}"#).await;
    structured_round_previous(&db, &task.id, 1).await;
    let before = round_spending(&db, &task.id).await;
    complete_review_with(&db, &service, &execution,
        r#"{"result":"fail","reason":"still broken","repeat":false,"findings":[{"id":"F1","summary":"issue 1"}]}"#).await;
    assert_owner_review_park(&db, &task.id, "repeated finding: F1 issue 1").await;
    assert_eq!(round_spending(&db, &task.id).await, before);
}

#[tokio::test]
async fn review_round_progress_zero_disables_extra_rounds() {
    let (db, service, task, execution, _dir) =
        seed_admitted_review_with(r#"{"retry_budgets":{"review":2,"review_progress":0}}"#).await;
    structured_round_previous(&db, &task.id, 3).await;
    complete_review_with(&db, &service, &execution,
        r#"{"result":"fail","reason":"one left","findings":[{"id":"F1","summary":"issue 1"}],"resolved":["F2","F3"]}"#).await;
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let annotation: Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "review_budget_exhausted");
    assert_eq!(round_spending(&db, &task.id).await, (2, 0));
}

#[tokio::test]
async fn review_round_autonomous_progress_uses_working_target() {
    let workflow = crate::workflow::default_autonomous_workflow::default_autonomous_workflow();
    let (db, service, task, execution, _dir) =
        seed_admitted_review_with_workflow(r#"{"retry_budgets":{"review":2}}"#, Some(workflow))
            .await;
    structured_round_previous(&db, &task.id, 3).await;
    complete_review_with(&db, &service, &execution,
        r#"{"result":"fail","reason":"one left","findings":[{"id":"F1","summary":"issue 1"}],"resolved":["F2","F3"]}"#).await;
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.status, "working");
    assert_eq!(round_spending(&db, &task.id).await, (2, 1));
}

#[tokio::test]
async fn review_round_repeated_ci_never_parks_as_owner_finding() {
    let (db, service, task, execution, _dir) =
        seed_admitted_review_with(r#"{"retry_budgets":{"review":2}}"#).await;
    seed_previous_review_attempt(&db, &task.id, ReviewStatus::Failed).await;
    let steps = json!({"ci_steps":[{"index":0,"command":"cargo test","exit_code":101,"stderr_tail":"failure","failures":["a"]}]});
    sqlx::query("UPDATE review SET status='failed',step_results_json=? WHERE task_id=?")
        .bind(steps.to_string())
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let (task, target, _) = service
        .review_failure_target(&task, Some(&execution.id))
        .await
        .unwrap();
    assert!(target.is_none());
    let annotation: Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "review_budget_exhausted");
    assert_eq!(round_spending(&db, &task.id).await, (2, 0));
}

#[tokio::test]
async fn review_round_progress_receipt_replays_the_last_grant_without_refund() {
    let (db, service, task, execution, _dir) =
        seed_admitted_review_with(r#"{"retry_budgets":{"review":2,"review_progress":1}}"#).await;
    structured_round_previous(&db, &task.id, 3).await;
    // Crash boundary: the verdict committed, disposition/transition has not.
    sqlx::query("UPDATE review SET status='failed',step_results_json=? WHERE task_id=? AND attempt_number=2")
        .bind(json!({"conformance":{"status":"failed","reason":"one left","checks":[],"assessment":{"result":"fail","reason":"one left","findings":[{"id":"F1","summary":"issue 1"}],"resolved":["F2","F3"]}}}).to_string())
        .bind(&task.id).execute(db.pool()).await.unwrap();
    for _ in 0..2 {
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let (_, target, _) = service
            .review_failure_target(&current, Some(&execution.id))
            .await
            .unwrap();
        assert_eq!(target.as_deref(), Some("in_progress"));
        assert_eq!(round_spending(&db, &task.id).await, (2, 1));
    }
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|r| r.attempt_number)
        .unwrap();
    assert!(db::budget::receipt_recorded(
        db.pool(),
        &task.id,
        "review_progress",
        &format!("progress:{}", review.id)
    )
    .await
    .unwrap());
}

// An owner's review re-run spends no progress round, but it is still bounded
// by the configured limit: `review_progress = 0` disables extra rounds for
// owner re-runs too, and a spent allowance is not refilled by one.
#[tokio::test]
async fn review_round_owner_rerun_never_exceeds_the_progress_limit() {
    for (config, spent) in [
        (r#"{"retry_budgets":{"review":1,"review_progress":0}}"#, 0),
        (r#"{"retry_budgets":{"review":1,"review_progress":1}}"#, 1),
    ] {
        let (db, service, task, execution, _dir) = seed_admitted_review_with(config).await;
        structured_round_previous(&db, &task.id, 3).await;
        let owner = api_types::Actor::user(api_types::UserActionSource::Api);
        let bridge = api_types::TransitionBridge::review_rerun(&owner);
        sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,bridge_kind,bridge_payload,status_epoch) VALUES(?,?,'review','review','user','owner re-run','2000-01-01T00:00:00Z',?,?,99)")
            .bind(new_uuid_v4())
            .bind(&task.id)
            .bind(bridge.bridge_kind.map(api_types::TransitionBridgeKind::as_str))
            .bind(bridge.bridge_payload.map(|payload| payload.to_string()))
            .execute(db.pool())
            .await
            .unwrap();
        if spent > 0 {
            sqlx::query("INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES(?,'review_progress',COALESCE((SELECT window_id FROM task_budget WHERE task_id=? AND kind='review'),'initial'),?)")
                .bind(&task.id)
                .bind(&task.id)
                .bind(spent)
                .execute(db.pool())
                .await
                .unwrap();
        }
        // The owner's re-run verdict committed without spending the review budget.
        sqlx::query("UPDATE review SET status='failed',step_results_json=? WHERE task_id=? AND attempt_number=2")
            .bind(json!({"conformance":{"status":"failed","reason":"one left","checks":[],"assessment":{"result":"fail","reason":"one left","findings":[{"id":"F1","summary":"issue 1"}],"resolved":["F2","F3"]}}}).to_string())
            .bind(&task.id).execute(db.pool()).await.unwrap();
        sqlx::query("UPDATE task_budget SET spent=1 WHERE task_id=? AND kind='review'")
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        let current = TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap();
        let (task, target, _) = service
            .review_failure_target(&current, Some(&execution.id))
            .await
            .unwrap();
        assert!(target.is_none(), "{config}: owner re-run granted a round");
        let annotation: Value =
            serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
        assert_eq!(annotation["type"], "review_budget_exhausted", "{config}");
        assert_eq!(round_spending(&db, &task.id).await, (1, spent), "{config}");
    }
}

#[tokio::test]
async fn review_anchoring_nonblocking_only_parks_without_spending_or_dispatching() {
    for finding in [
        json!({"summary":"naming in untouched code", "where":"src/untouched.rs:42", "requirement_ids":["R99"]}),
        json!({"summary":"later project feature", "kind":"out_of_scope"}),
    ] {
        let (db, service, task, execution, _dir) =
            seed_admitted_review_with(r#"{"retry_budgets":{"review":3}}"#).await;
        structured_round_previous(&db, &task.id, 1).await;
        let before = round_spending(&db, &task.id).await;
        let execution_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id=?")
                .bind(&task.id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        complete_review_with(&db, &service, &execution,
            &json!({"result":"fail","reason":"one remaining nit","findings":[finding],"resolved":["F1"]}).to_string()).await;
        assert_owner_review_park(&db, &task.id, "only non-blocking findings remain").await;
        assert_eq!(round_spending(&db, &task.id).await, before);
        let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id=?")
            .bind(&task.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(
            after, execution_count,
            "a non-blocking-only fail dispatched a coder"
        );
        let comments = review_comment_contents(&db, &task.id).await.join("\n");
        assert!(comments.contains("only non-blocking findings remain"));
        assert!(
            comments.contains("follow-up candidate")
                || comments.contains("outside requirements and Task files")
        );
        let conformance = db::ReviewConformanceRepo::review_conformance(&*db, &execution.id)
            .await
            .unwrap()
            .unwrap();
        assert!(conformance
            .assessment
            .unwrap()
            .findings
            .iter()
            .all(|finding| !finding.requirement_ids.contains(&"R99".to_owned())));
    }
}

#[tokio::test]
async fn review_anchoring_stranded_lead_root_keeps_the_nonblocking_owner_park() {
    let (db, service, task, execution, repo_dir) = seed_admitted_review().await;
    let reply = r#"{"result":"fail","reason":"separate scope","findings":[{"summary":"later project feature","kind":"out_of_scope"}]}"#;
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
    // A settled verdict can be recovered after its completion delivery stopped.
    sqlx::query("UPDATE execution SET status='completed',summary=?,updated_at=? WHERE id=?")
        .bind(reply)
        .bind(&now)
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,task_type,parent_task_id,created_at,updated_at) VALUES(?,?,'Finished step','done','sub_task',?,?,?)")
        .bind(new_uuid_v4()).bind(&task.project_id).bind(&task.id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let lead = crate::test_support::seed_native_agent(&db).await;
    seed_role_assignment(
        &db,
        &task.id,
        crate::workflow::default_roles::PLANNER,
        Some(&lead),
    )
    .await;
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let before = round_spending(&db, &task.id).await;
    let (_, target, reason) = service
        .review_failure_target(&current, Some(&execution.id))
        .await
        .unwrap();
    assert!(
        target.is_none(),
        "non-blocking findings dispatched the lead"
    );
    assert_eq!(reason, "only non-blocking findings remain");
    assert_owner_review_park(&db, &task.id, "only non-blocking findings remain").await;
    assert_eq!(round_spending(&db, &task.id).await, before);
    assert_eq!(
        db::budget::spent(db.pool(), &task.id, "replan")
            .await
            .unwrap(),
        0
    );
}
