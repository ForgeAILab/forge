//! Review metadata, candidate binding and settlement authority.
use super::*;

#[test]
fn review_details_accept_forge_metadata_and_refuse_unknown_fields() {
    let mut review = db::Review {
        id: "review-metadata".into(),
        task_id: "task".into(),
        execution_id: "candidate".into(),
        reviewer_execution_id: None,
        auditor_execution_id: None,
        attempt_number: 1,
        status: ReviewStatus::Passed,
        step_results_json: String::new(),
        started_at: String::new(),
        finished_at: None,
        created_at: String::new(),
        updated_at: String::new(),
    };
    for metadata in [
        serde_json::json!({"ci_steps":[],"manual_override":{"reason":"checked"}}),
        serde_json::json!({"ci_steps":[],"carry_hook_step_id":"step","carry_hook_index":1}),
        serde_json::json!({"ci_steps":[],"manual_override":{},"carry_hook_step_id":"step","carry_hook_index":0}),
    ] {
        review.step_results_json = metadata.to_string();
        assert_eq!(
            crate::task_service::strict_review_details(&review).unwrap(),
            metadata
        );
    }
    for invalid in [
        serde_json::json!({"unknown_forge_field":{}}),
        serde_json::json!({"manual_override":false}),
        serde_json::json!({"carry_hook_step_id":12}),
        serde_json::json!({"carry_hook_index":-1}),
    ] {
        review.step_results_json = invalid.to_string();
        assert!(
            crate::task_service::strict_review_details(&review).is_err(),
            "{invalid}"
        );
    }
}

#[tokio::test]
async fn settled_reviewer_outcome_reconciles_a_missed_task_cascade() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    let now = now_rfc3339();
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("review completed".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("reviewer execution creates");
    let review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review creates");
    ReviewRepo::update_status(
        &*db,
        &review.id,
        ReviewStatus::Failed,
        json!({ "ci_steps": [], "auditor": { "verdict": "fail" } }).to_string(),
        Some(now.clone()),
        &now,
    )
    .await
    .expect("review outcome commits");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("missed task cascade reconciles");

    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, crate::workflow::default_states::IN_PROGRESS);
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Failed);
}

#[tokio::test]
async fn duplicate_parent_bound_reviewer_delivery_reconciles_without_new_review_attempt() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    let now = now_rfc3339();
    let candidate = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::CODER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("candidate completed".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("candidate execution creates");
    let reviewer = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: Some(candidate.id.clone()),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("review completed".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("reviewer execution creates");
    let review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review creates");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&reviewer.id)
        .bind(&review.id)
        .execute(db.pool())
        .await
        .expect("reviewer attempt binding records");
    ReviewRepo::update_status(
        &*db,
        &review.id,
        ReviewStatus::Passed,
        json!({ "ci_steps": [], "auditor": { "verdict": "pass" } }).to_string(),
        Some(now.clone()),
        &now,
    )
    .await
    .expect("review outcome commits");

    service
        .maybe_cascade_executor_completion(&reviewer.id)
        .await
        .expect("parent-bound settled review reconciles");
    service
        .maybe_cascade_executor_completion(&reviewer.id)
        .await
        .expect("duplicate parent-bound delivery is idempotent");

    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1, "duplicate delivery created a new attempt");
    assert_eq!(reviews[0].id, review.id);
    assert_eq!(reviews[0].execution_id, candidate.id);
}

#[tokio::test]
async fn old_reviewer_cannot_settle_newer_review_sharing_its_candidate() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    let now = now_rfc3339();
    let agent_id = seed_agent(&db).await;
    let candidate = seed_completed_coder_execution(&db, &task, &agent_id, None).await;

    let old_reviewer = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: Some(candidate.id.clone()),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("old reviewer".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("old reviewer creates");
    let new_reviewer = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: None,
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some(now.clone()),
            parent_execution_id: Some(candidate.id.clone()),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("new reviewer".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("new reviewer creates");
    let first_review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("first review creates");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&old_reviewer.id)
        .bind(&first_review.id)
        .execute(db.pool())
        .await
        .expect("first review binding records");
    let second_review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 2,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("second review creates");
    sqlx::query("UPDATE review SET reviewer_execution_id = ? WHERE id = ?")
        .bind(&new_reviewer.id)
        .bind(&second_review.id)
        .execute(db.pool())
        .await
        .expect("second review binding records");

    service
        .maybe_cascade_executor_completion(&old_reviewer.id)
        .await
        .expect("superseded reviewer delivery is ignored");

    let current_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(current_task.status, crate::workflow::default_states::REVIEW);
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews reload");
    assert_eq!(reviews.len(), 2);
    assert!(reviews
        .iter()
        .all(|review| review.status == ReviewStatus::Running));
}

#[tokio::test]
async fn unbound_legacy_reviewer_cannot_settle_a_newer_review() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    let candidate = seed_completed_coder_execution(&db, &task, &agent_id, None).await;
    let review_started_at = "2026-01-01T00:00:00Z";
    let review = ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: candidate.id.clone(),
            attempt_number: 2,
            status: ReviewStatus::Running,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: review_started_at.to_owned(),
            created_at: review_started_at.to_owned(),
            updated_at: review_started_at.to_owned(),
        },
    )
    .await
    .expect("new review creates");

    // This reviewer has no direct Review binding and no candidate parent. Its
    // later timestamp is deliberately chosen so timestamp-only legacy repair
    // would mistake it for the current attempt.
    let reviewer = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id),
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: Some("system:executor".to_owned()),
            resume_policy: None,
            stopped_at: Some("2026-01-01T00:00:02Z".to_owned()),
            parent_execution_id: None,
            agent_session_id: Some("legacy-reviewer-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: Some("legacy reviewer completion".to_owned()),
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: "2026-01-01T00:00:01Z".to_owned(),
            updated_at: "2026-01-01T00:00:02Z".to_owned(),
        },
    )
    .await
    .expect("legacy reviewer execution creates");

    service
        .maybe_cascade_executor_completion(&reviewer.id)
        .await
        .expect("unbound reviewer completion is ignored");

    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].id, review.id);
    assert_eq!(reviews[0].status, ReviewStatus::Running);
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, crate::workflow::default_states::REVIEW);
}

#[tokio::test]
async fn daemon_placement_reviewer_completion_evaluates_through_owner() {
    use crate::workspace_backend::DaemonWorkspaceBackend;
    let (db, service, task, execution, repo_dir) = seed_admitted_review().await;
    let (_, owner, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    let workspace_id = execution.workspace_id.as_deref().unwrap();
    let repo_id: String = sqlx::query_scalar("SELECT repo_id FROM workspace WHERE id = ?")
        .bind(workspace_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let now = now_rfc3339();
    let location = db::RepoLocationRepo::create(
        &*db,
        db::CreateRepoLocation {
            id: new_uuid_v4(),
            repo_id,
            owner_kind: db::RepoLocationOwnerKind::Daemon,
            daemon_id: owner.daemon_id.clone(),
            runtime_id: owner.runtime_id.clone(),
            path: "/owner-only/repo".into(),
            kind: db::RepoLocationKind::PrimaryCheckout,
            is_default: true,
            status: db::RepoLocationStatus::Ready,
            last_verified_at: Some(now.clone()),
            last_error: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    db::WorkspacePlacementRepo::create(
        &*db,
        db::CreateWorkspacePlacement {
            id: new_uuid_v4(),
            workspace_id: workspace_id.into(),
            task_id: task.id.clone(),
            agent_id: None,
            owner_kind: db::PlacementOwnerKind::Daemon,
            daemon_id: owner.daemon_id.clone(),
            runtime_id: owner.runtime_id,
            repo_location_id: location.id,
            execution_daemon_id: owner.daemon_id.clone(),
            workspace_handle: Some("opaque-review-owner".into()),
            generation: 1,
            state: db::PlacementState::Ready,
            selected_by: db::PlacementSelectedBy::Scheduler,
            selection_reason: "{}".into(),
            reserved_until: None,
            disconnected_at: None,
            failure_cause: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let daemon_id = owner.daemon_id.unwrap();
    let (connection_id, mut outbound) =
        crate::recovery::tests::owner_connection(&registry, &daemon_id, false);
    let sha = git::get_current_sha(repo_dir.path()).await.unwrap();
    let evidence_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let responder = {
        let registry = registry.clone();
        let evidence_reads = evidence_reads.clone();
        tokio::spawn(async move {
            while let Some(api_types::DaemonFrame::Request { id, method, params }) =
                outbound.recv().await
            {
                assert_eq!(params["workspace_handle"], "opaque-review-owner");
                let result = match method.as_str() {
                    api_types::METHOD_WORKSPACE_DESCRIBE => {
                        json!({"workspace_handle": "opaque-review-owner", "generation": 1,
                        "exists": true, "head_sha": sha, "dirty": false, "branch": "review-candidate", "locked": false,
                        "active_execution_ids": [], "journaled_execution_ids": []})
                    }
                    api_types::METHOD_WORKSPACE_READ => {
                        evidence_reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        match params["query"]["kind"].as_str().unwrap() {
                            "head" | "resolve_ref" | "merge_base" => {
                                json!({"kind": "git", "output": format!("{sha}\n")})
                            }
                            "tracked_changes" | "candidate_paths" => {
                                json!({"kind": "git", "output": ""})
                            }
                            query => panic!("unexpected conformance query: {query}"),
                        }
                    }
                    method => panic!("unexpected reviewer owner operation: {method}"),
                };
                registry.dispatch_incoming_for_connection(
                    &daemon_id,
                    connection_id,
                    api_types::DaemonFrame::Response { id, result },
                );
            }
        })
    };
    let router = (*service.workspace_backend_router())
        .clone()
        .with_daemon(Arc::new(DaemonWorkspaceBackend::new(db.clone(), registry)));
    let service = service.with_workspace_backend_router(Arc::new(router));
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        complete_review_with(
            &db,
            &service,
            &execution,
            r#"{"result":"pass","reason":"acceptance satisfied"}"#,
        ),
    )
    .await
    .unwrap();
    responder.abort();
    let review = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(review.status, ReviewStatus::Passed);
    let details: api_types::ReviewDetails =
        serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(
        details.conformance.status,
        api_types::ConformanceStatus::Passed
    );
    assert!(evidence_reads.load(std::sync::atomic::Ordering::SeqCst) >= 2);
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "merging"
    );
}

#[tokio::test]
async fn human_required_review_can_be_rejected_by_the_bound_project_agent() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let mut workflow = crate::workflow::default_workflow::default_workflow();
    workflow
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::REVIEW)
        .and_then(|state| state.gate_config.as_mut())
        .expect("review gate config")
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
        .bind(serde_json::to_string(&workflow).expect("workflow serializes"))
        .bind(&project_id)
        .execute(db.pool())
        .await
        .expect("project workflow updates");
    let agent_id = seed_agent(&db).await;
    let task = seed_task_with_status(&db, &project_id, "review".to_owned()).await;
    let now = now_rfc3339();
    let execution = ExecutionRepo::create(
        &*db,
        db::CreateExecution {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            agent_id: Some(agent_id.clone()),
            role: crate::workflow::default_roles::REVIEWER.to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: Some("reviewer-session".to_owned()),
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: Some(
                workflow_execution_snapshot(&db, &project_id).await,
            ),
            workspace_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("completed reviewer execution creates");
    ReviewRepo::create(
        &*db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            execution_id: execution.id.clone(),
            attempt_number: 1,
            status: ReviewStatus::AwaitingHuman,
            step_results_json: json!({ "ci_steps": [] }).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("running review creates");

    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("reviewer completion cascade succeeds");
    service
        .maybe_cascade_executor_completion(&execution.id)
        .await
        .expect("reviewer completion cascade is idempotent");

    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews load");
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::AwaitingHuman);
    assert!(reviews[0].finished_at.is_none());
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(current.status, "review");
    assert!(service
        .is_awaiting_human(task.id.clone())
        .await
        .expect("awaiting human resolves"));
    // Explicit human review does not invent an agent conformance assessment.
    assert!(
        serde_json::from_str::<serde_json::Value>(&reviews[0].step_results_json)
            .unwrap()
            .get("conformance")
            .is_none()
    );

    // Model the completed implementation attempt that preceded review and
    // exhaust the optional automatic review-fix retry. This keeps the test
    // focused on Project-Agent authority instead of launching a follow-up
    // executor against this deliberately workspace-free fixture.
    sqlx::query(
        "INSERT INTO execution (
             id, task_id, agent_id, role, status, created_at, updated_at
         ) VALUES (?, ?, ?, 'coder', 'completed', ?, ?)",
    )
    .bind(new_uuid_v4())
    .bind(&task.id)
    .bind(&agent_id)
    .bind(now_rfc3339())
    .bind(now_rfc3339())
    .execute(db.pool())
    .await
    .expect("completed coder attempt creates");
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(r#"{"retry_budgets":{"review":0}}"#)
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("review retry budget updates");

    let agent = AgentRepo::get_by_id(&*db, &agent_id)
        .await
        .expect("Project Agent reads")
        .expect("Project Agent exists");
    let binding = ProjectAgentBindingRepo::get_active_project_binding(&*db, &project_id)
        .await
        .expect("Project Agent binding reads")
        .expect("Project Agent binding exists");
    ProjectAgentBindingRepo::replace_project_binding(
        &*db,
        ReplaceProjectAgentBinding {
            project_id: project_id.clone(),
            expected_version: binding.version,
            replacement: CreateProjectAgentBinding {
                id: new_uuid_v4(),
                project_id: project_id.clone(),
                identity_id: Some(agent_id.clone()),
                profile_id: Some(agent.profile_id),
                state: "active".to_owned(),
                autonomy_policy_json: "{}".to_owned(),
                permission_ceiling_json: r#"{"permissions":["propose_task"]}"#.to_owned(),
                subscriptions_json: "[]".to_owned(),
                wake_budget: 1,
                operating_skill_revision_id: None,
                policy_revision: "default".to_owned(),
                policy_digest: String::new(),
                charter_id: None,
                charter_revision_id: None,
                charter_setup_required: true,
                admission_receipt_id: None,
                charter_approval_id: None,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
            replacement_reason: Some("test human-required review".to_owned()),
        },
    )
    .await
    .expect("Project Agent binding updates");
    crate::test_support::clear_project_execution_role_defaults(&db, &project_id).await;

    let unbound = service
        .perform_project_agent_review(
            &project_id,
            task.id.clone(),
            false,
            Some("not authorized".to_owned()),
            current.version,
            "another-agent",
        )
        .await;
    assert!(matches!(
        unbound,
        Err(ServiceError::TaskActionUnavailable { .. })
    ));

    let reviewed = service
        .perform_project_agent_review(
            &project_id,
            task.id.clone(),
            false,
            Some("Please address the review feedback".to_owned()),
            current.version,
            &agent_id,
        )
        .await
        .expect("bound Project Agent rejects human-required review");
    assert_eq!(
        reviewed.action,
        api_types::TaskAction::SendBack {
            guidance: "Please address the review feedback".to_owned()
        }
    );
    assert_eq!(
        reviewed.task.status,
        crate::workflow::default_states::IN_PROGRESS
    );
    let reviews = ReviewRepo::list_by_task(&*db, &task.id)
        .await
        .expect("reviews reload");
    assert_eq!(reviews[0].status, ReviewStatus::Failed);
}
