//! Execution lease authority and captured execution policy.
use super::*;

#[tokio::test]
async fn run_execution_reissues_stale_lease_after_task_row_moves() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Lease survives task-row movement",
            Some("printf lease-race-ok".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");

    // Any Task-row mutation between lease issuance and dispatch (role
    // handoff, retry-metadata clear, concurrent transition) bumps the
    // version and breaks the lease's exact match. The dispatch must
    // recover by reissuing this execution's lease, not hard-fail.
    sqlx::query("UPDATE task SET version = version + 1 WHERE id = ?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .expect("task version bumps");

    let updated = service
        .run_execution(claimed.execution.id, &NoDiffExecutor)
        .await
        .expect("execution recovers from the stale lease instead of failing");

    assert_eq!(updated.status, ExecutionStatus::Completed);
}

#[tokio::test]
async fn run_execution_rejects_stale_coder_lease_after_task_moves_to_review() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Reject stale coder lease after review handoff",
            Some("this must not run after the role handoff".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims as coder");

    // Move the Task to the default workflow's reviewer state without changing
    // the execution's persisted coder role or its active lease. The lease
    // version is now stale, which forces run_execution through the reissue
    // branch that previously accepted any role found elsewhere in the
    // workflow.
    sqlx::query(
        "UPDATE task
         SET status = 'review', version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(now_rfc3339())
    .bind(&task.id)
    .execute(db.pool())
    .await
    .expect("task moves to review");

    let error = service
        .run_execution(claimed.execution.id.clone(), &NoDiffExecutor)
        .await
        .expect_err("a coder lease must not be reissued for reviewer state");
    assert!(
        error
            .to_string()
            .contains("current effective role is 'reviewer'"),
        "unexpected admission error: {error}"
    );

    let execution = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution reloads")
        .expect("execution exists");
    assert_eq!(
        execution.status,
        ExecutionStatus::Failed,
        "obsolete role admission must terminalize the execution instead of leaving a running orphan"
    );
    assert_ne!(execution.status, ExecutionStatus::Running);
    assert!(
        ExecutionRepo::list_running_by_task(&*db, &task.id)
            .await
            .expect("running execution lookup")
            .is_empty(),
        "authority loss must not leave a running execution occupying the Task slot"
    );
    assert!(
        db::WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
            .await
            .expect("active lease lookup")
            .is_none(),
        "the stale coder lease must be revoked without issuing reviewer authority"
    );
    let current_task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert!(
        current_task.error_annotation.is_none() && current_task.blocked_json.is_none(),
        "the obsolete coder failure must not block or annotate the newer reviewer state"
    );
}

#[tokio::test]
async fn workspace_lease_reissue_rejects_stale_project_workflow_authority() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent(&db).await;
    let task = service
        .create_task(
            project_id,
            "Reject stale project workflow lease",
            Some("workflow authority race".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");
    let claimed = service
        .claim_task(task.id.clone(), Assignee::Agent(agent_id), None)
        .await
        .expect("task claims");
    let lease = db::WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
        .await
        .expect("active lease loads")
        .expect("claim creates a workspace lease");
    let project = db::ProjectRepo::get_by_id(&*db, &task.project_id)
        .await
        .expect("project loads")
        .expect("project exists");
    let expected_project_version = project.version;
    let expected_workflow_definition = project.workflow_definition.clone();

    // Simulate the Project workflow edit winning after the dispatcher took
    // its authority snapshot but before the lease INSERT. The workflow stays
    // valid and can retain the same effective role; the Project revision must
    // still be exact because the prompt/config authority came from the old
    // snapshot.
    let edited_workflow =
        serde_json::to_string(&crate::workflow::default_workflow::default_workflow())
            .expect("workflow serializes");
    sqlx::query(
        "UPDATE project
         SET workflow_definition = ?, version = version + 1, updated_at = ?
         WHERE id = ?",
    )
    .bind(edited_workflow)
    .bind(now_rfc3339())
    .bind(&task.project_id)
    .execute(db.pool())
    .await
    .expect("project workflow edits");

    let now = now_rfc3339();
    let stale_input = db::CreateWorkspaceLease {
        id: new_uuid_v4(),
        project_id: lease.project_id.clone(),
        task_id: lease.task_id.clone(),
        task_version: lease.task_version,
        execution_id: claimed.execution.id.clone(),
        operation_idempotency_key: format!("{}::workflow-race", claimed.execution.id),
        repository_binding_id: lease.repository_binding_id.clone(),
        base_ref: lease.base_ref.clone(),
        role: lease.role.clone(),
        capabilities_json: lease.capabilities_json.clone(),
        assigned_principal_type: lease.assigned_principal_type.clone(),
        assigned_principal_id: lease.assigned_principal_id.clone(),
        capability_profile_revision: lease.capability_profile_revision.clone(),
        capability_profile_digest: lease.capability_profile_digest.clone(),
        issuing_principal_type: lease.issuing_principal_type.clone(),
        issuing_principal_id: lease.issuing_principal_id.clone(),
        issued_at: now.clone(),
        expires_at: now.clone(),
        created_at: now.clone(),
        updated_at: now,
    };
    let error = db::WorkspaceLeaseRepo::replace_with_project_authority(
        &*db,
        stale_input.clone(),
        expected_project_version,
        &expected_workflow_definition,
        &task.status,
        crate::workflow::default_roles::CODER,
        &lease.id,
        lease.version,
    )
    .await
    .expect_err("edited Project authority must reject the stale lease snapshot");
    assert!(matches!(error, db::DbError::VersionConflict));
    assert!(
        db::WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
            .await
            .expect("active lease lookup")
            .is_some_and(|active| active.id == lease.id && active.version == lease.version),
        "a rejected authority snapshot must roll back the replacement and preserve the old lease"
    );

    // The Project snapshot now is current, so this second rejection can only
    // come from the transaction's live Task/workflow role check. The default
    // in-progress state is coder, never reviewer.
    let current_project = db::ProjectRepo::get_by_id(&*db, &task.project_id)
        .await
        .expect("current project loads")
        .expect("current project exists");
    let mut wrong_role_input = stale_input.clone();
    wrong_role_input.id = new_uuid_v4();
    wrong_role_input.operation_idempotency_key =
        format!("{}::workflow-role-race", claimed.execution.id);
    let role_error = db::WorkspaceLeaseRepo::replace_with_project_authority(
        &*db,
        wrong_role_input,
        current_project.version,
        &current_project.workflow_definition,
        &task.status,
        crate::workflow::default_roles::REVIEWER,
        &lease.id,
        lease.version,
    )
    .await
    .expect_err("live effective role must reject a reviewer lease for coder state");
    assert!(matches!(role_error, db::DbError::VersionConflict));
    assert!(
        db::WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
            .await
            .expect("active lease lookup after role rejection")
            .is_some_and(|active| active.id == lease.id && active.version == lease.version),
        "role rejection must preserve the old lease transactionally"
    );

    // Finally force the INSERT trigger to reject after the replacement code
    // has reached the old-lease retirement point. BEGIN IMMEDIATE must roll
    // that retirement back as well.
    let mut trigger_failure_input = stale_input;
    trigger_failure_input.id = new_uuid_v4();
    trigger_failure_input.operation_idempotency_key =
        format!("{}::insert-trigger-race", claimed.execution.id);
    trigger_failure_input.assigned_principal_id = "missing-agent".to_owned();
    let issued_at = now_rfc3339();
    trigger_failure_input.issued_at = issued_at.clone();
    trigger_failure_input.expires_at =
        (chrono::Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
    trigger_failure_input.created_at = issued_at.clone();
    trigger_failure_input.updated_at = issued_at;
    let trigger_error = db::WorkspaceLeaseRepo::replace_with_project_authority(
        &*db,
        trigger_failure_input,
        current_project.version,
        &current_project.workflow_definition,
        &task.status,
        crate::workflow::default_roles::CODER,
        &lease.id,
        lease.version,
    )
    .await
    .expect_err("lease scope trigger must reject the mismatched assignment");
    assert!(matches!(trigger_error, db::DbError::VersionConflict));
    assert!(
        db::WorkspaceLeaseRepo::get_active_for_task(&*db, &task.id)
            .await
            .expect("active lease lookup after trigger rejection")
            .is_some_and(|active| active.id == lease.id && active.version == lease.version),
        "an INSERT-trigger failure must roll back old-lease retirement"
    );
}

#[tokio::test]
async fn claim_task_records_execution_policy_overrides_in_snapshot() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id =
        seed_shell_agent_with_config(&db, r#"{"command":"echo","args":["profile-default"]}"#).await;
    let task = service
        .create_task(
            project_id,
            "Snapshot override",
            Some("unused".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");

    let claimed = service
        .claim_task(
            task.id,
            Assignee::Agent(agent_id),
            Some(ExecutionOverrides {
                model_id: None,
                reasoning_effort: None,
                permission_policy: Some("auto".to_owned()),
                hard_deadline_seconds: Some(600),
            }),
        )
        .await
        .expect("task claims");
    let execution = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
        .await
        .expect("execution loads")
        .expect("execution exists");
    let snapshot: Value = serde_json::from_str(
        execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("snapshot recorded"),
    )
    .expect("snapshot parses");

    assert_eq!(snapshot["config"]["permission_policy"], "auto");
    assert_eq!(snapshot["hard_deadline_seconds"], 600);
    assert!(execution.hard_deadline_at.is_some());
    let execution_keys = snapshot["overrides_applied"]["execution"]
        .as_array()
        .expect("execution override keys are recorded");
    assert!(execution_keys
        .iter()
        .any(|key| key.as_str() == Some("permission_policy")));
}

#[tokio::test]
async fn claim_task_applies_agent_run_time_limit_unless_overridden() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(
        &db,
        "codex",
        r#"{"model":"agent-model","hard_deadline_seconds":5400}"#,
    )
    .await;
    let mut snapshots = Vec::new();
    for overrides in [
        None,
        Some(ExecutionOverrides {
            model_id: None,
            reasoning_effort: None,
            permission_policy: None,
            hard_deadline_seconds: Some(600),
        }),
    ] {
        let task = service
            .create_task(
                project_id.clone(),
                "Agent run time limit",
                Some("unused".to_owned()),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("task creates");
        let claimed = service
            .claim_task(task.id, Assignee::Agent(agent_id.clone()), overrides)
            .await
            .expect("task claims");
        let execution = ExecutionRepo::get_by_id(&*db, &claimed.execution.id)
            .await
            .expect("execution loads")
            .expect("execution exists");
        let snapshot: Value = serde_json::from_str(
            execution
                .executor_config_snapshot_json
                .as_deref()
                .expect("snapshot recorded"),
        )
        .expect("snapshot parses");
        assert!(execution.hard_deadline_at.is_some());
        assert!(snapshot["config"].get("hard_deadline_seconds").is_none());
        snapshots.push(snapshot["hard_deadline_seconds"].clone());
    }
    assert_eq!(snapshots, vec![json!(5400), json!(600)]);
}

#[tokio::test]
async fn claim_task_records_codex_overrides_in_normalized_snapshot() {
    let db = Arc::new(sqlite_db().await);
    let event_bus = Arc::new(EventBus::new(16));
    let service = TaskService::new(Arc::clone(&db), event_bus);
    let (project_id, _repo_id, _repo_dir) = seed_project_repo(&db).await;
    let agent_id = seed_agent_with_executor_type(
        &db,
        "codex",
        r#"{"model":"agent-model","model_reasoning_effort":"medium","sandbox":"danger-full-access","permission_policy":"supervised"}"#,
    )
    .await;
    let task = service
        .create_task(
            project_id,
            "Snapshot codex overrides",
            Some("unused".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("task creates");

    let claimed = service
        .claim_task(
            task.id,
            Assignee::Agent(agent_id),
            Some(ExecutionOverrides {
                model_id: Some("gpt-5-codex".to_owned()),
                reasoning_effort: Some("high".to_owned()),
                permission_policy: Some("auto".to_owned()),
                hard_deadline_seconds: None,
            }),
        )
        .await
        .expect("task claims");
    let snapshot: Value = serde_json::from_str(
        claimed
            .execution
            .executor_config_snapshot_json
            .as_deref()
            .expect("snapshot recorded"),
    )
    .expect("snapshot parses");

    assert_eq!(snapshot["executor_type"], "codex");
    assert_eq!(snapshot["config"]["model"], "gpt-5-codex");
    assert_eq!(snapshot["config"]["model_reasoning_effort"], "high");
    assert_eq!(snapshot["config"]["permission_policy"], "auto");
    assert!(snapshot["config"].get("effort").is_none());

    let agent_keys = snapshot["overrides_applied"]["agent"]
        .as_array()
        .expect("agent keys are recorded");
    assert!(agent_keys.iter().any(|key| key.as_str() == Some("model")));
    assert!(agent_keys
        .iter()
        .any(|key| key.as_str() == Some("model_reasoning_effort")));
    assert!(agent_keys
        .iter()
        .any(|key| key.as_str() == Some("permission_policy")));

    let execution_keys = snapshot["overrides_applied"]["execution"]
        .as_array()
        .expect("execution keys are recorded");
    assert!(execution_keys
        .iter()
        .any(|key| key.as_str() == Some("model")));
    assert!(execution_keys
        .iter()
        .any(|key| key.as_str() == Some("model_reasoning_effort")));
    assert!(execution_keys
        .iter()
        .any(|key| key.as_str() == Some("permission_policy")));
    assert!(!execution_keys
        .iter()
        .any(|key| key.as_str() == Some("effort")));
}
