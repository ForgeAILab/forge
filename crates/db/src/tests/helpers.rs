//! Shared database fixtures and test request builders.
use super::*;

pub(super) fn page(limit: i64) -> PageRequest {
    PageRequest {
        cursor: None,
        limit,
        include_total: true,
        sort_by: SortBy::CreatedAt,
        sort_order: SortOrder::Asc,
    }
}

pub(super) async fn sqlite_db() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    SqliteDb::new(pool)
}

pub(super) fn pending_claim_lease(execution_id: &str, now: &str) -> ClaimExecutionLease {
    ClaimExecutionLease {
        execution_id: execution_id.to_owned(),
        expected_version: 1,
        owner: format!("dispatch-pending:{execution_id}"),
        lease_expires_at: now.to_owned(),
        hard_deadline_at: Some("2099-01-01T00:00:00Z".to_owned()),
        now: now.to_owned(),
    }
}

// Transaction-kernel tests own a real Task step, as production claim does.
pub(super) async fn claim_in_test_step(
    db: &SqliteDb,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    input: ClaimTask,
) -> crate::Result<crate::ClaimedTask> {
    use crate::TaskStepRepo;
    let task = TaskRepo::get_by_id_in_tx(db, transaction, &input.task_id, false)
        .await?
        .ok_or(DbError::NotFound)?;
    if task.version != input.expected_version {
        return Err(DbError::VersionConflict);
    }
    let id = new_uuid_v4();
    let now = now_rfc3339();
    let until = (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
    db.enqueue_step_in_tx(
        transaction,
        &crate::EnqueueTaskStep {
            id: id.clone(),
            task_id: task.id.clone(),
            kind: "command".into(),
            payload_json: "{}".into(),
            causation_step_id: None,
            causation_key: id.clone(),
            chain_id: id.clone(),
            chain_position: 1,
            expected_status: task.status.clone(),
            expected_version: task.version,
            expected_epoch: None,
            lane: "fast".into(),
            available_at: now.clone(),
        },
    )
    .await?;
    let epoch: i64 = sqlx::query_scalar("SELECT expected_epoch FROM task_step WHERE id=?")
        .bind(&id)
        .fetch_one(&mut **transaction)
        .await?;
    let changed = sqlx::query("UPDATE task_step SET status='claimed',claimed_by='claim-test',lease_until=?,attempts=1 WHERE id=? AND NOT EXISTS(SELECT 1 FROM task_step held WHERE held.task_id=? AND held.id<>? AND held.lease_until>?)")
        .bind(&until).bind(&id).bind(&task.id).bind(&id).bind(&now).execute(&mut **transaction).await?.rows_affected();
    assert_eq!(changed, 1, "one writer for the fixture Task");
    let step = crate::TaskStep {
        id: id.clone(),
        task_id: task.id,
        seq: 0,
        kind: "command".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: id.clone(),
        chain_id: id.clone(),
        chain_position: 1,
        expected_status: task.status,
        expected_version: task.version,
        expected_epoch: epoch,
        lane: "fast".into(),
        status: "claimed".into(),
        claimed_by: Some("claim-test".into()),
        lease_until: Some(until),
        available_at: now.clone(),
        attempts: 1,
        last_error: None,
        created_at: now.clone(),
        updated_at: now,
        completed_at: None,
        result_json: None,
        entry_fenced: true,
        suspended_until: None,
        awaited_consumer_id: None,
    };
    let result =
        crate::task_writer::in_task_step(step.clone(), TaskRepo::claim(db, transaction, input))
            .await;
    db.finish_step_in_tx(
        transaction,
        &step,
        if result.is_ok() { "done" } else { "failed" },
        None,
    )
    .await?;
    sqlx::query("UPDATE task_step SET claimed_by=NULL,lease_until=NULL WHERE id=?")
        .bind(&id)
        .execute(&mut **transaction)
        .await?;
    result
}

pub(super) async fn seed_daemon(db: &SqliteDb) -> String {
    let now = now_rfc3339();
    let daemon_id = new_uuid_v4();
    DaemonRepo::upsert_by_machine_id(
        db,
        UpsertDaemon {
            max_concurrent_runs: None,
            id: daemon_id.clone(),
            machine_id: format!("machine-{daemon_id}"),
            hostname: "test-host".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            agent_version: None,
            labels_json: "{}".to_owned(),
            status: DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon creates");
    daemon_id
}

pub(super) async fn seed_project_repo_agent(db: &SqliteDb) -> (String, String, String) {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    let repo_id = new_uuid_v4();
    let agent_id = new_uuid_v4();
    let daemon_id = seed_daemon(db).await;

    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");
    RepoRepo::create(
        db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "forge".to_owned(),
            remote_url: Some("https://example.com/forge.git".to_owned()),
            local_path: Some("/tmp/forge-test-repo".to_owned()),
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");
    AgentRepo::create(
        db,
        CreateAgent {
            id: agent_id.clone(),
            name: "shell".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            capabilities_json: r#"["rust"]"#.to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            prompt_template: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent creates");

    (project_id, repo_id, agent_id)
}

pub(super) async fn seed_project(db: &SqliteDb, name: &str, owner_id: Option<String>) -> String {
    let now = now_rfc3339();
    let project_id = new_uuid_v4();
    ProjectRepo::create(
        db,
        CreateProject {
            id: project_id.clone(),
            name: name.to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("project creates");
    project_id
}

pub(super) fn memory_item(project_id: &str, title: &str, body: &str) -> MemoryItem {
    MemoryItem {
        row_id: 0,
        id: new_uuid_v4(),
        project_id: Some(project_id.to_owned()),
        task_id: None,
        execution_id: None,
        scope_type: "project".to_owned(),
        scope_id: project_id.to_owned(),
        visibility: "project".to_owned(),
        owner_identity_id: None,
        authority: "observation".to_owned(),
        sensitivity: "internal".to_owned(),
        retention_priority: 10,
        provenance_json: "{}".to_owned(),
        publication_source_id: None,
        supersedes_id: None,
        valid_from: None,
        valid_until: None,
        source_event_id: None,
        source_scope_type: Some("project".to_owned()),
        source_scope_id: Some(project_id.to_owned()),
        source_revision: None,
        source_type: MemorySourceType::Comment.to_string(),
        kind: MemoryKind::Observation.to_string(),
        title: title.to_owned(),
        summary: None,
        body: body.to_owned(),
        metadata_json: "{}".to_owned(),
        confidence: Some(MemoryConfidence::Confirmed.to_string()),
        quality_score: None,
        created_by_type: None,
        created_by_id: None,
        created_at: now_rfc3339(),
    }
}

pub(super) fn memory_cursor(item: &MemoryItem) -> String {
    URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&serde_json::json!({
            "created_at": &item.created_at,
            "id": &item.id,
        }))
        .expect("memory cursor serializes"),
    )
}

pub(super) async fn seed_agent(
    db: &SqliteDb,
    name: &str,
    visibility: &str,
    owner_id: Option<String>,
) -> String {
    let now = now_rfc3339();
    let agent_id = new_uuid_v4();
    AgentRepo::create(
        db,
        CreateAgent {
            id: agent_id.clone(),
            name: name.to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id,
            visibility: visibility.to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent creates");
    agent_id
}

pub(super) async fn seed_task(
    db: &SqliteDb,
    project_id: &str,
    agent_id: Option<&str>,
    status: String,
    title: &str,
) -> String {
    let now = now_rfc3339();
    let task_id = new_uuid_v4();
    TaskRepo::create(
        db,
        CreateTask {
            id: task_id.clone(),
            project_id: project_id.to_owned(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: title.to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status,
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
    if let Some(agent_id) = agent_id {
        TaskRoleAssignmentRepo::assign(
            db,
            CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: task_id.clone(),
                role_name: "coder".to_owned(),
                assignee_type: Some(crate::AssigneeKind::Agent),
                assignee_id: Some(agent_id.to_owned()),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("role assignment creates");
    }
    task_id
}

pub(super) async fn seed_workspace_for_task(db: &SqliteDb, task_id: &str, repo_id: &str) -> String {
    let now = now_rfc3339();
    let workspace_id = new_uuid_v4();
    WorkspaceRepo::create(
        db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.to_owned(),
            repo_id: repo_id.to_owned(),
            worktree_path: format!("/tmp/forge/worktrees/{task_id}"),
            branch: format!("forge/{task_id}"),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("workspace creates");
    workspace_id
}

pub(super) async fn seed_terminal_session(
    db: &SqliteDb,
    task_id: &str,
    workspace_id: &str,
    user_id: &str,
    created_at: &str,
) -> crate::TerminalSession {
    TerminalSessionRepo::create_terminal_session(
        db,
        CreateTerminalSession {
            id: new_uuid_v4(),
            task_id: task_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            daemon_id: None,
            created_by_user_id: user_id.to_owned(),
            rows: 24,
            cols: 80,
            created_at: created_at.to_owned(),
        },
    )
    .await
    .expect("terminal session creates")
}

pub(super) async fn seed_ordered_task(
    db: &SqliteDb,
    project_id: &str,
    parent_task_id: Option<&str>,
    subtask_order: Option<i64>,
    title: &str,
    created_at: &str,
) -> Task {
    TaskRepo::create(
        db,
        CreateTask {
            id: new_uuid_v4(),
            project_id: project_id.to_owned(),
            parent_task_id: parent_task_id.map(str::to_owned),
            subtask_order,
            assignee_type: None,
            assignee_id: None,
            title: title.to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "todo".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: created_at.to_owned(),
            updated_at: created_at.to_owned(),
        },
    )
    .await
    .expect("ordered task creates")
}

// Freeze both contract and passed assessment so these tests exercise the same
// verification paths as acceptance, integration, and authority carry.
pub(super) async fn seed_review_digest_contract(
    version: u32,
) -> (SqliteDb, String, String, api_types::ReviewContract) {
    use api_types::{canonical_digest, review_source_digest, ReviewConformance, ReviewContract};
    let db = sqlite_db().await;
    let (project_id, repo_id, agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(&db, &project_id, None, "review".into(), "Digest scope").await;
    let now = now_rfc3339();
    TaskRoleAssignmentRepo::assign(
        &db,
        CreateTaskRoleAssignment {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            role_name: "reviewer".into(),
            assignee_type: Some(crate::AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE project SET settings = ?, workflow_definition = ? WHERE id = ?")
        .bind(
            serde_json::json!({
                "default_review_config": {"ci_steps": ["true"]},
                "environment": {"env": {"TOKEN": "original"}},
            })
            .to_string(),
        )
        .bind(
            serde_json::json!({"states": [
                {"name": "review", "role": "reviewer", "config": {}},
                {"name": "working", "role": "coder", "config": {}},
            ]})
            .to_string(),
        )
        .bind(&project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let workspace_id = seed_workspace_for_task(&db, &task_id, &repo_id).await;
    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: Some(agent_id),
            role: "reviewer".into(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let source = db
        .review_source(&task_id, Some(&execution_id))
        .await
        .unwrap();
    let mut context_json = serde_json::json!({
        "project_id": project_id, "task_id": task_id, "repo_id": repo_id,
        "charter_revision_id": null, "charter_digest": null, "charter": null,
        "task_scope": source["task_scope"], "linked_documents": [],
        "requirements": [], "setup_steps": [], "required_checks": [],
        "source_digest": if version == 1 {
            // Independently reproduce the pre-change algorithm.
            canonical_digest(&source).unwrap()
        } else {
            review_source_digest(&source, version).unwrap()
        },
    });
    if version != 1 {
        context_json["source_digest_version"] = serde_json::json!(version);
    }
    let mut contract = ReviewContract {
        execution_id: execution_id.clone(),
        policy: api_types::REVIEW_CONFORMANCE_POLICY.into(),
        commit_sha: "candidate".into(),
        base_sha: "base".into(),
        candidate_changed_paths: vec!["file.rs".into()],
        context: serde_json::from_value(context_json).unwrap(),
        check_results: Vec::new(),
        digest: String::new(),
    };
    contract.digest = canonical_digest(&contract).unwrap();
    db.create_review_contract(&contract).await.unwrap();
    let conformance = ReviewConformance {
        status: api_types::ConformanceStatus::Passed,
        contract: Some(contract.clone()),
        assessment: Some(api_types::ReviewAssessment {
            result: api_types::ReviewResult::Pass,
            reason: "checked".into(),
            fixable_by: api_types::FixableBy::Coder,
            repeat: false,
            findings: Vec::new(),
            resolved: Vec::new(),
            report: String::new(),
        }),
        checks: Vec::new(),
        reason: None,
    };
    db.record_review_conformance(&conformance).await.unwrap();
    ReviewRepo::create(
        &db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            execution_id,
            attempt_number: 1,
            status: ReviewStatus::Passed,
            step_results_json: serde_json::json!({"conformance": conformance}).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    TaskRepo::set_review_passed_at(&db, &task_id, Some(now.clone()), &now)
        .await
        .unwrap();
    (db, project_id, task_id, contract)
}

pub(super) async fn assert_review_digest_integrates(db: &SqliteDb, task_id: &str) {
    let guard = db.lock_review_integration(task_id).await.unwrap();
    assert!(guard.contract.is_some(), "must check automated authority");
    guard.release().await.unwrap();
}

pub(super) async fn seed_child(
    db: &SqliteDb,
    project_id: &str,
    root: &str,
    order: i64,
    title: &str,
) -> String {
    let id = seed_task(db, project_id, None, "todo".into(), title).await;
    sqlx::query("UPDATE task SET parent_task_id=?, subtask_order=? WHERE id=?")
        .bind(root)
        .bind(order)
        .bind(&id)
        .execute(db.pool())
        .await
        .unwrap();
    id
}

// ── User / RefreshToken tests ──────────────────────────────────────────────

pub(super) async fn seed_user(db: &SqliteDb) -> String {
    let now = now_rfc3339();
    let id = new_uuid_v4();
    let user = User {
        id: id.clone(),
        email: format!("user-{}@example.com", id),
        password_hash: "hash".to_owned(),
        display_name: None,
        is_admin: false,
        created_at: now.clone(),
        updated_at: now,
    };
    UserRepo::create_user(db, &user).await.expect("seed user");
    id
}

pub(super) async fn long_form_review_assessment() -> (SqliteDb, api_types::ReviewConformance, String)
{
    let (db, _, _, mut contract) = seed_review_digest_contract(2).await;
    let now = now_rfc3339();
    contract.execution_id = new_uuid_v4();
    contract.digest = String::new();
    contract.digest = api_types::canonical_digest(&contract).unwrap();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES(?,?,'reviewer','completed',?,?)")
        .bind(&contract.execution_id).bind(&contract.context.task_id).bind(&now).bind(&now)
        .execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO execution_review_contract VALUES(?,?,?,?,?,?)")
        .bind(&contract.execution_id)
        .bind(&contract.context.task_id)
        .bind(&contract.digest)
        .bind(&contract.context.source_digest)
        .bind(serde_json::to_string(&contract).unwrap())
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    let assessment = api_types::ReviewConformance {
        status: api_types::ConformanceStatus::Failed,
        contract: Some(contract.clone()),
        assessment: Some(api_types::ReviewAssessment {
            result: api_types::ReviewResult::Fail,
            reason: "missing test".into(),
            report: "## Review\n\nNo test.".into(),
            fixable_by: api_types::FixableBy::Coder,
            repeat: false,
            findings: Vec::new(),
            resolved: Vec::new(),
        }),
        checks: Vec::new(),
        reason: Some("missing test".into()),
    };
    let mut historical = serde_json::to_value(&assessment).unwrap();
    historical["assessment"]["fixable_by"] = serde_json::json!("coder");
    historical["assessment"]["repeat"] = serde_json::json!(false);
    let historical = serde_json::to_string_pretty(&historical).unwrap();
    sqlx::query("INSERT INTO execution_review_assessment VALUES(?,?,?)")
        .bind(&contract.execution_id)
        .bind(&historical)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    (db, assessment, historical)
}
