//! Environment capacity cases.

use super::*;

#[tokio::test]
async fn revision_two_only_owner_waits_without_execution_with_upgrade_reason() {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    sqlx::query("DELETE FROM execution WHERE id = ?")
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM workspace_placement WHERE id = ?")
        .bind(&placement.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM workspace WHERE id = ?")
        .bind(&placement.workspace_id)
        .execute(db.pool())
        .await
        .unwrap();
    let agent = AgentRepo::get_by_id(&*db, placement.agent_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    let daemon_id = placement.daemon_id.as_deref().unwrap();
    sqlx::query("UPDATE daemon SET detected_clis_json = ? WHERE id = ?")
        .bind(r#"[{"kind":"shell","availability":"authenticated"}]"#)
        .bind(daemon_id)
        .execute(db.pool())
        .await
        .unwrap();
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let (connection, _outbound) = crate::daemon_transport::DaemonConnection::new(daemon_id.into());
    let id = connection.id();
    registry.register(daemon_id.into(), connection);
    registry.dispatch_incoming_for_connection(daemon_id, id, api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.into(),
            params: json!({"protocol_revision":2,"capabilities":["execution.terminal.usage_reports","execution.terminal.ack"]}),
        });
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_daemon_connections(registry.clone());
    let error = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .err()
        .expect("revision two is refused");
    let ServiceError::PlacementUnavailable(ref refusal) = error else {
        panic!("placement refusal");
    };
    assert!(refusal.needs_daemon_upgrade(), "{refusal:?}");
    assert!(!crate::placement::is_retryable_admission_refusal(&error));
    assert_eq!(execution_count(&db, &task.id).await, 0);
    let stored = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, task.status);
    assert!(stored.failed_json.is_none());
    assert!(stored.error_annotation.is_none());
    crate::workflow::engine::annotate_upgrade_dispatch_refusal(&db, &task.id, &task.status, &error)
        .await
        .unwrap();
    let stored = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let annotation: Value =
        serde_json::from_str(stored.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "dispatch_failed");
    assert!(annotation["message"]
        .as_str()
        .unwrap()
        .contains("daemon_upgrade_required"));
    assert!(annotation["message"]
        .as_str()
        .unwrap()
        .contains("upgrade the daemon"));
    assert_eq!(
        crate::agent_service::compute_effective_status(&db, &agent, Some(&registry))
            .await
            .unwrap(),
        crate::agent_service::EffectiveStatus::DaemonUpgradeRequired
    );
    let location = db::RepoLocationRepo::get_by_id(&*db, &placement.repo_location_id)
        .await
        .unwrap()
        .unwrap();
    let repo = RepoRepo::get_by_id(&*db, &location.repo_id)
        .await
        .unwrap()
        .unwrap();
    let runtime = db::RuntimeRepo::get_by_id(&*db, placement.runtime_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    let verifier = crate::repo_location::RemoteDaemonLocationVerifier::new(
        registry.clone(),
        PathBuf::from("unused"),
    );
    let verification =
        crate::repo_location::DaemonLocationVerifier::verify(&verifier, &repo, &location, &runtime)
            .await
            .unwrap();
    assert_eq!(verification.status, location.status);
    assert!(verification
        .last_error
        .unwrap()
        .contains("daemon_upgrade_required"));
    let location_service =
        crate::repo_location::RepoLocationService::new(db.clone(), Arc::new(verifier));
    location_service
        .retry_verification_on_reconnect(daemon_id)
        .await
        .unwrap();
    let stored_location = db::RepoLocationRepo::get_by_id(&*db, &location.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored_location.status, location.status);
    assert!(stored_location
        .last_error
        .unwrap()
        .contains("daemon_upgrade_required"));
    let error = service
        .reserve_claim_workspace(&stored, Some(&agent), "coder")
        .await
        .err()
        .unwrap();
    let ServiceError::PlacementUnavailable(refusal) = error else {
        panic!("placement refusal")
    };
    assert!(refusal.needs_daemon_upgrade());
}

#[tokio::test]
async fn no_checks_precheck_returns_before_invalid_selection_context() {
    let db = Arc::new(sqlite_db().await);
    let (mut task, placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    let agent = AgentRepo::get_by_id(&*db, placement.agent_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    let daemon_id = placement.daemon_id.as_deref().unwrap();
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let (connection, _outbound) = crate::daemon_transport::DaemonConnection::new(daemon_id.into());
    let id = connection.id();
    registry.register(daemon_id.into(), connection);
    registry.dispatch_incoming_for_connection(daemon_id,id,api_types::DaemonFrame::Notification {
            method:api_types::METHOD_DAEMON_HANDSHAKE.into(),
            params:json!({"protocol_revision":api_types::DAEMON_PROTOCOL_REVISION,"capabilities":["execution.terminal.usage_reports","journal.ack","workspace.v1","machine_probe.v1","repo_provision.v1"],"executor_capabilities":{},"workspace_run_policy":{"allowed_purposes":["environment_probe","repo_provision"]}}),
        });
    // A capable codeless runtime used to bypass the no-check early return.
    let root = TempDir::new().unwrap();
    db::RuntimeRepo::create(
        &*db,
        db::CreateRuntime {
            id: new_uuid_v4(),
            daemon_id: daemon_id.into(),
            kind: "probe-fast-path".into(),
            workspace_root: root.path().to_string_lossy().into_owned(),
            status: db::RuntimeStatus::Ready,
            labels_json: "{}".into(),
            created_at: now_rfc3339(),
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    task.task_state_config = Some("invalid workflow json".into());
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_daemon_connections(registry);
    assert!(!service
        .defer_initial_environment_probe(&task, &agent, "nonexistent-role")
        .await
        .unwrap());
    let facts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project_machine_readiness")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(facts, 0);
}

#[tokio::test]
async fn machine_capacity_precheck_does_not_hide_unavailable_server_executor() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let (project, _) = seed_project_with_real_repo(&db, repo.path()).await;
    let task = seed_task(&db, &project, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    sqlx::query("UPDATE agent_identity SET max_concurrent_tasks = 8 WHERE id = ?")
        .bind(&agent.id)
        .execute(db.pool())
        .await
        .unwrap();
    let agent = AgentRepo::get_by_id(&*db, &agent.id)
        .await
        .unwrap()
        .unwrap();
    db.server_run_cap
        .set(Some(1), 1, &::config::embedded_machine_id());
    let busy = seed_task(&db, &project, None).await;
    sqlx::query("INSERT INTO execution (id, task_id, agent_id, role, status, created_at, updated_at) VALUES (?, ?, ?, 'coder', 'running', ?, ?)")
            .bind(new_uuid_v4()).bind(&busy.id).bind(&agent.id).bind(now_rfc3339()).bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_placement_adapter_registry(Arc::new(executors::AdapterRegistry::new()));
    assert!(service
        .capacity_wait_for(&task, &agent, None)
        .await
        .unwrap()
        .is_none());
    let refusal = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .err()
        .unwrap();
    assert!(
        matches!(refusal, ServiceError::PlacementUnavailable(ref e) if e.rejected_candidates.iter().all(|r| r.filter_codes.contains(&crate::placement::PlacementFilterCode::ExecutorUnavailable)))
    );
    assert!(!crate::placement::is_machine_capacity_refusal(&refusal));
}

#[tokio::test]
async fn machine_capacity_precheck_allows_verification_of_unverified_server_clone() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let clone = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let (project, repo_id) = seed_project_with_real_repo(&db, repo.path()).await;
    let status = std::process::Command::new("git")
        .arg("clone")
        .arg(repo.path())
        .arg(clone.path())
        .output()
        .unwrap();
    assert!(status.status.success());
    let task = seed_task(&db, &project, None).await;
    let busy = seed_task(&db, &project, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    sqlx::query("UPDATE agent_identity SET max_concurrent_tasks = 8 WHERE id = ?")
        .bind(&agent.id)
        .execute(db.pool())
        .await
        .unwrap();
    let agent = AgentRepo::get_by_id(&*db, &agent.id)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("DELETE FROM repo_location WHERE repo_id = ?")
        .bind(&repo_id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query(r#"INSERT INTO daemon (id,machine_id,hostname,os,arch,status,visibility,detected_clis_json,labels_json,max_concurrent_runs,created_at,updated_at) VALUES ('capacity-remote','capacity-remote','remote','linux','x86_64','online','global','[{"kind":"shell","availability":"authenticated"}]','{}',1,?,?)"#)
            .bind(now_rfc3339()).bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO runtime (id,daemon_id,kind,workspace_root,status,created_at,updated_at) VALUES ('capacity-runtime','capacity-remote','local',?,'ready',?,?)")
            .bind(root.path().to_string_lossy().as_ref()).bind(now_rfc3339()).bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    for (id, owner, daemon, runtime, path, kind, state, default) in [
        (
            "capacity-clone",
            "server",
            None,
            None,
            clone.path(),
            "managed_clone",
            "unverified",
            1,
        ),
        (
            "capacity-checkout",
            "daemon",
            Some("capacity-remote"),
            Some("capacity-runtime"),
            root.path(),
            "primary_checkout",
            "ready",
            0,
        ),
    ] {
        sqlx::query("INSERT INTO repo_location (id,repo_id,owner_kind,daemon_id,runtime_id,path,kind,is_default,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)")
                .bind(id).bind(&repo_id).bind(owner).bind(daemon).bind(runtime).bind(path.to_string_lossy().as_ref()).bind(kind).bind(default).bind(state).bind(now_rfc3339()).bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    }
    sqlx::query(r#"INSERT INTO execution (id,task_id,agent_id,role,status,executor_config_snapshot_json,created_at,updated_at) VALUES (?, ?, ?, 'coder','running','{"daemon_id":"capacity-remote"}',?,?)"#)
            .bind(new_uuid_v4()).bind(&busy.id).bind(&agent.id).bind(now_rfc3339()).bind(now_rfc3339()).execute(db.pool()).await.unwrap();
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let (connection, _outgoing) =
        crate::daemon_transport::DaemonConnection::new("capacity-remote".to_owned());
    registry.register("capacity-remote".to_owned(), connection.clone());
    let handshake = api_types::DaemonHandshakeNotification {
        protocol_revision: api_types::DAEMON_PROTOCOL_REVISION,
        capabilities: api_types::DAEMON_REQUIRED_CAPABILITIES
            .iter()
            .map(|s| (*s).to_owned())
            .chain(std::iter::once("workspace.v1".to_owned()))
            .collect(),
        executor_capabilities: std::collections::BTreeMap::from([(
            "shell".to_owned(),
            crate::daemon_transport::EmbeddedExecutionProvider::adapter_capabilities("shell"),
        )]),
        workspace_run_policy: api_types::WorkspaceRunPolicy {
            allowed_purposes: vec![
                api_types::WorkspaceRunPurpose::EnvironmentSetup,
                api_types::WorkspaceRunPurpose::Hook,
                api_types::WorkspaceRunPurpose::CiStep,
            ],
        },
    };
    registry.dispatch_incoming_for_connection(
        "capacity-remote",
        connection.id(),
        api_types::DaemonFrame::Notification {
            method: api_types::METHOD_DAEMON_HANDSHAKE.to_owned(),
            params: serde_json::to_value(handshake).unwrap(),
        },
    );
    db.server_run_cap
        .set(Some(0), 0, &::config::embedded_machine_id());
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(root.path().to_path_buf())
        .with_daemon_connections(registry);
    assert!(service
        .capacity_wait_for(&task, &agent, None)
        .await
        .unwrap()
        .is_none());
    let reserved = service
        .reserve_claim_workspace(&task, Some(&agent), "coder")
        .await
        .unwrap();
    assert_eq!(reserved.placement.repo_location_id, "capacity-clone");
    let state: String =
        sqlx::query_scalar("SELECT status FROM repo_location WHERE id = 'capacity-clone'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(state, "ready");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn machine_capacity_precheck_is_read_only_without_full_machines() {
    let data = TempDir::new().unwrap();
    let pool = create_sqlite_pool(&format!(
        "sqlite:{}",
        data.path().join("forge.db").display()
    ))
    .await
    .unwrap();
    run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    let repo = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    db.server_run_cap
        .set(Some(2), 2, &::config::embedded_machine_id());
    let before: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM workspace_placement), (SELECT COUNT(*) FROM repo_location), (SELECT SUM(list_revision) FROM project)").fetch_one(db.pool()).await.unwrap();
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()));
    let writer = db::begin_immediate(db.pool()).await.unwrap();
    assert!(tokio::time::timeout(
        std::time::Duration::from_secs(15),
        service.capacity_wait_for(&task, &agent, None)
    )
    .await
    .unwrap()
    .unwrap()
    .is_none());
    drop(writer);
    let after: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM workspace_placement), (SELECT COUNT(*) FROM repo_location), (SELECT SUM(list_revision) FROM project)").fetch_one(db.pool()).await.unwrap();
    assert_eq!(before, after);
    assert_eq!(
        TaskRepo::get_by_id(&*db, &task.id, false)
            .await
            .unwrap()
            .unwrap(),
        task
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn machine_capacity_reserve_race_for_last_slot() {
    let data = TempDir::new().unwrap();
    let url = format!("sqlite:{}", data.path().join("forge.db").display());
    let pool = create_sqlite_pool(&url).await.expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let repo_dir = TempDir::new().unwrap();
    let workspace_root = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    sqlx::query("UPDATE agent_identity SET max_concurrent_tasks = 100 WHERE id = ?")
        .bind(&agent.id)
        .execute(db.pool())
        .await
        .unwrap();
    let agent = AgentRepo::get_by_id(&*db, &agent.id)
        .await
        .unwrap()
        .unwrap();
    let service = Arc::new(
        TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(workspace_root.path().to_path_buf()),
    );
    let mut admitted = Vec::new();
    let mut total_reserved = 0_i64;
    for (round, cap) in [(0, 1_u32), (1, 3), (2, 4), (3, 4)] {
        db.server_run_cap
            .set(Some(cap), cap, &::config::embedded_machine_id());
        let mut tasks = Vec::new();
        for _ in 0..8 {
            tasks.push(seed_task(&db, &project_id, None).await);
        }
        let mut handles = Vec::new();
        for task in tasks {
            let service = Arc::clone(&service);
            let agent = agent.clone();
            handles.push(tokio::spawn(async move {
                match service
                    .reserve_claim_workspace(&task, Some(&agent), "coder")
                    .await
                {
                    Ok(admission) => Ok(admission),
                    Err(ServiceError::PlacementUnavailable(refusal)) => Err(format!(
                        "placement_unavailable:{:?}",
                        refusal
                            .rejected_candidates
                            .iter()
                            .map(|candidate| candidate.filter_codes.clone())
                            .collect::<Vec<_>>()
                    )),
                    Err(error) => Err(format!("other:{error:?}")),
                }
            }));
        }
        let mut won = 0;
        let mut errors = std::collections::BTreeMap::<String, usize>::new();
        for handle in handles {
            match handle.await.unwrap() {
                Ok(admission) => {
                    won += 1;
                    admitted.push(admission);
                }
                Err(error) => *errors.entry(error).or_default() += 1,
            }
        }
        total_reserved += won;
        let reserved: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM workspace_placement WHERE state IN ('reserved','preparing')",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(reserved, i64::from(cap), "round {round}: {errors:?}");
        assert_eq!(reserved, total_reserved);
        assert!(reserved <= i64::from(cap), "cap {cap} exceeded: {reserved}");
    }
}

/// The slot rules through the real reserve transaction, raced: sixteen
/// Tasks ask at once on a file-backed database, with checks queued on the
/// machine. A reservation leaves the queued checks their share, never
/// more, and the machine never holds more than its cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn machine_capacity_reserve_race_leaves_queued_checks_their_slots() {
    let data = TempDir::new().unwrap();
    let url = format!("sqlite:{}", data.path().join("forge.db").display());
    let pool = create_sqlite_pool(&url).await.expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let repo_dir = TempDir::new().unwrap();
    let workspace_root = TempDir::new().unwrap();
    let (project_id, repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
    let agent = seed_unpinned_claim_agent(&db).await;
    sqlx::query("UPDATE agent_identity SET max_concurrent_tasks = 100 WHERE id = ?")
        .bind(&agent.id)
        .execute(db.pool())
        .await
        .unwrap();
    let agent = AgentRepo::get_by_id(&*db, &agent.id)
        .await
        .unwrap()
        .unwrap();
    let service = Arc::new(
        TaskService::new(db.clone(), Arc::new(EventBus::default()))
            .with_workspace_root(workspace_root.path().to_path_buf()),
    );
    db.server_run_cap
        .set(Some(4), 4, &::config::embedded_machine_id());
    // Three Tasks in review, each with a queued entry check on the server.
    let now = db::now_rfc3339();
    let mut reviewed = Vec::new();
    for i in 0..3 {
        let task = seed_task(&db, &project_id, None).await;
        sqlx::query("INSERT INTO check_run(id,project_id,repo_id,commit_sha,spec_digest,identity_key,input_json,cacheable,state,operation_id,created_at,updated_at) VALUES(?,?,?,?,'digest',?,'{}',0,'queued',?,?,?)")
                .bind(format!("run-{i}")).bind(&project_id).bind(&repo_id).bind("a".repeat(40)).bind(format!("key-{i}")).bind(format!("op-{i}")).bind(&now).bind(&now)
                .execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO check_consumer(id,project_id,repo_id,task_id,status_epoch,origin,request_key,identity_key,run_id,created_at) SELECT ?,?,?,id,status_epoch,'entry',?,?,?,? FROM task WHERE id=?")
                .bind(format!("consumer-{i}")).bind(&project_id).bind(&repo_id).bind(format!("request-{i}")).bind(format!("key-{i}")).bind(format!("run-{i}")).bind(&now).bind(&task.id)
                .execute(db.pool()).await.unwrap();
        reviewed.push(task);
    }
    let reserved = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workspace_placement WHERE state IN ('reserved','preparing')",
        )
        .fetch_one(db.pool())
        .await
        .unwrap()
    };
    let race = || async {
        let mut tasks = Vec::new();
        for _ in 0..16 {
            tasks.push(seed_task(&db, &project_id, None).await);
        }
        let mut handles = Vec::new();
        for task in tasks {
            let service = Arc::clone(&service);
            let agent = agent.clone();
            handles.push(tokio::spawn(async move {
                service
                    .reserve_claim_workspace(&task, Some(&agent), "coder")
                    .await
                    .map(|_| ())
            }));
        }
        let mut refusals = Vec::new();
        for handle in handles {
            if let Err(error) = handle.await.unwrap() {
                assert!(
                    crate::placement::is_machine_capacity_refusal(&error),
                    "{error:?}"
                );
                refusals.push(error);
            }
        }
        refusals.len()
    };
    // Cap 4, share 3, three queued: one slot is an execution's.
    assert_eq!(race().await, 15);
    assert_eq!(reserved().await, 1);
    // One check is admitted to its own slot: two stay reserved, the
    // machine is full for executions (1 + 1 + 2 kept).
    sqlx::query("UPDATE check_run SET state='running',admitted_at=?,lease_owner='w',lease_until='2099-01-01T00:00:00Z' WHERE id='run-0'")
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
    assert_eq!(race().await, 16);
    assert_eq!(reserved().await, 1);
    // A queued check's Task leaves review: its reservation is gone and
    // exactly one more execution gets in (1 + 1 running check + 1 kept).
    sqlx::query("UPDATE task SET status_epoch=status_epoch+1 WHERE id=?")
        .bind(&reviewed[1].id)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(race().await, 15);
    assert_eq!(reserved().await, 2);
    // The last queued check is cancelled: the last slot is taken, and no
    // more than the cap is ever held.
    sqlx::query("UPDATE check_consumer SET cancelled_at=? WHERE id='consumer-2'")
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(race().await, 15);
    assert_eq!(reserved().await, 3);
    assert_eq!(race().await, 16);
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    let machine = db::machine_capacity::count_machine_capacity(
        &mut tx,
        None,
        Some(4),
        &::config::embedded_machine_id(),
    )
    .await
    .unwrap();
    assert_eq!(machine.active_runs(), 4);
    assert_eq!((machine.reservations, machine.check_runs), (3, 1));
}

#[tokio::test]
async fn environment_daemon_readiness_bridge_is_identical_at_reserve_and_claim() {
    use db::ProjectMachineReadinessRepo;
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    sqlx::query("DELETE FROM execution WHERE id=?")
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE workspace_placement SET state='ready' WHERE id=?")
        .bind(&placement.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE project SET settings=? WHERE id=?")
        .bind(
            json!({"environment":{"checks":[{"name":"cargo","command":"cargo --version"}]}})
                .to_string(),
        )
        .bind(&task.project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let agent = AgentRepo::get_by_id(&*db, placement.agent_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    let daemon_id = placement.daemon_id.as_deref().unwrap();
    sqlx::query("UPDATE daemon SET detected_clis_json=? WHERE id=?")
        .bind(r#"[{"kind":"shell","availability":"authenticated"}]"#)
        .bind(daemon_id)
        .execute(db.pool())
        .await
        .unwrap();
    let registry = Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers());
    let (connection, mut outbound) =
        crate::daemon_transport::DaemonConnection::new(daemon_id.into());
    let id = connection.id();
    registry.register(daemon_id.into(), connection);
    registry.dispatch_incoming_for_connection(daemon_id,id,api_types::DaemonFrame::Notification{
            method:api_types::METHOD_DAEMON_HANDSHAKE.into(),
            params:json!({"protocol_revision":api_types::DAEMON_PROTOCOL_REVISION,"capabilities":["workspace.v1",api_types::DAEMON_CAPABILITY_JOURNAL_ACK,api_types::DAEMON_CAPABILITY_USAGE_REPORTS],"executor_capabilities":{"shell":{"structured_events":true,"usage":true,"resume":true,"cancel_ack":true,"terminal_observed":true}},"workspace_run_policy":{"allowed_purposes":["environment_setup","ci_step"]}})});
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_daemon_connections(registry);
    let environment: api_types::ProjectEnvironment =
        serde_json::from_value(json!({"checks":[{"name":"cargo","command":"cargo --version"}]}))
            .unwrap();
    let machine = db::EnvironmentMachine::from_placement(&placement);
    for state in ["missing", "unknown", "stale"] {
        sqlx::query("DELETE FROM project_machine_readiness WHERE project_id=?")
            .bind(&task.project_id)
            .execute(db.pool())
            .await
            .unwrap();
        if state != "missing" {
            let row = crate::placement::environment::unknown_record(
                &task.project_id,
                machine.clone(),
                &environment,
            );
            db.put_readiness(row, None).await.unwrap();
            if state == "stale" {
                sqlx::query("UPDATE project_machine_readiness SET status='not_ready', checks_digest='old' WHERE project_id=?").bind(&task.project_id).execute(db.pool()).await.unwrap();
            }
        }
        let admission = service
            .reserve_claim_workspace(&task, Some(&agent), "interactive")
            .await
            .unwrap();
        // Claim checks placement inside the Task's claim step.
        let lease = crate::test_support::TestTaskLease::claim(&db, &task.id).await;
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        lease
            .run(service.check_claim_placement_in_tx(&mut tx, &task, &admission))
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        lease.release().await;
        assert!(
            outbound.try_recv().is_err(),
            "{state}: daemon admission never proactively probes or runs another workspace"
        );
    }
    sqlx::query("DELETE FROM project_machine_readiness WHERE project_id=?")
        .bind(&task.project_id)
        .execute(db.pool())
        .await
        .unwrap();
    let admission = service
        .reserve_claim_workspace(&task, Some(&agent), "interactive")
        .await
        .unwrap();
    let mut failed =
        crate::placement::environment::unknown_record(&task.project_id, machine, &environment);
    failed.status = db::EnvironmentReadinessStatus::NotReady;
    failed.failing_checks = vec![db::ReadinessCheckFailure {
        name: "cargo".into(),
        output_tail: "missing".into(),
    }];
    db.put_readiness(failed, None).await.unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    assert!(matches!(
        service
            .check_claim_placement_in_tx(&mut tx, &task, &admission)
            .await,
        Err(ServiceError::PlacementUnavailable(_))
    ));
    tx.rollback().await.unwrap();
    assert!(matches!(
        service
            .reserve_claim_workspace(&task, Some(&agent), "interactive")
            .await,
        Err(ServiceError::PlacementUnavailable(_))
    ));
}
