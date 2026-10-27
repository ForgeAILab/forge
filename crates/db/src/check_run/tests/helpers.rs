//! Shared database fixtures and test request builders.
use super::*;

pub(super) const NOW: &str = "2026-10-08T00:00:00Z";
pub(super) const UNTIL: &str = "2026-10-08T00:01:00Z";

pub(super) async fn seed(db: &SqliteDb) {
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','project',?,?)")
        .bind(NOW)
        .bind(NOW)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('r','p','repo','main',?,?)").bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','task','todo',?,?)").bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('w','t','r',?,'task/branch','ready',?,?)")
        .bind(std::env::temp_dir().join("check-contract-workspace").to_str().unwrap()).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO daemon(id,machine_id,hostname,os,arch,status,created_at,updated_at) VALUES('m','check-machine','test','linux','aarch64','offline',?,?)").bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
}

pub(super) async fn fixture() -> SqliteDb {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    seed(&db).await;
    db
}

pub(super) fn identity() -> CheckRunIdentity {
    CheckRunIdentity {
        project_id: "p".into(),
        repo_id: "r".into(),
        commit_sha: "a".repeat(40),
        inputs: CheckDigestInput {
            spec: CheckSpec {
                schema_revision: CHECK_SPEC_REVISION,
                scope: CheckScope::Commit,
                commands: vec![CheckCommandSpec {
                    id: "ci:0".into(),
                    shell_text: "cargo test".into(),
                    shell: "bash -lc".into(),
                    working_directory: CheckWorkingDirectory::TaskRoot,
                    environment_keys: Default::default(),
                    timeout_seconds: None,
                    failure_policy: CheckFailurePolicy::StopBundle,
                    cacheability: CheckCacheability::DeclaredControlledInputs,
                    requirement_ids: Default::default(),
                }],
                declares_cleanup: false,
                configured_commands: 1,
                blank_commands: vec![],
                execution_policy: "controlled/1".into(),
            },
            environment: Default::default(),
            environment_identity: CheckEnvironmentIdentity::Attested {
                input_digest: "a".repeat(64),
            },
            execution_revision: CheckExecutionRevision {
                number: 0,
                audit_ref: None,
            },
        },
    }
}

pub(super) fn request(key: &str) -> CheckRunRequest {
    CheckRunRequest {
        identity: identity(),
        request_key: key.into(),
        task_id: Some("t".into()),
        status_epoch: 0,
        origin: CheckConsumerOrigin::Entry,
        purpose: CheckPurpose::EntryCi,
        workspace_id: Some("w".into()),
        machine_id: Some("m".into()),
        wall_timeout_seconds: 1800,
    }
}

pub(super) fn fence(run: &StoredCheckRun) -> CheckRunFence {
    CheckRunFence {
        run_id: run.id.clone(),
        version: run.version,
        lease_generation: run.lease_generation,
        lease_owner: run.lease_owner.clone(),
    }
}

pub(super) fn evidence(outcome: CheckResultOutcome, cleanup: CheckCleanup) -> CheckResultEvidence {
    CheckResultEvidence {
        outcome,
        cleanup,
        commands: vec![CheckCommandOutcome {
            failures: vec![],
            failures_truncated: false,
            index: 0,
            command: "cargo test".into(),
            exit_code: if outcome == CheckResultOutcome::Pass {
                0
            } else {
                1
            },
            stderr_tail: String::new(),
            output_tail: String::new(),
            started_at: NOW.into(),
            finished_at: NOW.into(),
        }],
        output_truncated: false,
        redaction_values: vec![],
        reusable: true,
    }
}

pub(super) async fn cleaning(db: &SqliteDb, req: CheckRunRequest) -> StoredCheckRun {
    let response = db.request_check_run(req).await.unwrap();
    let run = db
        .claim_check_run(
            response.consumer.run_id.as_deref().unwrap(),
            1,
            "worker",
            NOW,
            UNTIL,
        )
        .await
        .unwrap();
    db.transition_check_run(&fence(&run), CheckRunState::Cleaning, NOW)
        .await
        .unwrap()
}

pub(super) async fn complete(
    db: &SqliteDb,
    req: CheckRunRequest,
    outcome: CheckResultOutcome,
    cleanup: CheckCleanup,
) -> StoredCheckResult {
    let run = cleaning(db, req).await;
    db.finish_check_run(&fence(&run), evidence(outcome, cleanup), NOW)
        .await
        .unwrap()
}

/// A fresh run driven to `state` through the repository alone.
pub(super) async fn run_in(db: &SqliteDb, state: CheckRunState) -> StoredCheckRun {
    let id = db
        .request_check_run(request("edge"))
        .await
        .unwrap()
        .consumer
        .run_id
        .unwrap();
    if state == CheckRunState::Queued {
        return db.check_run(&id).await.unwrap().unwrap();
    }
    let run = db
        .claim_check_run(&id, 1, "worker", NOW, UNTIL)
        .await
        .unwrap();
    if state == CheckRunState::Running {
        return run;
    }
    db.transition_check_run(&fence(&run), state, NOW)
        .await
        .unwrap()
}

pub(super) async fn counts(db: &SqliteDb) -> (i64, i64, i64) {
    let run = sqlx::query_scalar("SELECT COUNT(*) FROM check_run")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let result = sqlx::query_scalar("SELECT COUNT(*) FROM check_result")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let consumer = sqlx::query_scalar("SELECT COUNT(*) FROM check_consumer")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(db.pool())
        .await
        .unwrap()
        .is_empty());
    (run, result, consumer)
}

pub(super) async fn legacy_snapshot(db: &SqliteDb) -> Vec<Vec<Vec<Option<String>>>> {
    let mut snapshot = Vec::new();
    for table in [
        "project",
        "repo",
        "task",
        "workspace",
        "daemon",
        "execution",
        "review",
        "domain_event",
    ] {
        let columns = sqlx::query(&format!("PRAGMA table_info({table})"))
            .fetch_all(db.pool())
            .await
            .unwrap();
        let projection = columns
            .iter()
            .map(|row| {
                let name: String = row.get("name");
                format!("CAST(\"{name}\" AS TEXT)")
            })
            .collect::<Vec<_>>()
            .join(",");
        let rows = sqlx::query(&format!("SELECT {projection} FROM {table} ORDER BY id"))
            .fetch_all(db.pool())
            .await
            .unwrap();
        snapshot.push(
            rows.iter()
                .map(|row| {
                    (0..columns.len())
                        .map(|i| row.get::<Option<String>, _>(i))
                        .collect()
                })
                .collect(),
        );
    }
    snapshot
}

/// A second Task `t2` with its own worktree `w2`, at the same commit.
pub(super) async fn seed_second_task(db: &SqliteDb) {
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t2','p','task two','todo',?,?)").bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('w2','t2','r',?,'task/branch-two','ready',?,?)")
        .bind(std::env::temp_dir().join("check-contract-workspace-two").to_str().unwrap()).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
}

/// The same blocking before-work script, asked for by `task` in `workspace`.
pub(super) fn before_work(key: &str, task: &str, workspace: &str) -> CheckRunRequest {
    let mut req = request(key);
    req.task_id = Some(task.into());
    req.workspace_id = Some(workspace.into());
    req.origin = CheckConsumerOrigin::BeforeWork;
    req.purpose = CheckPurpose::BeforeWork;
    let spec = &mut req.identity.inputs.spec;
    spec.scope = CheckScope::Workspace {
        workspace_id: workspace.into(),
        generation: 1,
    };
    spec.commands[0].id = "hook:0".into();
    spec.commands[0].shell_text = "./prepare-worktree".into();
    spec.commands[0].cacheability = CheckCacheability::Uncacheable;
    req
}

pub(super) async fn rows(db: &SqliteDb, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(db.pool())
        .await
        .unwrap()
}
