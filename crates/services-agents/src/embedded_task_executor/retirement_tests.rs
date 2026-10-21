use super::*;
use agent_runtime::{
    core::{
        catalog::{ModelLimits, ResolvedModelProfile},
        content::UserInput,
        ids::SessionId,
        provider::{Capabilities, FinishReason, ModelId, ProviderStreamEvent},
    },
    provider::fake::{usage_event, FakeProvider, ScriptedStream},
    runtime::{RuntimeBuilder, StartSession},
};
use db::{AgentSessionRepo, CreateAgentIdentity, CreateAgentProfile, ScopedMemoryRepository};

const USER: &str = "10000000-0000-4000-8000-000000000001";
const AGENT: &str = "10000000-0000-4000-8000-000000000002";
const PROFILE: &str = "10000000-0000-4000-8000-000000000003";
const PROJECT: &str = "10000000-0000-4000-8000-000000000004";
const TASK: &str = "10000000-0000-4000-8000-000000000005";
const EXECUTION: &str = "10000000-0000-4000-8000-000000000006";
const CREDENTIAL: &str = "10000000-0000-4000-8000-000000000009";

struct Fixture {
    db: Arc<SqliteDb>,
    executor: EmbeddedTaskExecutor,
    session: db::AgentSession,
    context: ExecutionContext,
    provider: Arc<FakeProvider>,
    _worktree: tempfile::TempDir,
    _logs: tempfile::TempDir,
}

fn provider(replies: &[&str]) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        replies
            .iter()
            .map(|text| {
                ScriptedStream::new(vec![
                    ProviderStreamEvent::TextDelta {
                        text: (*text).to_owned(),
                    },
                    usage_event(600, 60),
                    ProviderStreamEvent::Finish {
                        reason: FinishReason::Stop,
                    },
                ])
            })
            .collect(),
    ))
}

async fn fixture(role: &str, replies: &[&str]) -> Fixture {
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        fixture_inner(role, replies),
    )
    .await
    .expect("Task rotation fixture must finish")
}

async fn fixture_inner(role: &str, replies: &[&str]) -> Fixture {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO user (id, email, password_hash, created_at, updated_at) VALUES (?, 'task-rotation@example.test', 'test', ?, ?)")
        .bind(USER).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let service = EmbeddedAgentService::new(db.clone(), b"task-manifest-retirement-test");
    service
        .protected_store()
        .create_credential(
            CREDENTIAL,
            USER,
            "openai",
            "test",
            forge_agent_host::Secret::new("unused-test-key"),
            &now,
        )
        .await
        .unwrap();
    AgentRepo::create_identity_with_profile(
        &*db,
        CreateAgentIdentity {
            id: AGENT.into(),
            name: "Task rotation".into(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(USER.into()),
            visibility: "account".into(),
            account_permission_ceiling: "{}".into(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: PROFILE.into(),
            identity_id: AGENT.into(),
            backend_kind: "native".into(),
            executor_type: "embedded".into(),
            provider: Some("openai".into()),
            model: Some("fake".into()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".into(),
            tool_policy_json: "{}".into(),
            config_json: r#"{"base_url":"https://unused.invalid/v1"}"#.into(),
            credential_ref: Some(CREDENTIAL.into()),
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO project (id, name, owner_id, created_at, updated_at) VALUES (?, 'test', ?, ?, ?)")
        .bind(PROJECT).bind(USER).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO project_member (project_id, user_id, role, created_at, updated_at) VALUES (?, ?, 'owner', ?, ?)")
        .bind(PROJECT).bind(USER).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task (id, project_id, title, task_type, status, created_at, updated_at) VALUES (?, ?, 'rotation', 'planning_task', ?, ?, ?)")
        .bind(TASK).bind(PROJECT).bind(if role == "reviewer" { "review" } else { "in_progress" })
        .bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task_role_assignment (id, task_id, role_name, assignee_type, assignee_id, created_at, updated_at) VALUES ('assignment', ?, ?, 'agent', ?, ?, ?)")
        .bind(TASK).bind(role).bind(AGENT).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO execution (id, task_id, agent_id, role, status, created_at, updated_at) VALUES (?, ?, ?, ?, 'running', ?, ?)")
        .bind(EXECUTION).bind(TASK).bind(AGENT).bind(role).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let worktree = tempfile::tempdir().unwrap();
    let cloned = tokio::process::Command::new("git")
        .args(["clone", "--shared", "--no-checkout", "--"])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .arg(worktree.path())
        .output()
        .await
        .unwrap();
    assert!(cloned.status.success(), "local fixture clone: {cloned:?}");
    sqlx::query("INSERT INTO workspace (id, task_id, repo_id, worktree_path, branch, status, created_at, updated_at) VALUES ('workspace', ?, 'historical-repo', ?, 'test', 'ready', ?, ?)")
        .bind(TASK).bind(worktree.path().to_string_lossy().as_ref()).bind(&now).bind(&now)
        .execute(db.pool()).await.unwrap();
    let session = service
        .create_or_resume_session(CreateScopedSession {
            actor_user_id: USER.into(),
            identity_id: AGENT.into(),
            profile_id: Some(PROFILE.into()),
            scope: RequestedCanonicalScope::Task {
                task_id: TASK.into(),
                role: role.into(),
            },
        })
        .await
        .unwrap();

    // Publish A before the race, including for the reviewer's ephemeral turns.
    let store = service.protected_store();
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .provider_name("fake")
        .provider(provider(&["published history"]))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(32_000, 28_000, 4_000),
        ))
        .session_store(store.clone())
        .checkpoint_store(store.clone())
        .build()
        .unwrap();
    let handle = runtime
        .start_session(StartSession::create(
            SessionId::new(session.runtime_session_id.as_deref().unwrap()),
            vec![],
        ))
        .await
        .unwrap();
    handle.run(UserInput::text("seed A")).await.unwrap();
    handle.shutdown().await.unwrap();
    drop(handle);
    drop(runtime);

    let provider = provider(replies);
    let backend =
        Arc::new(NativeAgentRuntimeBackend::new(store).with_provider_override(provider.clone()));
    let executor =
        EmbeddedTaskExecutor::new(db.clone(), Arc::new(service.with_native_backend(backend)));
    let logs = tempfile::tempdir().unwrap();
    let mut config = serde_json::json!({
        "executor_type": "embedded", "agent_id": AGENT, "profile_id": PROFILE,
        "provider": "openai", "model": "fake", "config": {"base_url": "https://unused.invalid/v1"},
        TASK_ROLE_MARKER: role,
    });
    executors::mark_worktree_read_only(&mut config);
    let context = ExecutionContext {
        task_id: TASK.into(),
        execution_id: EXECUTION.into(),
        worktree_path: worktree.path().to_string_lossy().into_owned(),
        description: "Review this Task".into(),
        agent_config: config,
        logs_path: logs
            .path()
            .join("execution.jsonl")
            .to_string_lossy()
            .into_owned(),
        heartbeat_interval_seconds: 30,
        max_turns: None,
        log_sender: None,
    };
    Fixture {
        db,
        executor,
        session,
        context,
        provider,
        _worktree: worktree,
        _logs: logs,
    }
}

async fn assert_manifest_uses_rotated_session(fixture: &Fixture, result: &ExecutionResult) {
    assert_eq!(result.status, ExecutionOutcome::Completed, "{result:?}");
    let original = fixture
        .db
        .get_agent_session(&fixture.session.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.status, "replaced");
    let fresh = fixture
        .db
        .get_agent_session(original.replaced_by_session_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(fresh.id, original.id);
    assert_ne!(fresh.runtime_session_id, original.runtime_session_id);
    assert_eq!(result.agent_session_id, fresh.runtime_session_id);
    assert_eq!(
        fixture.db.list_agent_sessions(AGENT).await.unwrap().len(),
        2
    );
    let manifests = fixture
        .db
        .list_context_manifests_scoped(AGENT, Some(&fresh.context_scope_id), 10)
        .await
        .unwrap();
    assert_eq!(manifests.len(), 1);
    let manifest = &manifests[0];
    assert_eq!(
        manifest.agent_session_id.as_deref(),
        Some(fresh.id.as_str()),
        "final runtime B must record evidence against host B"
    );
    let link = RuntimeContextManifestLink {
        turn_id: manifest.runtime_manifest_id.clone().unwrap(),
        schema_version: 1,
        context_fingerprint: String::new(),
        cache_plan_fingerprint: String::new(),
        runtime_manifest_fingerprint: String::new(),
        segments: vec![],
        summaries: vec![],
        lossless_summaries: vec![],
        lcm_timeline_id: None,
        lcm_binding_revision: None,
        lcm_store_revision: None,
    };
    assert_eq!(
        manifest.id,
        runtime_manifest_id(AGENT, &fresh.id, EXECUTION, &link).to_string()
    );
    assert_ne!(
        manifest.id,
        runtime_manifest_id(AGENT, &original.id, EXECUTION, &link).to_string()
    );
    assert!(fixture.executor.active.read().await.is_empty());
}

#[tokio::test]
async fn task_retirement_between_selection_and_open_records_rotated_host_manifest() {
    let fixture = fixture("worker", &["Task finished"]).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    fixture
        .executor
        .backend
        .set_open_barrier_for_test(barrier.clone());
    let (result, ()) = tokio::try_join!(fixture.executor.execute(fixture.context.clone()), async {
        tokio::time::timeout(std::time::Duration::from_secs(20), barrier.wait())
            .await
            .expect("Task executor must reach native open");
        let progress = fixture
            .executor
            .embedded_agents
            .protected_store()
            .journals()
            .retire_published(fixture.session.runtime_session_id.as_deref().unwrap())
            .await
            .unwrap();
        assert_eq!(progress.retired_domains, 2);
        barrier.wait().await;
        Ok::<(), ExecutorError>(())
    })
    .unwrap();
    assert_manifest_uses_rotated_session(&fixture, &result).await;
    assert_eq!(fixture.provider.requests().len(), 1);
}

#[tokio::test]
async fn review_correction_rotation_records_final_host_session_manifest() {
    let fixture = fixture(
        "reviewer",
        &[
            "Review without a result block",
            r#"{"result":"pass","reason":"checked"}"#,
        ],
    )
    .await;
    let contract = serde_json::json!({
        "execution_id": EXECUTION, "policy": "test", "commit_sha": "candidate", "base_sha": "base", "digest": "contract",
        "context": {"project_id": PROJECT, "task_id": TASK, "task_scope": {}, "linked_documents": [],
                    "requirements": [], "required_checks": [], "source_digest": "source"},
    });
    let _: api_types::ReviewContract = serde_json::from_value(contract.clone()).unwrap();
    sqlx::query("INSERT INTO execution_review_contract VALUES (?, ?, 'contract', 'source', ?, ?)")
        .bind(EXECUTION)
        .bind(TASK)
        .bind(contract.to_string())
        .bind(db::now_rfc3339())
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    fixture
        .executor
        .backend
        .set_open_barrier_for_test(barrier.clone());
    let (result, ()) = tokio::try_join!(fixture.executor.execute(fixture.context.clone()), async {
        tokio::time::timeout(std::time::Duration::from_secs(20), barrier.wait())
            .await
            .expect("Task executor must reach native open");
        // The first review already prepared A and opens ephemerally. Its
        // correction sees the new tombstone and must rotate to B.
        let progress = fixture
            .executor
            .embedded_agents
            .protected_store()
            .journals()
            .retire_published(fixture.session.runtime_session_id.as_deref().unwrap())
            .await
            .unwrap();
        assert_eq!(progress.retired_domains, 2);
        barrier.wait().await;
        Ok::<(), ExecutorError>(())
    })
    .unwrap();
    assert_manifest_uses_rotated_session(&fixture, &result).await;
    assert_eq!(fixture.provider.requests().len(), 2);
    assert_eq!(result.usage_reports.len(), 2);
    assert!(
        result.usage_reports[0].report_id.starts_with(&format!(
            "native:{}:",
            fixture.session.runtime_session_id.as_deref().unwrap()
        )),
        "the initial review must run on A"
    );
    assert!(
        result.usage_reports[1].report_id.starts_with(&format!(
            "native:{}:",
            result.agent_session_id.as_deref().unwrap()
        )),
        "the correction must run on B"
    );
    let text = result.summary.as_deref().unwrap();
    assert!(text.contains("Review without a result block"));
    assert_eq!(
        ::review::contract::parse_assessment(text).unwrap().result,
        api_types::ReviewResult::Pass
    );
}
