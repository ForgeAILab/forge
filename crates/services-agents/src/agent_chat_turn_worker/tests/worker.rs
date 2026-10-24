//! Tests for Agent Chat worker lifecycle, cancellation, and usage admission.

use super::*;

struct AuthorityOnlyRunner;

#[async_trait::async_trait]
impl AgentChatTurnRunner for AuthorityOnlyRunner {
    async fn run_turn(
        &self,
        _job: &AgentChatTurnJob,
        _cancellation: CancellationToken,
    ) -> Result<CompletedAgentChatTurn> {
        Err(ServiceError::invalid_operation(
            "forged authority test must stop before provider execution",
        ))
    }
}

async fn seed_chat_user(db: &SqliteDb, id: &str, email: &str, now: &str) {
    db::UserRepo::create_user(
        db,
        &db::User {
            id: id.to_owned(),
            email: email.to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.to_owned(),
            updated_at: now.to_owned(),
        },
    )
    .await
    .expect("chat test user creates");
}

struct BlockingTurnRunner {
    entered: tokio::sync::mpsc::UnboundedSender<(String, CancellationToken)>,
    cancelled: tokio::sync::mpsc::UnboundedSender<String>,
}
#[async_trait::async_trait]
impl AgentChatTurnRunner for BlockingTurnRunner {
    async fn run_turn(
        &self,
        job: &AgentChatTurnJob,
        cancellation: CancellationToken,
    ) -> Result<CompletedAgentChatTurn> {
        self.entered
            .send((job.id.clone(), cancellation.clone()))
            .unwrap();
        cancellation.cancelled().await;
        self.cancelled.send(job.id.clone()).unwrap();
        Err(ServiceError::invalid_operation("fixture turn cancelled"))
    }
}

async fn seed_restart_chat(db: &SqliteDb) -> String {
    let now = now_rfc3339();
    seed_chat_user(db, "turn-owner", "turn@example.test", &now).await;
    db::AgentRepo::create_identity_with_profile(
        db,
        db::CreateAgentIdentity {
            id: "turn-agent".to_owned(),
            name: "Turn Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("turn-owner".to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        db::CreateAgentProfile {
            id: "turn-profile".to_owned(),
            identity_id: "turn-agent".to_owned(),
            backend_kind: "cli".to_owned(),
            executor_type: "codex".to_owned(),
            provider: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    db::AgentChatRepo::get_main_chat(db, "turn-owner")
        .await
        .unwrap()
        .unwrap()
        .id
}

async fn seed_restart_job(db: &SqliteDb, chat: &str, id: &str) {
    let now = now_rfc3339();
    let message = format!("{id}-message");
    sqlx::query("INSERT INTO agent_chat_message (id, chat_id, sequence, author_type, author_id, content, status, correlation_id, created_at) VALUES (?, ?, (SELECT COALESCE(MAX(sequence), 0) + 1 FROM agent_chat_message WHERE chat_id = ?), 'user', 'turn-owner', 'hello', 'complete', ?, ?)")
            .bind(&message).bind(chat).bind(chat).bind(id).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_chat_turn_job (id, chat_id, triggering_message_id, responder_identity_id, profile_id, canonical_scope_type, canonical_scope_id, dedupe_key, correlation_id, created_at, updated_at) VALUES (?, ?, ?, 'turn-agent', 'turn-profile', 'agent_chat', ?, ?, ?, ?, ?)")
            .bind(id).bind(chat).bind(&message).bind(chat).bind(id).bind(id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
}

#[tokio::test]
async fn supervised_chat_panic_drains_in_flight_turn_and_same_chat_accepts_new_turn() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let db = worker_test_db().await;
    let chat = seed_restart_chat(&db).await;
    seed_restart_job(&db, &chat, "turn-one").await;
    seed_restart_job(&db, &chat, "turn-two").await;
    let (entered, mut turns) = tokio::sync::mpsc::unbounded_channel();
    let (cancelled, mut cancellations) = tokio::sync::mpsc::unbounded_channel();
    let worker = Arc::new(AgentChatTurnWorker::with_runner(
        Arc::clone(&db),
        Arc::new(BlockingTurnRunner { entered, cancelled }),
    ));
    let registry = crate::worker_runtime::PeriodicWorkers::new(Arc::clone(&db));
    let calls = Arc::new(AtomicUsize::new(0));
    let panic_poll = Arc::new(tokio::sync::Notify::new());
    let (tx, rx) = watch::channel(false);
    let handle = Arc::clone(&worker).start_with_claim(&registry, rx, Duration::from_secs(5), {
            let calls = Arc::clone(&calls);
            let panic_poll = Arc::clone(&panic_poll);
            move |worker, capacity| {
                let calls = Arc::clone(&calls);
                let panic_poll = Arc::clone(&panic_poll);
                async move {
                    match calls.fetch_add(1, Ordering::SeqCst) {
                        1 => {
                            panic_poll.notified().await;
                            // The first turn is still executing when the next poll panics.
                            let active = db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*worker.db, "turn-one").await?.unwrap();
                            assert_eq!(active.status, db::AgentChatTurnState::Leased);
                            panic!("poller panicked with a live turn");
                        }
                        2 => {
                            let old = db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*worker.db, "turn-one").await?.unwrap();
                            assert_eq!(old.status, db::AgentChatTurnState::Queued, "restart waits for the cancelled turn's lease refund");
                            assert!(old.lease_owner.is_none());
                            assert_eq!(old.attempt_count, 0);
                            // Model cancelling the old queued message, so the next
                            // queued turn proves this SAME chat is no longer fenced.
                            sqlx::query("UPDATE agent_chat_turn_job SET status = 'cancelled', version = version + 1 WHERE id = 'turn-one'").execute(worker.db.pool()).await?;
                        }
                        _ => {}
                    }
                    worker.claim_available(capacity).await
                }
            }
        });
    let (first, first_token) = tokio::time::timeout(Duration::from_secs(5), turns.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, "turn-one");
    panic_poll.notify_one();
    let cancelled = tokio::time::timeout(Duration::from_secs(5), cancellations.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancelled, first);
    assert!(first_token.is_cancelled());
    let (next, _) = tokio::time::timeout(Duration::from_secs(5), turns.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next, "turn-two");
    assert_eq!(registry.status().await.unwrap()[0].restart_count, 1);
    tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();
    let next = db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &next)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.status, db::AgentChatTurnState::Queued);
    assert!(next.lease_owner.is_none());
}

#[tokio::test]
async fn aborted_turn_cancels_driver_and_stops_renewing_its_lease() {
    const RENEW: Duration = Duration::from_millis(50);
    async fn lease_version(db: &SqliteDb) -> i64 {
        db::AgentChatTurnJobRepo::get_agent_chat_turn_job(db, "aborted-turn")
            .await
            .unwrap()
            .unwrap()
            .version
    }
    let db = worker_test_db().await;
    let chat = seed_restart_chat(&db).await;
    seed_restart_job(&db, &chat, "aborted-turn").await;
    let (entered, mut turns) = tokio::sync::mpsc::unbounded_channel();
    let (cancelled, _cancellations) = tokio::sync::mpsc::unbounded_channel();
    let worker = Arc::new(
        AgentChatTurnWorker::with_runner(
            Arc::clone(&db),
            Arc::new(BlockingTurnRunner { entered, cancelled }),
        )
        .with_lease_renew_interval(RENEW),
    );
    let job = worker.claim_one().await.unwrap().unwrap();
    let handle = tokio::spawn(async move {
        worker.process_claimed(job, CancellationToken::new()).await;
    });
    let (_, token) = tokio::time::timeout(Duration::from_secs(5), turns.recv())
        .await
        .unwrap()
        .unwrap();
    // The renewer is live while the turn runs, so the check below can
    // only pass because the abort stopped it.
    let claimed = lease_version(&db).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while lease_version(&db).await == claimed {
            tokio::time::sleep(RENEW).await;
        }
    })
    .await
    .expect("the lease is renewed while the turn runs");
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    assert!(
        token.is_cancelled(),
        "the provider driver also follows turn lifetime"
    );
    // Let a renewal that was already in flight at the abort finish.
    tokio::time::sleep(RENEW * 3).await;
    let settled = lease_version(&db).await;
    tokio::time::sleep(RENEW * 10).await;
    assert_eq!(
        lease_version(&db).await,
        settled,
        "no leaked renewal after turn abort"
    );
    sqlx::query("UPDATE agent_chat_turn_job SET leased_until = '2000-01-01T00:00:00Z' WHERE id = 'aborted-turn'").execute(db.pool()).await.unwrap();
    let recovering =
        AgentChatTurnWorker::with_runner(Arc::clone(&db), Arc::new(AuthorityOnlyRunner));
    recovering.recover_expired().await.unwrap();
    let after = db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, "aborted-turn")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        after.status,
        db::AgentChatTurnState::Leased,
        "an aborted turn can expire normally"
    );
}

#[tokio::test]
async fn supervised_chat_poller_restarts_panic_warns_without_cancelling_admission_and_stops() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct InFlight(Arc<AtomicBool>);
    impl Drop for InFlight {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }
    let db = worker_test_db().await;
    let workers = crate::worker_runtime::PeriodicWorkers::new(Arc::clone(&db));
    let worker = Arc::new(AgentChatTurnWorker::with_runner(
        db,
        Arc::new(AuthorityOnlyRunner),
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let in_flight = Arc::new(AtomicBool::new(false));
    let next = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let (tx, rx) = watch::channel(false);
    let handle = worker.start_with_claim(&workers, rx, Duration::from_millis(100), {
        let calls = Arc::clone(&calls);
        let in_flight = Arc::clone(&in_flight);
        let next = Arc::clone(&next);
        let resume = Arc::clone(&resume);
        move |_, capacity| {
            let calls = Arc::clone(&calls);
            let in_flight = Arc::clone(&in_flight);
            let next = Arc::clone(&next);
            let resume = Arc::clone(&resume);
            async move {
                assert_eq!(capacity, MAX_ACTIVE_TURNS);
                match calls.fetch_add(1, Ordering::SeqCst) {
                    0 => panic!("chat admission tick panic"),
                    1 => {
                        in_flight.store(true, Ordering::SeqCst);
                        let _in_flight = InFlight(in_flight);
                        resume.notified().await;
                    }
                    _ => next.notify_one(),
                }
                Ok(Vec::new())
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !workers.status().await.unwrap()[0]
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("tick running longer than 100ms"))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(in_flight.load(Ordering::SeqCst));
    assert_eq!(workers.status().await.unwrap()[0].restart_count, 1);
    resume.notify_one();
    tokio::time::timeout(Duration::from_secs(5), next.notified())
        .await
        .unwrap();
    tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();
    assert!(!workers.status().await.unwrap()[0].running);
    assert!(!in_flight.load(Ordering::SeqCst));
    assert!(calls.load(Ordering::SeqCst) >= 3);
}

#[tokio::test]
async fn supervised_chat_shutdown_drains_and_refunds_active_claim() {
    let db = worker_test_db().await;
    let now = now_rfc3339();
    seed_chat_user(&db, "shutdown-owner", "shutdown@example.test", &now).await;
    let chat = db::AgentChatRepo::get_main_chat(&*db, "shutdown-owner")
        .await
        .unwrap()
        .unwrap();
    sqlx::query("INSERT INTO agent_chat_message (id, chat_id, sequence, author_type, author_id, content, status, correlation_id, created_at) VALUES ('shutdown-message', ?, 1, 'user', 'shutdown-owner', 'hello', 'complete', 'shutdown-correlation', ?)")
            .bind(&chat.id).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_chat_turn_job (id, chat_id, triggering_message_id, canonical_scope_type, canonical_scope_id, dedupe_key, correlation_id, created_at, updated_at) VALUES ('shutdown-turn', ?, 'shutdown-message', 'agent_chat', ?, 'shutdown-key', 'shutdown-correlation', ?, ?)")
            .bind(&chat.id).bind(&chat.id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let workers = crate::worker_runtime::PeriodicWorkers::new(Arc::clone(&db));
    let worker = Arc::new(AgentChatTurnWorker::with_runner(
        Arc::clone(&db),
        Arc::new(AuthorityOnlyRunner),
    ));
    let claimed = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let (tx, rx) = watch::channel(false);
    let handle = worker.start_with_claim(&workers, rx, Duration::from_secs(5), {
        let claimed = Arc::clone(&claimed);
        let resume = Arc::clone(&resume);
        move |worker, capacity| {
            let claimed = Arc::clone(&claimed);
            let resume = Arc::clone(&resume);
            async move {
                let jobs = worker.claim_available(capacity).await?;
                assert_eq!(jobs.len(), 1);
                claimed.notify_one();
                resume.notified().await;
                Ok(jobs)
            }
        }
    });
    tokio::time::timeout(Duration::from_secs(5), claimed.notified())
        .await
        .unwrap();
    tx.send(true).unwrap();
    resume.notify_one();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();
    let job = db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, "shutdown-turn")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.status, db::AgentChatTurnState::Queued);
    assert_eq!(job.attempt_count, 0);
    assert_eq!(job.invocation_count, 1);
    assert!(job.lease_owner.is_none());
    assert!(!workers.status().await.unwrap()[0].running);
}

#[test]
fn daemon_tool_refusals_do_not_classify_chat_turns_as_configuration_failures() {
    for error in [
        ServiceError::PlacementUnavailable(crate::placement::PlacementUnavailable {
            task_id: "task".into(),
            repo_id: "repo".into(),
            rejected_candidates: vec![],
        }),
        ServiceError::DaemonUpgradeRequired {
            daemon_id: "owner".into(),
        },
        ServiceError::WorkspaceResetRequired {
            task_id: "task".into(),
            reason: "lost".into(),
        },
    ] {
        assert_eq!(TurnRunError::from(error).failure, TurnFailure::Unclassified);
    }
}

#[test]
fn load_turn_errors_preserve_typed_authority_and_configuration() {
    for error in [
        ServiceError::AgentPaused {
            agent_id: "agent".into(),
        },
        ServiceError::ProjectPaused {
            project_id: "project".into(),
        },
        ServiceError::AuthorizationDenied {
            message: "denied".into(),
        },
    ] {
        assert_eq!(TurnRunError::from(error).failure, TurnFailure::Authority);
    }
    for entity in ["agent_profile", "agent_identity"] {
        assert_eq!(
            TurnRunError::from(ServiceError::not_found(entity, "missing")).failure,
            TurnFailure::Configuration
        );
    }
    assert_eq!(
        TurnRunError::from(ServiceError::TurnFailure {
            failure: TurnFailure::Configuration,
            error: Box::new(ServiceError::invalid_operation("typed evidence"))
        })
        .failure,
        TurnFailure::Configuration
    );
    assert_eq!(
        TurnRunError::from(ServiceError::invalid_operation(
            "config usage limit credential"
        ))
        .failure,
        TurnFailure::Unclassified
    );
}

#[tokio::test]
async fn server_shutdown_refunds_claim_without_changing_invocation_identity() {
    let db = worker_test_db().await;
    let now = now_rfc3339();
    seed_chat_user(&db, "shutdown-owner", "shutdown@example.test", &now).await;
    let chat = db::AgentChatRepo::get_main_chat(&*db, "shutdown-owner")
        .await
        .unwrap()
        .unwrap();
    sqlx::query("INSERT INTO agent_chat_message (id, chat_id, sequence, author_type, author_id, content, status, correlation_id, created_at) VALUES ('shutdown-message', ?, 1, 'user', 'shutdown-owner', 'hello', 'complete', 'shutdown-correlation', ?)")
            .bind(&chat.id).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_chat_turn_job (id, chat_id, triggering_message_id, canonical_scope_type, canonical_scope_id, dedupe_key, correlation_id, created_at, updated_at) VALUES ('shutdown-turn', ?, 'shutdown-message', 'agent_chat', ?, 'shutdown-key', 'shutdown-correlation', ?, ?)")
            .bind(&chat.id).bind(&chat.id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let worker = AgentChatTurnWorker::with_runner(db.clone(), Arc::new(AuthorityOnlyRunner));
    let job = worker.claim_one().await.unwrap().unwrap();
    assert_eq!(job.attempt_count, 1);
    let token = CancellationToken::new();
    token.cancel();
    worker.process_claimed(job, token).await;
    let recovered = db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, "shutdown-turn")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.status, db::AgentChatTurnState::Queued);
    assert_eq!(recovered.attempt_count, 0);
    assert_eq!(recovered.invocation_count, 1);
    assert!(recovered.lease_owner.is_none());
    assert!(recovered.failure_class.is_none());
    assert!(recovered.retry_decision.is_none());
}

#[test]
fn cli_sandbox_is_job_scoped() {
    assert!(chat_sandbox_path("job-a") != chat_sandbox_path("job-b"));
}

#[test]
fn chat_terminalization_rejects_duration_overflow_without_clamping() {
    let too_long = Duration::from_millis(
        u64::try_from(i64::MAX)
            .expect("i64::MAX fits in u64")
            .checked_add(1)
            .expect("test duration fits in u64"),
    );
    assert!(checked_duration_ms(too_long).is_err());
}

#[tokio::test]
async fn forged_cross_account_chat_job_fails_before_usage_admission() {
    let db = worker_test_db().await;
    let now = db::now_rfc3339();
    for (id, email) in [
        ("chat-owner", "chat-owner@example.test"),
        ("agent-owner", "agent-owner@example.test"),
    ] {
        db::UserRepo::create_user(
            &*db,
            &db::User {
                id: id.to_owned(),
                email: email.to_owned(),
                password_hash: "test".to_owned(),
                display_name: None,
                is_admin: false,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("test user");
    }

    let agent = db::AgentRepo::create_identity_with_profile(
        &*db,
        db::CreateAgentIdentity {
            id: "forged-agent".to_owned(),
            name: "Forged Agent".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: db::AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some("agent-owner".to_owned()),
            visibility: "account".to_owned(),
            account_permission_ceiling: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        db::CreateAgentProfile {
            id: "forged-profile".to_owned(),
            identity_id: "forged-agent".to_owned(),
            backend_kind: "cli".to_owned(),
            executor_type: "codex".to_owned(),
            provider: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            tool_policy_json: "{}".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("test agent");
    let chat = db::AgentChatRepo::get_main_chat(&*db, "chat-owner")
        .await
        .expect("test chat lookup")
        .expect("test Main Chat");
    let trigger = db::AgentChatMessageRepo::append_agent_chat_message(
        &*db,
        db::CreateAgentChatMessage {
            id: "owner-trigger".to_owned(),
            chat_id: chat.id.clone(),
            sequence: 0,
            author_type: db::AgentChatMessageAuthorType::User,
            author_id: Some("chat-owner".to_owned()),
            content: "hello".to_owned(),
            content_guard_json: "{}".to_owned(),
            sensitivity: "public".to_owned(),
            status: db::AgentChatMessageStatus::Complete,
            outcome: None,
            model: None,
            profile_id: None,
            session_id: None,
            context_manifest_id: None,
            token_usage_json: None,
            duration_ms: None,
            error: None,
            correlation_id: "correlation".to_owned(),
            causation_id: None,
            handoff_id: None,
            source_type: "native".to_owned(),
            source_id: None,
            source_message_id: None,
            source_room_id: None,
            source_conversation_id: None,
            source_sequence: None,
            source_metadata_json: "{}".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("test trigger");
    let job = db::AgentChatTurnJobRepo::create_agent_chat_turn_job(
        &*db,
        db::CreateAgentChatTurnJob {
            id: "forged-job".to_owned(),
            chat_id: chat.id.clone(),
            triggering_message_id: trigger.id,
            responder_identity_id: agent.id,
            profile_id: "forged-profile".to_owned(),
            responder_binding_id: None,
            responder_binding_version: None,
            responder_identity_version: None,
            profile_version: None,
            operating_skill_revision_id: None,
            policy_revision: None,
            policy_digest: None,
            permission_policy_digest: None,
            tool_policy_digest: None,
            admission_digest: None,
            canonical_scope_provenance_json: None,
            canonical_scope_type: "agent_chat".to_owned(),
            canonical_scope_id: chat.id.clone(),
            dedupe_key: "forged-dedupe".to_owned(),
            max_attempts: 3,
            correlation_id: "correlation".to_owned(),
            causation_id: None,
            causation_depth: 0,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("test job");
    let worker = AgentChatTurnWorker::with_runner(Arc::clone(&db), Arc::new(AuthorityOnlyRunner));
    sqlx::query(
        "UPDATE agent_chat_turn_job
             SET status = 'leased', lease_owner = ?, leased_until = ?,
                 attempt_count = 1, version = version + 1, updated_at = ?
             WHERE id = ?",
    )
    .bind(&worker.lease_owner)
    .bind(&now)
    .bind(&now)
    .bind(&job.id)
    .execute(db.pool())
    .await
    .expect("lease test job");
    let leased = db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &job.id)
        .await
        .expect("leased job read")
        .expect("leased job");
    worker
        .process_claimed(leased, CancellationToken::new())
        .await;

    let usage_rows: i64 = sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM pricing_selection)
                  + (SELECT COUNT(*) FROM usage_invocation)
                  + (SELECT COUNT(*) FROM usage_event)",
    )
    .fetch_one(db.pool())
    .await
    .expect("usage rows count");
    assert_eq!(
        usage_rows, 0,
        "forged authority must not write accounting rows"
    );
    let current = db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*db, &job.id)
        .await
        .expect("terminal job read")
        .expect("terminal job");
    assert_eq!(current.status, db::AgentChatTurnState::RetryWait);
}
