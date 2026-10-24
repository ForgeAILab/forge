//! Explicitly invoked conversion on a disposable, offline database copy.
use forge_agent_host::SqliteProtectedRuntimeStore;
use services_agents::{
    session_journal_conversion::{
        ConversionCursor, ParkedSession, SessionJournalConversion, PARKED_SESSION_REPORT_LIMIT,
    },
    session_journal_retention::{RetentionCursor, SessionJournalRetention},
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Instant,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn refuse_install_path(path: &Path) -> TestResult<()> {
    // Reject every .forge directory, including a symlink's canonical target.
    // This is stricter than only protecting the invoking user's installation.
    if path
        .components()
        .any(|part| matches!(part, Component::Normal(name) if name == ".forge"))
    {
        return Err(
            "journal dry run refuses installation paths under .forge; use a disposable copy".into(),
        );
    }
    Ok(())
}

fn copy_file(path: &Path) -> TestResult<PathBuf> {
    refuse_install_path(path)?;
    let canonical = path.canonicalize()?;
    refuse_install_path(&canonical)?;
    if !canonical.is_file() {
        return Err("journal dry run needs an existing regular file".into());
    }
    Ok(canonical)
}

async fn physical_bytes(pool: &sqlx::SqlitePool) -> TestResult<serde_json::Value> {
    // Include the tables' own pages and all their indexes. These are allocated
    // dbstat pages, independent of readable canonical bytes or freelist space.
    let legacy: i64 = sqlx::query_scalar("SELECT COALESCE(sum(pgsize),0) FROM dbstat WHERE name IN (SELECT name FROM sqlite_schema WHERE tbl_name='protected_agent_session_state')")
        .fetch_one(pool).await?;
    let journal: i64 = sqlx::query_scalar("SELECT COALESCE(sum(pgsize),0) FROM dbstat WHERE name IN (SELECT name FROM sqlite_schema WHERE tbl_name GLOB 'agent_session_journal_*')")
        .fetch_one(pool).await?;
    Ok(serde_json::json!({"legacy_dbstat":legacy,"journal_dbstat":journal}))
}

async fn legacy_chat_lcm_facts(
    pool: &sqlx::SqlitePool,
    store: &SqliteProtectedRuntimeStore,
) -> TestResult<Vec<serde_json::Value>> {
    use agent_runtime::core::{checkpoint::CheckpointStore, ids::SessionId, store::SessionStore};
    use agent_runtime::harness::LCM_COMPONENT_ID;
    let rows: Vec<(String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT s.id,s.runtime_session_id,s.profile_id,i.selected_profile_id
         FROM agent_session s JOIN agent_identity i ON i.id=s.identity_id
         JOIN agent_context_scope c ON c.id=s.context_scope_id
         JOIN agent_chat chat ON c.scope_type='agent_chat' AND chat.id=c.scope_id
         JOIN protected_agent_session_state l ON l.session_id=s.id
         WHERE chat.kind='account_main' AND s.runtime_session_id IS NOT NULL ORDER BY s.id",
    )
    .fetch_all(pool)
    .await?;
    let mut facts = Vec::new();
    for (owner, runtime, profile, selected) in rows {
        let id = SessionId::new(&runtime);
        let snapshot = SessionStore::load(store, &id)
            .await
            .map_err(|_| "legacy snapshot metadata could not be decoded")?;
        let checkpoint = CheckpointStore::load_latest(store, &id)
            .await
            .map_err(|_| "legacy checkpoint metadata could not be decoded")?;
        let state = |snapshot: &agent_runtime::core::store::SessionSnapshot| {
            let lcm = snapshot.extension_state.get(LCM_COMPONENT_ID);
            serde_json::json!({
                "history_entries":snapshot.history.len(),
                "timeline_id":lcm.and_then(|s| s.value.get("timeline_id")).and_then(serde_json::Value::as_str),
                "dag_revision":lcm.and_then(|s| s.value.get("dag_revision")).and_then(serde_json::Value::as_u64)
            })
        };
        facts.push(serde_json::json!({
            "forge_session_id":owner,"runtime_session_id":runtime,"profile_id":profile,
            "selected_profile_id":selected,"snapshot":snapshot.as_ref().map(&state),
            "checkpoint":checkpoint.as_ref().map(|c| state(&c.snapshot))
        }));
    }
    Ok(facts)
}

#[tokio::test]
#[ignore = "reads metadata from an explicitly selected disposable database copy"]
async fn journal_real_data_lcm_metadata() -> TestResult<()> {
    let path = copy_file(Path::new(&std::env::var("FORGE_JOURNAL_DRYRUN_DB")?))?;
    let secret_path = copy_file(Path::new(&std::env::var(
        "FORGE_JOURNAL_DRYRUN_SECRET_FILE",
    )?))?;
    let key: [u8; 32] = Sha256::digest(std::fs::read(secret_path)?).into();
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(path)
                .read_only(true),
        )
        .await?;
    let store = SqliteProtectedRuntimeStore::new(Arc::new(db::SqliteDb::new(pool.clone())), key, 1);
    println!(
        "{}",
        serde_json::to_string(&legacy_chat_lcm_facts(&pool, &store).await?)?
    );
    pool.close().await;
    Ok(())
}

// Live LCM recovery may publish a repaired ordinary successor. Inspect the
// retained import boundary so exact-import assertions still cover every field.
async fn native_import_counts(
    raw: &forge_agent_host::session_journal::ForgeJournalStorage,
    store: &SqliteProtectedRuntimeStore,
    id: &agent_runtime::core::ids::SessionId,
    ordinary: &agent_runtime::core::store::SessionSnapshot,
    protected: &agent_runtime::core::store::SessionSnapshot,
    first_import: bool,
) -> TestResult<Vec<usize>> {
    use agent_runtime::core::journal::*;
    let projected = forge_agent_host::session_journals::ForgeJournalProjection
        .project(ordinary.clone())
        .map_err(|_| "ordinary projection unavailable")?;
    let mut counts = Vec::new();
    for (engine, role, expected) in [
        (
            store.journals().ordinary(),
            JournalRole::Ordinary,
            &projected,
        ),
        (
            store.journals().protected(),
            JournalRole::Protected,
            protected,
        ),
    ] {
        let root = if first_import && role == JournalRole::Ordinary {
            let domain = JournalDomain {
                storage_id: engine.storage_id(),
                role: JournalRole::Ordinary,
            };
            let members = raw
                .members(&domain, id)
                .await
                .map_err(|_| "native import members unavailable")?;
            let mut roots = Vec::new();
            for ids in members.chunks(128) {
                let bodies = raw
                    .objects(&domain, id, ids)
                    .await
                    .map_err(|_| "native import bodies unavailable")?;
                for (object_id, bytes) in ids.iter().zip(bodies) {
                    let bytes = bytes.ok_or("native import object absent")?;
                    let object = JournalObject {
                        id: object_id.clone(),
                        bytes: bytes.to_vec(),
                    };
                    if let Ok(head) = object.decode::<JournalHead>(JournalObjectKind::Head) {
                        if head.session == *id
                            && head.role == JournalRole::Ordinary
                            && head.revision == JournalRevision(1)
                        {
                            roots.push(object_id.clone());
                        }
                    }
                }
            }
            assert!(
                roots.len() == 1,
                "expected exactly one original ordinary import root"
            );
            JournalRootSelection::Retained {
                root: roots.remove(0),
            }
        } else {
            JournalRootSelection::Latest
        };
        let nonce = db::new_uuid_v4();
        let lease = engine
            .open(
                id,
                JournalOpen {
                    root,
                    expected_revision: None,
                    operation: JournalOperationId::new(&nonce),
                    pin: JournalPinId::new(&nonce),
                    intent: JournalOpenIntent::ReadOnly,
                },
            )
            .await
            .map_err(|e| format!("native inspection failed: {:?}", e.kind))?;
        let imported = JournalReader::new(
            &engine,
            &lease,
            JournalReadLimits {
                objects: 100_000,
                encoded_bytes: 32 * 1024 * 1024,
                entries: 100_000,
            },
        )
        .map_err(|_| "native reader unavailable")?
        .snapshot()
        .await
        .map_err(|_| "native snapshot could not be decoded")?
        .ok_or("native snapshot absent")?;
        assert!(
            imported == *expected,
            "native import differs from its receipt-filtered legacy state"
        );
        counts.push(imported.history.len());
        engine
            .close(lease)
            .await
            .map_err(|_| "native read lease could not close")?;
    }
    Ok(counts)
}

#[tokio::test]
async fn retained_import_inspection_is_exact_after_a_live_successor() -> TestResult<()> {
    use agent_runtime::{
        core::{
            catalog::{ModelLimits, ResolvedModelProfile},
            checkpoint::TurnCheckpoint,
            content::Message,
            ids::SessionId,
            journal::*,
            provider::{Capabilities, ModelId},
            store::SessionSnapshot,
        },
        provider::fake::FakeProvider,
        runtime::{RuntimeBuilder, StartSession},
    };
    let f = forge_agent_host::test_support::FileDb::new().await?;
    let id = SessionId::new("retained-import-inspection");
    let owner = f.ensure_session(&id).await?;
    let db = f.database();
    let raw = forge_agent_host::session_journal::ForgeJournalStorage::new(db.clone(), [7; 32], 1)?;
    let store = Arc::new(SqliteProtectedRuntimeStore::new(db, [7; 32], 1));
    let mut original: SessionSnapshot =
        serde_json::from_value(serde_json::json!({"id":id,"history":[],"updated":1}))?;
    original
        .history
        .push(Message::user("Synthetic retained import"));
    let checkpoint = TurnCheckpoint::session_boundary(original.clone(), 6)?;
    store
        .seed_legacy_bytes(
            &id,
            Some(&serde_json::to_vec(&original)?),
            Some(&serde_json::to_vec(&checkpoint)?),
            false,
        )
        .await?;
    store.convert_legacy_session(&owner, &id).await?;
    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        Vec::new(),
    ));
    let rt = RuntimeBuilder::new(ModelId::new("fake"))
        .provider_name("fake")
        .provider(provider.clone())
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(32_000, 28_000, 4_000),
        ))
        .session_store(store.clone())
        .checkpoint_store(store.clone())
        .build()?;
    let session = rt.start_session(StartSession::resume(id.clone())).await?;
    session.persist().await?;
    assert!(provider.requests().is_empty());
    drop(session);
    drop(rt);
    let domain = JournalDomain {
        storage_id: store.journals().ordinary().storage_id(),
        role: JournalRole::Ordinary,
    };
    assert!(raw.load(&domain, &id).await?.unwrap().revision.0 > 1);
    assert_eq!(
        native_import_counts(&raw, &store, &id, &original, &checkpoint.snapshot, true).await?,
        vec![1, 1]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "resumes only converted Main Chats on an explicitly selected disposable copy"]
async fn journal_real_data_lazy_resume() -> TestResult<()> {
    lazy_resume_copy(false).await
}

#[tokio::test]
#[ignore = "resumes selected Main Chats before background conversion on a disposable copy"]
async fn journal_real_data_unconverted_lazy_resume() -> TestResult<()> {
    lazy_resume_copy(true).await
}

async fn lazy_resume_copy(unconverted: bool) -> TestResult<()> {
    use agent_runtime::{
        core::{
            catalog::{ModelLimits, ResolvedModelProfile},
            checkpoint::CheckpointStore,
            ids::SessionId,
            provider::{Capabilities, ModelId},
            store::SessionStore,
        },
        harness::{
            LcmCoordinator, LcmCoordinatorPolicy, LcmRecoveryPolicy, StaticLcmTimelineResolver,
            LCM_COMPONENT_ID,
        },
        provider::fake::FakeProvider,
        runtime::{CheckpointRecoveryPolicy, RuntimeBuilder, StartSession},
    };
    let path = copy_file(Path::new(&std::env::var("FORGE_JOURNAL_DRYRUN_DB")?))?;
    let secret_path = copy_file(Path::new(&std::env::var(
        "FORGE_JOURNAL_DRYRUN_SECRET_FILE",
    )?))?;
    let key: [u8; 32] = Sha256::digest(std::fs::read(secret_path)?).into();
    let pool = db::create_sqlite_pool(&format!("sqlite://{}", path.display())).await?;
    db::run_migrations(&pool).await?;
    let db = Arc::new(db::SqliteDb::new(pool.clone()));
    let raw = forge_agent_host::session_journal::ForgeJournalStorage::new(db.clone(), key, 1)
        .map_err(|_| "native inspection storage unavailable")?;
    let store = Arc::new(SqliteProtectedRuntimeStore::new(db, key, 1));
    let rows: Vec<(String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT s.runtime_session_id,c.scope_id,s.profile_id,i.selected_profile_id
         FROM agent_session s JOIN agent_identity i ON i.id=s.identity_id
         JOIN agent_context_scope c ON c.id=s.context_scope_id
         JOIN agent_chat chat ON c.scope_type='agent_chat' AND chat.id=c.scope_id
         WHERE chat.kind='account_main' AND s.replaced_by_session_id IS NULL
           AND ((?1=0 AND EXISTS(SELECT 1 FROM agent_session_journal_session j WHERE j.runtime_session_id=s.runtime_session_id AND j.role='ordinary' AND j.published=1 AND j.retired=0))
             OR (?1=1 AND s.profile_id=i.selected_profile_id AND s.id=(
                 SELECT candidate.id FROM agent_session candidate WHERE candidate.identity_id=s.identity_id
                   AND candidate.context_scope_id=s.context_scope_id AND candidate.profile_id=s.profile_id
                   AND candidate.backend_kind='native' AND candidate.replaced_by_session_id IS NULL
                   AND candidate.status IN ('starting','ready','running','degraded','suspended')
                 ORDER BY candidate.status='suspended',candidate.created_at DESC LIMIT 1)
                 AND NOT EXISTS(SELECT 1 FROM agent_session_journal_session j WHERE j.runtime_session_id=s.runtime_session_id AND (j.published=1 OR j.retired=1))))",
    ).bind(unconverted).fetch_all(&pool).await?;
    if rows.is_empty() {
        return Err("no matching Main Chat was available for lazy resume".into());
    }
    let mut facts = Vec::new();
    for (runtime, chat, profile, selected) in rows {
        let id = SessionId::new(&runtime);
        let _foreground = store
            .foreground_session(&id)
            .await
            .map_err(|_| "foreground admission unavailable")?;
        let ordinary = SessionStore::load(&*store, &id)
            .await
            .map_err(|_| "legacy snapshot could not be decoded")?
            .ok_or("legacy snapshot absent")?;
        let protected = CheckpointStore::load_latest(&*store, &id)
            .await
            .map_err(|_| "legacy checkpoint could not be decoded")?
            .ok_or("legacy checkpoint absent")?;
        let mut counts = if unconverted {
            Vec::new()
        } else {
            native_import_counts(&raw, &store, &id, &ordinary, &protected.snapshot, false).await?
        };
        let mut resumed = false;
        let provider = Arc::new(FakeProvider::new(
            "fake",
            Capabilities::basic_streaming(),
            Vec::new(),
        ));
        if selected.as_deref() == Some(&profile) {
            let lcm = store
                .lcm_store_for_runtime_session(&runtime, "agent_chat", &chat)
                .await
                .map_err(|_| "converted LCM binding unavailable")?;
            let expected_timeline = protected
                .snapshot
                .extension_state
                .get(LCM_COMPONENT_ID)
                .and_then(|s| s.value.get("timeline_id"))
                .and_then(serde_json::Value::as_str)
                .ok_or("selected target has no LCM reference")?;
            assert!(
                lcm.timeline_id() == expected_timeline,
                "selected target lost the shared timeline"
            );
            let binding = lcm
                .runtime_binding(id.clone())
                .map_err(|_| "runtime LCM binding unavailable")?;
            let coordinator = LcmCoordinator::new(
                Arc::new(lcm),
                Arc::new(forge_agent_host::DeterministicLcmSummaryModel::default()),
                Arc::new(StaticLcmTimelineResolver::new(binding)),
                LcmCoordinatorPolicy {
                    input_budget_tokens: 28_000,
                    ..Default::default()
                },
            )
            .map_err(|_| "LCM coordinator unavailable")?;
            let rt = RuntimeBuilder::new(ModelId::new("fake"))
                .provider_name("fake")
                .provider(provider.clone())
                .model_profile(ResolvedModelProfile::explicit(
                    "fake",
                    ModelId::new("fake"),
                    ModelLimits::new(32_000, 28_000, 4_000),
                ))
                .session_store(store.clone())
                .checkpoint_store(store.clone())
                .lcm(Arc::new(coordinator))
                .build()
                .map_err(|_| "lazy runtime unavailable")?;
            let session = rt
                .start_session(
                    StartSession::resume(id.clone())
                        .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer)
                        .with_lcm_policy(LcmRecoveryPolicy::Adopt),
                )
                .await
                .map_err(|e| format!("lazy resume failed: {:?}", e.kind))?;
            assert!(
                session.history() == protected.snapshot.history.as_slice(),
                "lazy resume changed the selected target history"
            );
            resumed = true;
            drop(session);
            drop(rt);
        }
        if unconverted {
            counts = native_import_counts(&raw, &store, &id, &ordinary, &protected.snapshot, true)
                .await?;
        }
        assert!(
            provider.requests().is_empty(),
            "lazy resume called the provider"
        );
        facts.push(serde_json::json!({"runtime_session_id":runtime,"ordinary_entries":counts[0],"protected_entries":counts[1],
            "lcm_namespaces":usize::from(protected.snapshot.extension_state.contains_key(LCM_COMPONENT_ID)),"resumed":resumed,"provider_calls":provider.requests().len(),"foreground_first":unconverted}));
    }
    println!("{}", serde_json::to_string(&facts)?);
    pool.close().await;
    Ok(())
}

async fn metadata_rows(
    pool: &sqlx::SqlitePool,
    query: &str,
    session: &ParkedSession,
) -> TestResult<Vec<serde_json::Value>> {
    let rows: Vec<String> = sqlx::query_scalar(query)
        .bind(&session.forge_session_id)
        .bind(&session.runtime_session_id)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .iter()
        .map(|row| serde_json::from_str(row))
        .collect::<Result<_, _>>()?)
}

async fn parked_session_facts(
    pool: &sqlx::SqlitePool,
    session: &ParkedSession,
) -> TestResult<serde_json::Value> {
    // Explicit metadata allowlists: no ciphertext, nonces, transcripts, topic
    // labels/summaries, authority JSON, workspace paths, or rotation tokens.
    let legacy = metadata_rows(pool,
        "SELECT json_object(
            'forge_session_id',s.id,'runtime_session_id',s.runtime_session_id,
            'status',s.status,'version',s.version,'backend_kind',s.backend_kind,
            'identity_id',s.identity_id,'profile_id',s.profile_id,'context_scope_id',s.context_scope_id,
            'scope_type',c.scope_type,'scope_id',c.scope_id,'project_id',c.project_id,
            'task_id',c.task_id,'task_role',c.task_role,'chat_kind',chat.kind,
            'predecessor_session_id',s.predecessor_session_id,'replaced_by_session_id',s.replaced_by_session_id,
            'legacy_state_present',l.session_id IS NOT NULL,'state_revision',l.state_revision,
            'checkpoint_revision',l.checkpoint_revision,'key_revision',l.key_revision,
            'snapshot_present',l.snapshot_ciphertext IS NOT NULL,
            'checkpoint_present',l.checkpoint_ciphertext IS NOT NULL)
         FROM agent_session s
         LEFT JOIN protected_agent_session_state l ON l.session_id=s.id
         LEFT JOIN agent_context_scope c ON c.id=s.context_scope_id
         LEFT JOIN agent_chat chat ON c.scope_type='agent_chat' AND chat.id=c.scope_id
         WHERE s.id=?1 OR s.runtime_session_id=?2 ORDER BY s.id", session).await?;
    let native = metadata_rows(pool,
        "SELECT json_object(
            'forge_session_id',forge_session_id,'runtime_session_id',runtime_session_id,
            'storage_id',storage_id,'role',role,'published',published,'retired',retired,
            'has_checkpoint',has_checkpoint,'collection_dirty',collection_dirty,
            'generation_hex',hex(generation),'revision_hex',hex(revision),
            'writer_epoch_hex',hex(writer_epoch),'layout_generation',layout_generation)
         FROM agent_session_journal_session
         WHERE runtime_session_id=?2 OR forge_session_id=?1 ORDER BY storage_id,role,runtime_session_id",
        session).await?;
    let linked = metadata_rows(pool,
        "SELECT json_object(
            'forge_session_id',s.id,'runtime_session_id',s.runtime_session_id,
            'status',s.status,'version',s.version,'context_scope_id',s.context_scope_id,
            'predecessor_session_id',s.predecessor_session_id,'replaced_by_session_id',s.replaced_by_session_id)
         FROM agent_session s WHERE s.predecessor_session_id=?1 OR s.replaced_by_session_id=?1
            OR s.id IN (SELECT predecessor_session_id FROM agent_session WHERE id=?1 AND runtime_session_id=?2
                        UNION SELECT replaced_by_session_id FROM agent_session WHERE id=?1 AND runtime_session_id=?2)
         ORDER BY s.id", session).await?;
    let rotations = metadata_rows(pool,
        "SELECT json_object(
            'rotation_id',r.id,'chat_id',r.chat_id,'source_session_id',r.source_session_id,
            'successor_session_id',r.successor_session_id,'successor_runtime_id',r.successor_runtime_id,
            'rotation_pending',r.rotation_pending,'attempt_count',r.attempt_count,'last_error_kind',r.last_error_kind)
         FROM agent_chat_topic_rotation r
         WHERE r.source_session_id=?1 OR r.successor_session_id=?1 OR r.successor_runtime_id=?2
            OR r.successor_session_id IN (SELECT id FROM agent_session WHERE predecessor_session_id=?1)
            OR r.chat_id IN (SELECT c.scope_id FROM agent_session s
                JOIN agent_context_scope c ON c.id=s.context_scope_id
                WHERE s.id=?1 AND c.scope_type='agent_chat')
         ORDER BY r.id", session).await?;
    Ok(serde_json::json!({
        "legacy_rows":legacy,"native_journal":{
            "exists":native.iter().any(|s| s["runtime_session_id"] == session.runtime_session_id),
            "sessions":native},
        "linked_sessions":linked,"rotations":rotations
    }))
}

async fn run_copy(path: &Path, secret_path: &Path) -> TestResult<serde_json::Value> {
    let path = copy_file(path)?;
    let secret_path = copy_file(secret_path)?;
    let secret = std::fs::read(secret_path)?;
    if secret.is_empty() {
        return Err("the copied JWT secret is empty".into());
    }
    let key: [u8; 32] = Sha256::digest(&secret).into();
    let migration_started = Instant::now();
    let pool = db::create_sqlite_pool(&format!("sqlite://{}", path.display())).await?;
    db::run_migrations(&pool).await?;
    let migration_ms = migration_started.elapsed().as_millis();
    let before = physical_bytes(&pool).await?;
    let db = Arc::new(db::SqliteDb::new(pool.clone()));
    let store = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), key, 1));
    let conversion = SessionJournalConversion::new(db.clone(), store.clone());
    let initial = db.legacy_journal_conversion_remaining(&[]).await?;
    let conversion_started = Instant::now();
    let mut cursor = ConversionCursor::default();
    let (mut ticks, mut converted, mut skipped_retired, mut skipped_active, mut unreadable) =
        (0, 0, 0, 0, 0);
    let mut parked = BTreeMap::<String, usize>::new();
    let mut parked_sessions = Vec::new();
    let mut parked_sessions_omitted = 0;
    loop {
        // The production eight-candidate / two-second budget applies to every
        // tick. No runtime supervisor, executors, daemon or provider is started.
        let report = conversion.convert_once(&mut cursor).await?;
        ticks += 1;
        converted += report.converted;
        skipped_retired += report.skipped_retired;
        skipped_active += report.skipped_active;
        unreadable += report.skipped_unreadable;
        for (reason, count) in report.parked_by_reason {
            *parked.entry(reason).or_default() += count;
        }
        for session in report.parked_sessions {
            if parked_sessions.len() < PARKED_SESSION_REPORT_LIMIT {
                let facts = parked_session_facts(&pool, &session).await?;
                let mut diagnostic = serde_json::to_value(session)?;
                diagnostic["facts"] = facts;
                parked_sessions.push(diagnostic);
            } else {
                parked_sessions_omitted += 1;
            }
        }
        if report.remaining == 0 {
            break;
        }
        if ticks > initial.saturating_mul(2).saturating_add(2) {
            return Err(
                "journal dry run did not converge; active sessions or retryable errors remain"
                    .into(),
            );
        }
        tokio::task::yield_now().await;
    }
    let conversion_ms = conversion_started.elapsed().as_millis();
    let retention_started = Instant::now();
    let retention = SessionJournalRetention::new(db.clone(), &store);
    let collected = retention
        .maintain_once(&mut RetentionCursor::default())
        .await?;
    let retention_ms = retention_started.elapsed().as_millis();
    let after = physical_bytes(&pool).await?;
    let summary = serde_json::json!({
        "converted":converted, "skipped":{"retirement_policy":skipped_retired,"active":skipped_active},
        "parked_by_reason":parked, "unreadable":{"conversion":unreadable,"retention":collected.unreadable_sessions},
        "parked_sessions":parked_sessions,"parked_sessions_omitted":parked_sessions_omitted,
        "remaining":0, "conversion_ticks":ticks, "bytes":{"before":before,"after":after},
        "elapsed_ms":{"migrations":migration_ms,"conversion":conversion_ms,"retention":retention_ms},
        "retention":{"retired_domains":collected.retired_domains,"retirement_errors":collected.retirement_errors,
            "collection_calls":collected.collection_calls,"deleted_packs":collected.deleted_packs,
            "budget_exhausted":collected.budget_exhausted}
    });
    drop(retention);
    drop(conversion);
    drop(store);
    drop(db);
    pool.close().await;
    Ok(summary)
}

#[tokio::test]
#[ignore = "mutates only an explicitly selected disposable database copy"]
async fn journal_real_data_dry_run() -> TestResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "forge_session_journal=debug",
        ))
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init()?;
    let Some(path) = std::env::var_os("FORGE_JOURNAL_DRYRUN_DB") else {
        eprintln!("FORGE_JOURNAL_DRYRUN_DB unset; journal dry run skipped");
        return Ok(());
    };
    let path = PathBuf::from(path);
    refuse_install_path(&path)?;
    let secret = std::env::var_os("FORGE_JOURNAL_DRYRUN_SECRET_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| path.with_file_name("jwt_secret.bin"));
    println!("{}", run_copy(&path, &secret).await?);
    Ok(())
}

#[test]
fn dry_run_refuses_installation_paths_before_opening() {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    assert!(copy_file(&home.join(".forge/forge.db")).is_err());
    assert!(copy_file(Path::new("copies/.forge/forge.db")).is_err());
}

#[tokio::test]
async fn dry_run_entrypoint_on_generated_database() {
    use agent_runtime::core::ids::SessionId;
    use forge_agent_host::test_support::FileDb;
    let fixture = FileDb::new().await.unwrap();
    let db = fixture.database();
    let secret = b"journal-dryrun-fixture-secret-material";
    let key: [u8; 32] = Sha256::digest(secret).into();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), key, 1));
    for n in 0..10 {
        super::session_journal_conversion::seed(&fixture, &store, &format!("dryrun-{n:02}")).await;
    }
    sqlx::query("UPDATE agent_session SET status='replaced' WHERE runtime_session_id='dryrun-00'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE protected_agent_session_state SET checkpoint_ciphertext=X'000102' WHERE session_id='journal-fixture:dryrun-01'").execute(db.pool()).await.unwrap();
    // A mismatched host/runtime id is a genuine validation conflict. Keep
    // predecessor and pending rotation links available for the diagnostic.
    sqlx::query("UPDATE agent_session SET runtime_session_id='dryrun-conflict',version=7,predecessor_session_id='journal-fixture:dryrun-00' WHERE id='journal-fixture:dryrun-02'")
        .execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE agent_session SET replaced_by_session_id='journal-fixture:dryrun-02' WHERE id='journal-fixture:dryrun-00'")
        .execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_chat_topic_rotation (id,chat_id,source_session_id,successor_session_id,successor_runtime_id,label,cause,created_at) VALUES ('dryrun-rotation','chat:dryrun-02','journal-fixture:dryrun-02','dryrun-successor','dryrun-successor-runtime','private topic label','idle','c')")
        .execute(db.pool()).await.unwrap();
    let empty = SessionId::new("empty-legacy");
    fixture.ensure_session(&empty).await.unwrap();
    sqlx::query("INSERT INTO protected_agent_session_state (session_id,key_revision,state_revision,updated_at) VALUES ('journal-fixture:empty-legacy',1,1,'c')")
        .execute(db.pool()).await.unwrap();
    std::fs::write(fixture.directory().join("jwt_secret.bin"), secret).unwrap();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(db.pool())
        .await
        .unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "session_journal_dryrun::journal_real_data_dry_run",
            "--nocapture",
        ])
        .env(
            "FORGE_JOURNAL_DRYRUN_DB",
            fixture.directory().join("journal.db"),
        )
        .env_remove("FORGE_JOURNAL_DRYRUN_SECRET_FILE")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("background journal conversion parked until restart"),
        "{stderr}"
    );
    assert!(stderr.contains("forge_session_journal"), "{stderr}");
    eprint!("{stderr}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let summary = stdout
        .lines()
        .find_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .expect(&stdout);
    assert_eq!(summary["converted"], 7);
    assert_eq!(summary["skipped"]["retirement_policy"], 1);
    assert_eq!(summary["parked_by_reason"]["Serialization"], 1);
    assert_eq!(summary["parked_by_reason"]["Conflict"], 1);
    assert_eq!(summary["parked_sessions_omitted"], 0);
    let parked = summary["parked_sessions"].as_array().unwrap();
    assert_eq!(parked.len(), 2);
    let conflict = parked
        .iter()
        .find(|s| s["error_kind"] == "Conflict")
        .unwrap();
    assert_eq!(conflict["runtime_session_id"], "dryrun-conflict");
    assert_eq!(conflict["owner_kind"], "agent_chat");
    assert_eq!(
        conflict["error_message"],
        "session store returned another session's state"
    );
    let facts = &conflict["facts"];
    assert_eq!(facts["legacy_rows"][0]["status"], "suspended");
    assert_eq!(facts["legacy_rows"][0]["version"], 7);
    assert_eq!(
        facts["legacy_rows"][0]["context_scope_id"],
        "scope:dryrun-02"
    );
    assert_eq!(
        facts["legacy_rows"][0]["predecessor_session_id"],
        "journal-fixture:dryrun-00"
    );
    assert_eq!(
        facts["linked_sessions"][0]["replaced_by_session_id"],
        "journal-fixture:dryrun-02"
    );
    assert_eq!(facts["rotations"][0]["rotation_id"], "dryrun-rotation");
    assert_eq!(facts["native_journal"]["exists"], true);
    let native = facts["native_journal"]["sessions"].as_array().unwrap();
    assert_eq!(native.len(), 2);
    assert!(native
        .iter()
        .all(|s| s["published"] == 0 && s["retired"] == 0));
    assert!(!stdout.contains("accepted legacy input"));
    assert!(!stdout.contains("private topic label"));
    assert!(!stdout.contains(std::str::from_utf8(secret).unwrap()));
    assert_eq!(summary["unreadable"]["conversion"], 1);
    assert_eq!(summary["remaining"], 0);
    assert!(summary["conversion_ticks"].as_u64().unwrap() >= 2);
    assert!(
        summary["bytes"]["before"]["legacy_dbstat"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        summary["bytes"]["after"]["journal_dbstat"]
            .as_u64()
            .unwrap()
            > 0
    );
    println!("{summary}");
}

#[tokio::test]
async fn dry_run_converts_pre_topic_main_chat_lcm() {
    use forge_agent_host::test_support::{FileDb, LegacyLcmFixture};
    let f = FileDb::new().await.unwrap();
    let secret = b"synthetic-historical-lcm-secret";
    let key: [u8; 32] = Sha256::digest(secret).into();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(f.database(), key, 1));
    let id = super::session_journal_conversion::seed(&f, &store, "dryrun-pre-topic-main").await;
    let old = LegacyLcmFixture::seed(&f, &store, &id, false).await;
    old.bind_empty_replacement(&f).await;
    let before: (String,String) = sqlx::query_as("SELECT quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state")
        .fetch_one(f.database().pool()).await.unwrap();
    let secret_path = f.directory().join("jwt_secret.bin");
    std::fs::write(&secret_path, secret).unwrap();
    let summary = run_copy(&f.directory().join("journal.db"), &secret_path)
        .await
        .unwrap();
    assert_eq!(summary["converted"], 1);
    assert_eq!(summary["remaining"], 0);
    assert_eq!(summary["parked_by_reason"], serde_json::json!({}));
    assert_eq!(summary["parked_sessions"], serde_json::json!([]));
    let after: (String,String) = sqlx::query_as("SELECT quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state")
        .fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(after, before);
    let transcript: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_message")
        .fetch_one(f.database().pool())
        .await
        .unwrap();
    assert_eq!(transcript, 120);
}

#[tokio::test]
async fn dry_run_entrypoint_converts_shared_pre_topic_main_chat() {
    use agent_runtime::core::ids::SessionId;
    use forge_agent_host::test_support::{FileDb, LegacyLcmFixture};
    let f = FileDb::new().await.unwrap();
    let db = f.database();
    let secret = b"synthetic-shared-pre-topic-chat-secret";
    let store = SqliteProtectedRuntimeStore::new(db.clone(), Sha256::digest(secret).into(), 1);
    let id = SessionId::new("dryrun-shared-pre-topic-main");
    let (old, older) = LegacyLcmFixture::shared_pre_topic_chat(&f, &store, &id).await;
    let before: Vec<(String,String,String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
        .fetch_all(db.pool()).await.unwrap();
    std::fs::write(f.directory().join("jwt_secret.bin"), secret).unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "session_journal_dryrun::journal_real_data_dry_run",
            "--nocapture",
        ])
        .env("FORGE_JOURNAL_DRYRUN_DB", f.directory().join("journal.db"))
        .env(
            "FORGE_JOURNAL_DRYRUN_SECRET_FILE",
            f.directory().join("jwt_secret.bin"),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let summary = stdout
        .lines()
        .find_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .expect(&stdout);
    assert_eq!(summary["converted"], 1);
    assert_eq!(summary["skipped"]["retirement_policy"], 1);
    assert_eq!(summary["skipped"]["active"], 0);
    assert_eq!(summary["parked_by_reason"], serde_json::json!({}));
    assert_eq!(summary["parked_sessions"], serde_json::json!([]));
    assert_eq!(summary["remaining"], 0);
    let after: Vec<(String,String,String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
        .fetch_all(db.pool()).await.unwrap();
    assert_eq!(after, before);
    let rows: (String, i64, i64, i64, i64) = sqlx::query_as("SELECT runtime_session_id,revision,(SELECT COUNT(*) FROM agent_lcm_entry),(SELECT COUNT(*) FROM agent_lcm_operation),(SELECT COUNT(*) FROM agent_lcm_timeline) FROM agent_lcm_timeline WHERE id=?")
        .bind(&old.timeline_id).fetch_one(db.pool()).await.unwrap();
    assert_eq!(rows, (id.to_string(), 131, 114, 121, 1));
    let superseded: (String, Option<String>) = sqlx::query_as(
        "SELECT status,replaced_by_session_id FROM agent_session WHERE runtime_session_id=?",
    )
    .bind(older.as_str())
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        superseded,
        ("replaced".into(), Some(format!("journal-fixture:{id}")))
    );
    assert!(!stdout.contains(std::str::from_utf8(secret).unwrap()));
    assert!(!stdout.contains("Synthetic historical message"));
    println!("{summary}");
}

#[tokio::test]
async fn dry_run_entrypoint_converts_selected_profile_shared_main_chat() {
    use agent_runtime::core::ids::SessionId;
    use forge_agent_host::test_support::{FileDb, LegacyLcmFixture};
    let f = FileDb::new().await.unwrap();
    let db = f.database();
    let secret = b"synthetic-selected-profile-chat-secret";
    let store = SqliteProtectedRuntimeStore::new(db.clone(), Sha256::digest(secret).into(), 1);
    let id = SessionId::new("dryrun-selected-profile-main");
    let (old, older) = LegacyLcmFixture::unowned_pre_topic_profile_switch(&f, &store, &id).await;
    let before:Vec<(String,String,String)>=sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
        .fetch_all(db.pool()).await.unwrap();
    std::fs::write(f.directory().join("jwt_secret.bin"), secret).unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "session_journal_dryrun::journal_real_data_dry_run",
            "--nocapture",
        ])
        .env("FORGE_JOURNAL_DRYRUN_DB", f.directory().join("journal.db"))
        .env(
            "FORGE_JOURNAL_DRYRUN_SECRET_FILE",
            f.directory().join("jwt_secret.bin"),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let summary = stdout
        .lines()
        .find_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .expect(&stdout);
    assert_eq!(summary["converted"], 2);
    assert_eq!(summary["skipped"]["retirement_policy"], 9);
    assert_eq!(summary["skipped"]["active"], 0);
    assert_eq!(summary["parked_by_reason"], serde_json::json!({}));
    assert_eq!(summary["parked_sessions"], serde_json::json!([]));
    assert_eq!(summary["remaining"], 0);
    let after:Vec<(String,String,String)>=sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
        .fetch_all(db.pool()).await.unwrap();
    assert_eq!(before, after);
    let rows:(String,i64,i64,i64,i64)=sqlx::query_as("SELECT runtime_session_id,revision,(SELECT COUNT(*) FROM agent_lcm_entry),(SELECT COUNT(*) FROM agent_lcm_operation),(SELECT COUNT(*) FROM agent_lcm_timeline) FROM agent_lcm_timeline WHERE id=?")
        .bind(&old.timeline_id).fetch_one(db.pool()).await.unwrap();
    assert_eq!(rows, (id.to_string(), 131, 114, 121, 1));
    let a:(String,Option<String>,i64)=sqlx::query_as("SELECT status,replaced_by_session_id,version FROM agent_session WHERE runtime_session_id=?")
        .bind(older.as_str()).fetch_one(db.pool()).await.unwrap();
    assert_eq!(a, ("suspended".into(), None, 1));
    let old_native:i64=sqlx::query_scalar("SELECT COUNT(*) FROM agent_session_journal_session j JOIN agent_session s ON s.id=j.forge_session_id WHERE s.profile_id='glm-profile' AND (j.published=1 OR j.retired=1)")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(old_native, 2);
    let relinquishments: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(relinquishments, 10);
    assert!(!stdout.contains(std::str::from_utf8(secret).unwrap()));
    assert!(!stdout.contains("Synthetic historical message"));
    println!("selected-profile-generated-dryrun {summary}");
}

#[tokio::test]
async fn dry_run_facts_use_read_only_queries_without_native_state() {
    use forge_agent_host::test_support::FileDb;
    let fixture = FileDb::new().await.unwrap();
    let store = SqliteProtectedRuntimeStore::new(fixture.database(), [7; 32], 1);
    super::session_journal_conversion::seed(&fixture, &store, "metadata-only").await;
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(fixture.directory().join("journal.db"))
                .read_only(true),
        )
        .await
        .unwrap();
    let facts = parked_session_facts(
        &pool,
        &ParkedSession {
            forge_session_id: "journal-fixture:metadata-only".into(),
            runtime_session_id: "metadata-only".into(),
            owner_kind: "agent_chat".into(),
            error_kind: "Conflict".into(),
            error_message: "fixture diagnostic".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(facts["native_journal"]["exists"], false);
    assert_eq!(facts["native_journal"]["sessions"], serde_json::json!([]));
    assert_eq!(facts["legacy_rows"][0]["scope_type"], "agent_chat");
    assert_eq!(facts["legacy_rows"][0]["state_revision"], 1);
    assert_eq!(facts["rotations"], serde_json::json!([]));
    assert_eq!(facts["linked_sessions"], serde_json::json!([]));
    pool.close().await;
}

#[tokio::test]
async fn dry_run_caps_parked_details_across_conversion_ticks() {
    use forge_agent_host::test_support::FileDb;
    let fixture = FileDb::new().await.unwrap();
    let db = fixture.database();
    let secret = b"capped-diagnostics-fixture-secret";
    let store = SqliteProtectedRuntimeStore::new(db.clone(), Sha256::digest(secret).into(), 1);
    let count = PARKED_SESSION_REPORT_LIMIT + 3;
    for n in 0..count {
        super::session_journal_conversion::seed(&fixture, &store, &format!("capped-{n:02}")).await;
    }
    sqlx::query("UPDATE protected_agent_session_state SET checkpoint_ciphertext=X'000102'")
        .execute(db.pool())
        .await
        .unwrap();
    let secret_path = fixture.directory().join("jwt_secret.bin");
    std::fs::write(&secret_path, secret).unwrap();
    let summary = run_copy(&fixture.directory().join("journal.db"), &secret_path)
        .await
        .unwrap();
    assert_eq!(summary["parked_by_reason"]["Serialization"], count);
    assert_eq!(
        summary["parked_sessions"].as_array().unwrap().len(),
        PARKED_SESSION_REPORT_LIMIT
    );
    assert_eq!(summary["parked_sessions_omitted"], 3);
    assert_eq!(summary["remaining"], 0);
}
