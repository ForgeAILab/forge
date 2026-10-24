use agent_runtime::core::{
    clock::Timestamp,
    error::{ErrorKind, FailureClass, FailureComponent, FailureStage},
    ids::SessionId,
    journal::*,
};
use forge_agent_host::{
    session_journal::{ForgeJournalStorage, TestSealedUnit},
    test_support::{FileDb, MappingStorage},
};
use sqlx::Row;
use std::sync::Arc;

#[path = "session_journal_storage/atomicity.rs"]
mod atomicity;
#[path = "session_journal_storage/collection.rs"]
mod collection;
#[path = "session_journal_storage/corruption.rs"]
mod corruption;
#[path = "session_journal_storage/operations.rs"]
mod operations;
#[path = "session_journal_storage/performance.rs"]
mod performance;
#[path = "session_journal_storage/pins.rs"]
mod pins;
#[path = "session_journal_storage/reads.rs"]
mod reads;
#[path = "session_journal_storage/repacking.rs"]
mod repacking;
#[path = "session_journal_storage/retention.rs"]
mod retention;
#[path = "session_journal_storage/sharing.rs"]
mod sharing;

fn domain(role: JournalRole) -> JournalDomain {
    JournalDomain {
        storage_id: JournalStorageId::new("forge.storage-tests"),
        role,
    }
}
fn object(value: impl serde::Serialize) -> JournalStoredObject {
    let o = JournalObject::encode(JournalObjectKind::JsonInline, &value).unwrap();
    JournalStoredObject {
        id: o.id,
        kind: JournalObjectKind::JsonInline,
        bytes: o.bytes.into(),
    }
}
fn engine() -> JournalEngineState {
    JournalEngineState {
        version: u32::MAX,
        bytes: vec![0, 255, 128, 0, 17],
    }
}
fn head(d: &JournalDomain, s: &SessionId, id: JournalObjectId) -> JournalHead {
    JournalHead {
        schema_version: JOURNAL_SCHEMA_VERSION,
        storage_id: d.storage_id.clone(),
        role: d.role,
        session: s.clone(),
        revision: JournalRevision(7),
        writer_epoch: 11,
        batch_sequence: 3,
        batch: id,
        snapshot: JournalSnapshotRoots {
            history: Default::default(),
            history_chain: journal_history_chain(std::iter::empty()),
            usage: Default::default(),
            extensions: Default::default(),
            manifests: Default::default(),
            planned_step_count: 23,
            identity: Default::default(),
            updated: Timestamp(123),
        },
        checkpoint: None,
        superseded_by: None,
        retirement: None,
        created: Timestamp(12),
        updated: Timestamp(124),
    }
}
fn write(d: &JournalDomain, s: &str, mut objects: Vec<JournalStoredObject>) -> JournalWrite {
    objects.sort_by(|a, b| a.id.cmp(&b.id));
    let session = SessionId::new(s);
    let h = head(
        d,
        &session,
        objects
            .first()
            .map(|o| o.id.clone())
            .unwrap_or_else(|| JournalObjectId::from_bytes([9; 32])),
    );
    let ho = JournalObject::encode(JournalObjectKind::Head, &h).unwrap();
    JournalWrite {
        domain: d.clone(),
        session,
        kind: JournalWriteKind::Commit,
        expect: Default::default(),
        record: JournalSessionRecord {
            generation: 1,
            revision: h.revision,
            writer_epoch: h.writer_epoch,
            head: Some(ho.clone()),
            has_checkpoint: false,
            retirement: None,
            collection_dirty: true,
            engine: engine(),
        },
        objects: JournalObjectBatch { objects },
        operation: Some(JournalOperationRecord {
            operation: JournalOperationId::new("operation"),
            digest: journal_history_chain(std::iter::empty()),
            revision: h.revision,
            result: h,
        }),
        pins: vec![JournalPinChange::Put(JournalPinRecord {
            pin: JournalPinId::new("pin"),
            blocks_retirement: true,
            roots: vec![ho.id.clone()],
            engine: engine(),
        })],
        catalog: vec![JournalCatalogChange::Put(JournalCatalogEntry {
            head: ho.id,
            engine: engine(),
        })],
        release: vec![],
        prune_operations_before: None,
        inject: None,
    }
}
async fn storage(f: &FileDb) -> Arc<ForgeJournalStorage> {
    Arc::new(ForgeJournalStorage::new(f.database(), [7; 32], 1).unwrap())
}
async fn reopen(f: &FileDb) -> Arc<ForgeJournalStorage> {
    Arc::new(ForgeJournalStorage::new(f.reopen().await.unwrap(), [7; 32], 1).unwrap())
}

fn state_conflict(error: agent_runtime::core::error::RuntimeError) {
    assert_eq!(error.kind, ErrorKind::Conflict);
    assert_eq!(
        error.class,
        FailureClass::StateConflict {
            stage: FailureStage::PreProvider,
            component: FailureComponent::Checkpoint
        }
    );
}
fn control(d: &JournalDomain, s: &SessionId, record: JournalSessionRecord) -> JournalWrite {
    let expect = record.fence();
    let mut record = record;
    record.generation = expect.generation.checked_add(1).unwrap();
    JournalWrite {
        domain: d.clone(),
        session: s.clone(),
        kind: JournalWriteKind::Pin,
        expect,
        record,
        objects: Default::default(),
        operation: None,
        pins: vec![],
        catalog: vec![],
        release: vec![],
        prune_operations_before: None,
        inject: None,
    }
}
async fn image(f: &FileDb) -> Vec<Vec<String>> {
    let db = f.database();
    let mut result = Vec::new();
    for table in [
        "session",
        "pack",
        "membership",
        "pin",
        "catalog",
        "operation_pack",
    ] {
        let table = format!("agent_session_journal_{table}");
        let columns = sqlx::query(&format!("PRAGMA table_info({table})"))
            .fetch_all(db.pool())
            .await
            .unwrap();
        let quoted = columns
            .into_iter()
            .map(|r| format!("quote({})", r.get::<String, _>("name")))
            .collect::<Vec<_>>()
            .join(",");
        result.push(
            sqlx::query_scalar(&format!(
                "SELECT json_array({quoted}) FROM {table} ORDER BY 1"
            ))
            .fetch_all(db.pool())
            .await
            .unwrap(),
        );
    }
    result.push(
        sqlx::query_scalar(
            "SELECT quote(seq) FROM sqlite_sequence WHERE name='agent_session_journal_pack'",
        )
        .fetch_all(db.pool())
        .await
        .unwrap(),
    );
    result
}

fn engine_config() -> JournalEngineConfig {
    JournalEngineConfig {
        collection_enabled: true,
        validated_root_memo_bytes: 128 * 1024 * 1024,
    }
}
fn open_request(label: &str, root: JournalRootSelection, intent: JournalOpenIntent) -> JournalOpen {
    JournalOpen {
        root,
        expected_revision: None,
        operation: JournalOperationId::new(format!("{label}-open")),
        pin: JournalPinId::new(label),
        intent,
    }
}
// Public DTOs only: an empty snapshot has empty sequence/map roots. The engine
// encodes all durable control objects and computes its own dependency metadata.
fn empty_commit(
    s: &SessionId,
    epoch: u64,
    operation: &str,
    previous: Option<&JournalHead>,
    at: u64,
) -> JournalCommit {
    use agent_runtime::core::{store::SessionSnapshot, usage::UsageLedger};
    let roots = head(
        &domain(JournalRole::Protected),
        s,
        JournalObjectId::from_bytes([0; 32]),
    )
    .snapshot;
    let mut roots = roots;
    roots.planned_step_count = 0;
    roots.updated = Timestamp(at);
    let snapshot = SessionSnapshot {
        id: s.clone(),
        history: vec![],
        usage: UsageLedger::new(),
        identity: Default::default(),
        manifests: vec![],
        extension_state: Default::default(),
        updated: Timestamp(at),
    };
    let boundary = JournalBoundary {
        version: 1,
        session: s.clone(),
        turn: None,
        checkpoint_sequence: 0,
        planned_step_count: 0,
    };
    let batch = JournalBatch {
        session: s.clone(),
        sequence: previous.map_or(1, |h| h.batch_sequence + 1),
        predecessor: previous.map(|h| JournalDigest::parse(h.batch.as_str()).unwrap()),
        expected_revision: previous.map(|h| h.revision),
        writer_epoch: epoch,
        operation: JournalOperationId::new(operation),
        boundary: boundary.clone(),
        snapshot: roots,
        checkpoint: None,
    };
    let mut commit = JournalCommit {
        batch,
        digest: journal_history_chain(std::iter::empty()),
        objects: vec![],
        delta: JournalTransitionDelta {
            boundary,
            bootstrap: Some(Box::new(snapshot)),
            history_append: vec![],
            usage_append: vec![],
            extension_updates: Default::default(),
            extension_references: Default::default(),
            manifests_append: vec![],
            manifest_references: vec![],
        },
        checkpoint: None,
    };
    commit.digest = commit.input_digest().unwrap();
    commit
}

async fn operation_rows(
    f: &FileDb,
    d: &JournalDomain,
    s: &SessionId,
) -> std::collections::BTreeMap<i64, (Vec<u8>, Vec<u8>)> {
    sqlx::query("SELECT tier,nonce,payload FROM agent_session_journal_operation_pack WHERE storage_id=? AND role=? AND runtime_session_id=? ORDER BY tier")
        .bind(d.storage_id.as_str()).bind(if d.role == JournalRole::Protected { "protected" } else { "ordinary" }).bind(s.as_str()).fetch_all(f.database().pool()).await.unwrap().into_iter().map(|r| (r.get("tier"), (r.get("nonce"), r.get("payload")))).collect()
}

type PackRows = Vec<(i64, Vec<u8>, i64, i64, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)>;
async fn pack_rows(f: &FileDb, d: &JournalDomain, s: &SessionId) -> PackRows {
    // Packs are session-owned; a domain's packs are the ones it holds bits in
    // (plus fully unheld packs of the session, which no domain holds).
    sqlx::query("SELECT pack_id,generation,object_count,retained_canonical_bytes,index_nonce,index_payload,body_nonce,body_payload FROM agent_session_journal_pack p WHERE runtime_session_id=?3 AND (EXISTS(SELECT 1 FROM agent_session_journal_membership m WHERE m.pack_id=p.pack_id AND m.storage_id=?1 AND m.role=?2) OR NOT EXISTS(SELECT 1 FROM agent_session_journal_membership m WHERE m.pack_id=p.pack_id)) ORDER BY pack_id")
        .bind(d.storage_id.as_str()).bind(if d.role == JournalRole::Protected { "protected" } else { "ordinary" }).bind(s.as_str()).fetch_all(f.database().pool()).await.unwrap().into_iter().map(|r| (r.get(0),r.get(1),r.get(2),r.get(3),r.get(4),r.get(5),r.get(6),r.get(7))).collect()
}
async fn edit(raw: &ForgeJournalStorage, d: &JournalDomain, s: &SessionId) -> JournalWrite {
    control(d, s, raw.load(d, s).await.unwrap().unwrap())
}
fn equal_objects(prefix: &str, count: usize) -> Vec<JournalStoredObject> {
    (0..count)
        .map(|i| object(format!("{prefix}-{i:03}-{}", "x".repeat(100))))
        .collect()
}

async fn add_objects(
    raw: &ForgeJournalStorage,
    d: &JournalDomain,
    s: &SessionId,
    mut objects: Vec<JournalStoredObject>,
) {
    objects.sort_by(|a, b| a.id.cmp(&b.id));
    let mut add = edit(raw, d, s).await;
    add.objects.objects = objects;
    raw.apply(add).await.unwrap();
}

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn model_operation(
    d: &JournalDomain,
    s: &SessionId,
    id: &str,
    revision: u64,
    epoch: u64,
    variant: u64,
) -> JournalOperationRecord {
    let mut h = head(d, s, JournalObjectId::from_bytes([variant as u8; 32]));
    h.revision = JournalRevision(revision);
    h.writer_epoch = epoch;
    h.batch_sequence = variant;
    JournalOperationRecord {
        operation: JournalOperationId::new(id),
        digest: journal_history_chain(std::iter::empty()),
        revision: JournalRevision(revision),
        result: h,
    }
}

async fn session_object_count(f: &FileDb, s: &SessionId) -> i64 {
    sqlx::query_scalar(
        "SELECT coalesce(sum(object_count),0) FROM agent_session_journal_pack WHERE runtime_session_id=?",
    )
    .bind(s.as_str())
    .fetch_one(f.database().pool())
    .await
    .unwrap()
}
fn ids(objects: &[JournalStoredObject]) -> Vec<JournalObjectId> {
    objects.iter().map(|o| o.id.clone()).collect()
}
fn sorted_ids(objects: &[JournalStoredObject]) -> Vec<JournalObjectId> {
    let mut ids = ids(objects);
    ids.sort();
    ids
}

async fn session_pack_ids(f: &FileDb, s: &SessionId) -> Vec<i64> {
    sqlx::query_scalar(
        "SELECT pack_id FROM agent_session_journal_pack WHERE runtime_session_id=? ORDER BY pack_id",
    )
    .bind(s.as_str())
    .fetch_all(f.database().pool())
    .await
    .unwrap()
}

async fn drain_gc<S: JournalStorage>(
    journal: &JournalEngine<S>,
    mut request: JournalGcRequest,
) -> Result<JournalGcReport, agent_runtime::core::error::RuntimeError> {
    let mut total = journal.collect(request.clone()).await?;
    for _ in 0..10_000 {
        if !total.has_more {
            return Ok(total);
        }
        request.expected_root_epoch = journal.root_epoch()?;
        total.accumulate(journal.collect(request.clone()).await?);
    }
    panic!("collection continuation bound exhausted");
}
