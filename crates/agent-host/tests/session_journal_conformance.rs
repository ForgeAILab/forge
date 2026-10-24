#[path = "support/broken.rs"]
mod broken;
#[path = "support/reopen.rs"]
mod reopen;
#[path = "support/session_journal.rs"]
mod support;

use std::sync::Mutex;

use agent_runtime::core::{
    checkpoint::CheckpointStore, ids::SessionId, journal::*, store::SessionStore,
};
use agent_runtime_testkit::conformance::journal::*;
use agent_runtime_testkit::conformance::journal_storage::assert_journal_storage_conformance;
use forge_agent_host::{SqliteProtectedRuntimeStore, test_support::FileDb};

#[tokio::test]
async fn storage_conformance_no_repack() {
    assert_journal_storage_conformance(|| support::storage_with_repacking(false)).await;
}

#[tokio::test]
async fn storage_conformance_repack() {
    assert_journal_storage_conformance(support::storage).await;
}

#[tokio::test]
async fn storage_conformance_reopen() {
    assert_journal_storage_conformance(|| async {
        std::sync::Arc::new(reopen::Reopen::new().await)
    })
    .await;
}

#[tokio::test]
async fn broken_storage_torn_transaction() {
    broken::rejects(broken::Defect::TornTransaction, "atomic_apply_cuts").await;
}

#[tokio::test]
async fn broken_storage_non_retryable_lost_ack() {
    broken::rejects(broken::Defect::NonRetryableLostAck, "atomic_apply_cuts").await;
}

#[tokio::test]
async fn broken_storage_lost_write() {
    broken::rejects(broken::Defect::LostWrite, "restart_acknowledged_state").await;
}

#[tokio::test]
async fn broken_storage_reordered_commit() {
    let message = broken::rejects(broken::Defect::ReorderedCommit, "restart_commit_sequence").await;
    assert!(message.contains("acknowledged predecessor batch must survive"));
}

#[tokio::test]
async fn broken_storage_ignored_epoch() {
    broken::rejects(broken::Defect::StaleEpoch, "writer_epoch_fence").await;
}

#[tokio::test]
async fn broken_storage_remaining_holder_unlinked() {
    let message = broken::rejects(
        broken::Defect::SharedBodyDeleted,
        "shared_body_last_membership",
    )
    .await;
    assert!(message.contains("remaining holder must open before returning a lease"));
}

#[tokio::test]
async fn broken_storage_non_member_served() {
    broken::rejects(
        broken::Defect::DeadBodyServed,
        // v0.9.0's new warm-cache authority probe rejects this defect first.
        "body_cache_membership",
    )
    .await;
}

#[tokio::test]
async fn broken_storage_ignored_control_generation() {
    broken::rejects(broken::Defect::StaleGeneration, "control_generation_fence").await;
}

#[tokio::test]
async fn engine_harness_smoke() {
    let storage = support::storage().await;
    let fixture = storage.fixture.clone();
    let path = fixture.directory().to_path_buf();
    let domain = JournalDomain {
        storage_id: JournalStorageId::new("forge.harness"),
        role: JournalRole::Protected,
    };
    let unseen = SessionId::new("generated-at-runtime-not-preseeded");
    assert_eq!(storage.load(&domain, &unseen).await.unwrap(), None);
    let mappings: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_session")
        .fetch_one(fixture.database().pool())
        .await
        .unwrap();
    assert_eq!(mappings, 0, "reads must not seed runtime ids");
    // Exercise first-apply creation and repeated applies across domains.
    for role in [JournalRole::Protected, JournalRole::Ordinary] {
        let mut domain = domain.clone();
        domain.role = role;
        for generation in 1..=2 {
            storage
                .apply(JournalWrite {
                    domain: domain.clone(),
                    session: unseen.clone(),
                    kind: JournalWriteKind::OpenWriter,
                    expect: JournalFence {
                        generation: generation - 1,
                        ..Default::default()
                    },
                    record: JournalSessionRecord {
                        generation,
                        revision: JournalRevision(0),
                        writer_epoch: 0,
                        head: None,
                        has_checkpoint: false,
                        retirement: None,
                        collection_dirty: false,
                        engine: JournalEngineState {
                            version: 999,
                            bytes: vec![0, 255],
                        },
                    },
                    objects: Default::default(),
                    operation: None,
                    pins: vec![],
                    catalog: vec![],
                    release: vec![],
                    prune_operations_before: None,
                    inject: None,
                })
                .await
                .unwrap();
        }
    }
    let reopened = fixture.reopen().await.unwrap();
    let mappings: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_session WHERE runtime_session_id = ?")
            .bind(unseen.as_str())
            .fetch_one(reopened.pool())
            .await
            .unwrap();
    assert_eq!(mappings, 1);
    drop(fixture);
    assert!(path.exists(), "storage retains the temporary directory");
    reopened.pool().close().await;
    storage.fixture.database().pool().close().await;
    drop(storage);
    assert!(
        !path.exists(),
        "the final fixture drop removes its directory"
    );

    assert_session_journal_conformance(|| async {
        support::backend(support::protected(false).await)
    })
    .await;
    assert_ordinary_session_journal_conformance(|policy| async {
        support::ordinary(policy, false).await as std::sync::Arc<dyn SessionJournal>
    })
    .await;
    assert_session_journal_collection_conformance(|| async {
        support::collection(support::protected(true).await)
    })
    .await;
    let ordinary = |policy| async { support::collection(support::ordinary(policy, true).await) };
    assert_ordinary_journal_collection_conformance(&ordinary).await;
    assert_ordinary_gc_shared_suffix_conformance(&ordinary).await;
    assert_journal_gc_projected_intent_conformance(&ordinary).await;
    assert_journal_unsupported_collection_conformance(&|| async {
        support::backend(support::protected(false).await)
    })
    .await;
}

#[tokio::test]
async fn forge_legacy_decoder_accepts_native_materialized_checkpoint() {
    let materialized = Mutex::new(Vec::new());
    assert_native_materialized_checkpoint_schema_guard(|bytes| {
        SqliteProtectedRuntimeStore::decode_legacy_checkpoint(bytes)
            .map_err(|error| error.to_string())?;
        *materialized.lock().unwrap() = bytes.to_vec();
        Ok(())
    })
    .await;
    let bytes = materialized.into_inner().unwrap();
    let checkpoint = SqliteProtectedRuntimeStore::decode_legacy_checkpoint(&bytes).unwrap();
    let snapshot = serde_json::to_vec(&checkpoint.snapshot).unwrap();
    assert_eq!(
        SqliteProtectedRuntimeStore::decode_legacy_snapshot(&snapshot).unwrap(),
        checkpoint.snapshot
    );
    let fixture = FileDb::new().await.unwrap();
    fixture.ensure_session(&checkpoint.session).await.unwrap();
    let store = SqliteProtectedRuntimeStore::new(fixture.database(), [7; 32], 1);
    for enveloped in [false, true] {
        store
            .seed_legacy_bytes(
                &checkpoint.session,
                Some(&snapshot),
                Some(&bytes),
                enveloped,
            )
            .await
            .unwrap();
        let reopened =
            SqliteProtectedRuntimeStore::new(fixture.reopen().await.unwrap(), [7; 32], 1);
        let decoded = CheckpointStore::load_latest(&reopened, &checkpoint.session)
            .await
            .unwrap()
            .unwrap();
        decoded.validate().expect("real schema and transition gate");
        assert_eq!(decoded, checkpoint);
        assert_eq!(
            SessionStore::load(&reopened, &checkpoint.session)
                .await
                .unwrap()
                .unwrap(),
            checkpoint.snapshot
        );
    }
    let mut future: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    future["schema_version"] = serde_json::json!(4);
    let future = serde_json::to_vec(&future).unwrap();
    assert!(SqliteProtectedRuntimeStore::decode_legacy_checkpoint(&future).is_err());
    store
        .seed_legacy_bytes(&checkpoint.session, None, Some(&future), true)
        .await
        .unwrap();
    let decoded = CheckpointStore::load_latest(&store, &checkpoint.session)
        .await
        .unwrap()
        .unwrap();
    assert!(
        decoded.validate().is_err(),
        "the real consumer schema gate stays closed"
    );
}

#[tokio::test]
async fn broken_storage_unbounded_collection_page() {
    broken::rejects(broken::Defect::UnboundedPage, "bounded_collection_protocol").await;
}
#[tokio::test]
async fn broken_storage_stale_collection_page() {
    broken::rejects(
        broken::Defect::StaleCollectionPage,
        "bounded_collection_protocol",
    )
    .await;
}
#[tokio::test]
async fn broken_storage_partial_collection_apply() {
    broken::rejects(
        broken::Defect::PartialCollectionApply,
        "bounded_collection_protocol",
    )
    .await;
}
#[tokio::test]
async fn broken_storage_underreported_collection_units() {
    broken::rejects(
        broken::Defect::UnderReportedUnits,
        "bounded_collection_protocol",
    )
    .await;
}
#[tokio::test]
async fn broken_storage_dropped_pin_page() {
    broken::rejects(
        broken::Defect::DroppedPinPage,
        "bounded_collection_protocol",
    )
    .await;
}
