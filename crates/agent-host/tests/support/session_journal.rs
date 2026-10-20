use std::sync::Arc;

use agent_runtime::core::journal::*;
use agent_runtime_testkit::conformance::journal::{
    SessionJournalCollectionFixture, SessionJournalConformanceFixture,
};
use forge_agent_host::{
    session_journal::ForgeJournalStorage,
    test_support::{FileDb, MappingStorage},
};

pub type Storage = MappingStorage<ForgeJournalStorage>;

pub async fn storage() -> Arc<Storage> {
    storage_with_repacking(true).await
}

pub async fn storage_with_repacking(repack: bool) -> Arc<Storage> {
    let fixture = FileDb::new().await.expect("file-backed fixture");
    let storage = ForgeJournalStorage::new(fixture.database(), [7; 32], 1).unwrap();
    storage.set_repacking(repack);
    Arc::new(MappingStorage::new(Arc::new(storage), fixture))
}

pub fn config(collection_enabled: bool) -> JournalEngineConfig {
    JournalEngineConfig {
        collection_enabled,
        validated_root_memo_bytes: 128 * 1024 * 1024,
    }
}

pub async fn protected(gc: bool) -> Arc<JournalEngine<Storage>> {
    Arc::new(JournalEngine::protected(
        storage().await,
        JournalStorageId::new("forge.harness"),
        config(gc),
    ))
}

pub async fn ordinary(
    policy: Arc<dyn JournalProjectionPolicy>,
    gc: bool,
) -> Arc<JournalEngine<Storage>> {
    Arc::new(JournalEngine::ordinary(
        storage().await,
        JournalStorageId::new("forge.harness"),
        policy,
        config(gc),
    ))
}

pub fn backend(engine: Arc<JournalEngine<Storage>>) -> SessionJournalConformanceFixture {
    let failure = engine.clone();
    let revoke = engine.clone();
    SessionJournalConformanceFixture::new(
        engine,
        move |point| failure.fail_next_commit_at(point),
        move |session| revoke.revoke_writer(session),
    )
}

pub fn collection(engine: Arc<JournalEngine<Storage>>) -> SessionJournalCollectionFixture {
    SessionJournalCollectionFixture {
        backend: backend(engine.clone()),
        // The released testkit supplies this host impl for JournalEngine<S>.
        host: engine,
    }
}
