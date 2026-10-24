use std::sync::Arc;

use agent_runtime::core::journal::{
    JournalBodySharing, JournalDomain, JournalEngine, JournalEngineConfig, JournalFence,
    JournalRevision, JournalRole, JournalSessionRecord, JournalStorage, JournalStorageId,
    JournalWrite, VolatileJournalStorage,
};
use agent_runtime_testkit::conformance::journal::JournalCollectionHost;
use forge_agent_host::AGENT_RUNTIME_REVISION;

#[tokio::test]
async fn engine_release_contract() {
    const RELEASE_SHA: &str = "c036af7b4635cf7d76451aaa666a9df87405ca1a";
    assert_eq!(AGENT_RUNTIME_REVISION, RELEASE_SHA);
    let mut packages = 0;
    let mut testkit = false;
    for package in include_str!("../../../Cargo.lock").split("[[package]]") {
        let Some(name) = package
            .lines()
            .find_map(|line| line.strip_prefix("name = \""))
            .map(|name| name.trim_end_matches('"'))
            .filter(|name| *name == "agent-runtime" || name.starts_with("agent-runtime-"))
        else {
            continue;
        };
        assert!(package.contains("\nversion = \"0.10.0\"\n"), "{name}");
        let source = package
            .lines()
            .find_map(|line| line.strip_prefix("source = \""))
            .expect("every runtime package has a git source")
            .trim_end_matches('"');
        assert_eq!(
            source,
            format!(
                "git+https://github.com/ForgeAILab/agent-runtime.git?rev={RELEASE_SHA}#{RELEASE_SHA}"
            ),
            "{name}"
        );
        packages += 1;
        testkit |= name == "agent-runtime-testkit";
    }
    assert!(
        packages > 1 && testkit,
        "runtime and testkit must be locked"
    );

    // Exercise the released boundary independently of production wiring.
    let storage = Arc::new(VolatileJournalStorage::new(JournalBodySharing::PerSession));
    assert_eq!(
        JournalStorage::sharing(&*storage),
        JournalBodySharing::PerSession
    );
    let domain = JournalDomain {
        storage_id: JournalStorageId::new("forge.release-contract"),
        role: JournalRole::Protected,
    };
    let retained: Option<u64> = JournalStorage::retained_bytes(&*storage, &domain)
        .await
        .expect("retained-byte diagnostic");
    assert_eq!(retained, None);
    let engine: JournalEngine<VolatileJournalStorage> =
        JournalEngine::protected(storage, domain.storage_id, JournalEngineConfig::default());
    assert_eq!(engine.retained_bytes().await.expect("engine diagnostic"), 0);
    fn collection_host(_: &impl JournalCollectionHost) {}
    collection_host(&engine);

    let fence = JournalFence {
        revision: JournalRevision(0),
        writer_epoch: 0,
        generation: 0,
    };
    assert_eq!(fence, JournalFence::default());
    // These function pointers enforce a full fence and a required record, even
    // on control writes; the pre-generation tuple/Option API must not compile.
    let _: fn(&JournalWrite) -> JournalFence = |write| write.expect;
    let _: fn(&JournalWrite) -> &JournalSessionRecord = |write| &write.record;
    let _: fn(&JournalSessionRecord) -> u64 = |record| record.generation;
}
