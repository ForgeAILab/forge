use super::tests::{native_snapshot, runtime_v050_fixture_runtime, session_store};
use super::*;

/// Captured with Forge 78702e7c and runtime 82f5aac (0.5.0), before changing
/// Cargo.toml. The SQL contains exact encrypted snapshot/checkpoint rows and
/// their immutable LCM sources, nodes, operation ledger and ownership fence.
#[tokio::test]
async fn runtime_82f5aac_session_resumes_and_compacts_without_policy_reset() {
    use agent_runtime::{
        core::{
            content::UserInput,
            ids::SessionId,
            provider::{Capabilities, FinishReason, ProviderStreamEvent},
            store::SessionStore,
        },
        harness::LCM_COMPONENT_ID,
        provider::fake::{FakeProvider, ScriptedStream},
        runtime::{IdleCompactionAdmission, StartSession},
    };

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../tests/fixtures/runtime_82f5aac_session_checkpoint_lcm.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["producer_revision"],
        "82f5aac57a3b1012b4e96c1903f0ab4a0f00a625"
    );
    let (store, db, _) = session_store().await;
    for statement in fixture["inserts"].as_array().unwrap() {
        sqlx::query(statement.as_str().unwrap())
            .execute(db.pool())
            .await
            .unwrap();
    }
    assert!(SessionStore::journal(&store).is_some());
    assert!(CheckpointStore::journal(&store).is_some());
    let id = SessionId::new("state-runtime");
    let old = SessionStore::load(&store, &id).await.unwrap().unwrap();
    let old_checkpoint = CheckpointStore::load_latest(&store, &id)
        .await
        .unwrap()
        .unwrap();
    old_checkpoint.validate().unwrap();
    assert!(old_checkpoint.state.is_terminal());
    assert_eq!(serde_json::to_value(&old).unwrap(), fixture["snapshot"]);
    assert_eq!(old.history.len(), 8);
    let old_lcm = &old.extension_state[LCM_COMPONENT_ID];
    assert_eq!(
        old_lcm.revision.as_str(),
        "df66f2bd9e4f43e07789f33b6943af88"
    );
    assert_eq!(
        old_lcm.value["sizer_revision"],
        "c6ac7c823fc4bb3c826d9d3e3460706b"
    );
    assert_eq!(crate::FORGE_LCM_POLICY_REVISION, "forge-lcm-policy-2");
    let entries: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT entry_id, content_json, content_fingerprint FROM agent_lcm_entry ORDER BY sequence",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let nodes: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT node_id, summary, source_fingerprint, sizer_revision FROM agent_lcm_node ORDER BY node_id",
    ).fetch_all(db.pool()).await.unwrap();
    assert!(!nodes.is_empty());
    let revision: i64 = sqlx::query_scalar("SELECT revision FROM agent_lcm_timeline")
        .fetch_one(db.pool())
        .await
        .unwrap();

    let provider = Arc::new(FakeProvider::new(
        "fake",
        Capabilities::basic_streaming(),
        vec![ScriptedStream::new(vec![
            ProviderStreamEvent::TextDelta {
                text: "continued after the runtime upgrade".into(),
            },
            ProviderStreamEvent::Finish {
                reason: FinishReason::Stop,
            },
        ])],
    ));
    let runtime = runtime_v050_fixture_runtime(&store, id.as_str(), provider.clone()).await;
    let session = runtime
        .start_session(StartSession::resume(id.clone()))
        .await
        .unwrap();
    assert!(session.resumed());
    assert!(
        provider.requests().is_empty(),
        "resume must not call the provider"
    );
    assert_eq!(session.history(), old.history);
    let rebuilt = session.snapshot();
    let new_lcm = &rebuilt.extension_state[LCM_COMPONENT_ID];
    assert_ne!(
        new_lcm.revision, old_lcm.revision,
        "the component fingerprint includes the sizer"
    );
    assert_ne!(
        new_lcm.value["sizer_revision"],
        old_lcm.value["sizer_revision"]
    );
    for field in ["binding_revision", "classifier_revision", "timeline_id"] {
        assert_eq!(
            new_lcm.value[field], old_lcm.value[field],
            "identity field {field}"
        );
    }
    assert_eq!(
        new_lcm.value["algorithm_revision"],
        serde_json::to_value(
            agent_runtime::harness::LcmCoordinatorPolicy::default().algorithm_revision
        )
        .unwrap(),
        "derived algorithm state adopts the pinned runtime policy"
    );
    assert_ne!(
        new_lcm.value["algorithm_revision"],
        old_lcm.value["algorithm_revision"]
    );
    let compacted = session.try_idle_compaction().await.unwrap();
    assert!(matches!(
        compacted,
        IdleCompactionAdmission::Accepted { changed: true, .. }
    ));
    let compacted_revision: i64 = sqlx::query_scalar("SELECT revision FROM agent_lcm_timeline")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert!(
        compacted_revision > revision,
        "the resumed session must actually compact"
    );
    let turn = session
        .run(UserInput::text("continue after compacting"))
        .await
        .unwrap();
    turn.completed().await;
    // Completion must release admission before an immediate host operation.
    assert!(!matches!(
        session.try_idle_compaction().await.unwrap(),
        IdleCompactionAdmission::Busy
    ));
    session.persist().await.unwrap();
    assert_eq!(provider.requests().len(), 1);
    let saved = native_snapshot(&store, &id).await;
    assert_eq!(&saved.history[..old.history.len()], old.history.as_slice());
    assert_eq!(saved.history.len(), 10);
    let retained: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT entry_id, content_json, content_fingerprint FROM agent_lcm_entry WHERE sequence < 8 ORDER BY sequence",
    ).fetch_all(db.pool()).await.unwrap();
    assert_eq!(retained, entries);
    for (node_id, summary, source, sizer) in nodes {
        let retained: (String, String, String) = sqlx::query_as(
            "SELECT summary, source_fingerprint, sizer_revision FROM agent_lcm_node WHERE node_id = ?",
        ).bind(node_id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(retained, (summary, source, sizer));
    }
    drop(session);
    drop(runtime);
    let runtime = runtime_v050_fixture_runtime(&store, id.as_str(), provider.clone()).await;
    let resumed = runtime
        .start_session(StartSession::resume(id))
        .await
        .unwrap();
    assert_eq!(resumed.history(), saved.history);
    assert_eq!(
        provider.requests().len(),
        1,
        "a second resume needs no provider work"
    );
}
