//! Legacy LCM conversion, profile ownership, and lazy resume.

use super::*;

#[tokio::test]
async fn review_m1_foreground_accepts_legacy_sibling_shapes() {
    use agent_runtime::harness::{LCM_COMPONENT_ID, LcmRecoveryPolicy};
    use forge_agent_host::test_support::LegacyLcmFixture;
    for case in [
        "rotation-linked",
        "failed",
        "cancelled",
        "replaced",
        "newer",
        "topic",
        "rotation",
        "unrelated-published",
        "unrelated-retired",
    ] {
        let id = SessionId::new(format!("review-m1-{case}"));
        let (f, store) = fixture(&id).await;
        let (old, older) =
            LegacyLcmFixture::unowned_pre_topic_profile_switch(&f, &store, &id).await;
        let db = f.database();
        let target = f.ensure_session(&id).await.unwrap();
        let source = f.ensure_session(&older).await.unwrap();
        match case {
            "rotation-linked" => {
                sqlx::query("UPDATE agent_session SET status='replaced',replaced_by_session_id=? WHERE id=?").bind(&target).bind(&source).execute(db.pool()).await.unwrap();
                sqlx::query("UPDATE agent_session SET predecessor_session_id=? WHERE id=?")
                    .bind(&source)
                    .bind(&target)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            "failed" | "cancelled" | "replaced" => {
                sqlx::query("UPDATE agent_session SET status=? WHERE id=?")
                    .bind(case)
                    .bind(&source)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            "newer" => {
                sqlx::query(
                    "UPDATE agent_session SET created_at='2026-09-25T00:00:00Z' WHERE id=?",
                )
                .bind(&source)
                .execute(db.pool())
                .await
                .unwrap();
            }
            "topic" => {
                sqlx::query("INSERT INTO agent_chat_topic(id,chat_id,sequence,label,starting_message_sequence,principal_type,created_at,runtime_session_id) VALUES ('topic',?,0,'Synthetic',0,'system','c',?)").bind(format!("chat:{id}")).bind(id.as_str()).execute(db.pool()).await.unwrap();
            }
            "rotation" => {
                sqlx::query("INSERT INTO agent_chat_topic_rotation(chat_id,id,label,cause,created_at) VALUES (?,'rotation','Synthetic','idle','c')").bind(format!("chat:{id}")).execute(db.pool()).await.unwrap();
            }
            "unrelated-published" | "unrelated-retired" => {
                let unrelated = SessionId::new(format!("{id}:unrelated"));
                let owner = format!("journal-fixture:{unrelated}");
                sqlx::query("INSERT INTO agent_session(id,identity_id,profile_id,context_scope_id,backend_kind,runtime_session_id,status,created_at,updated_at) VALUES (?,'journal-agent','glm-profile','journal-scope','native',?,'suspended','2026-09-16T00:00:00Z','c')")
                    .bind(&owner).bind(unrelated.as_str()).execute(db.pool()).await.unwrap();
                let mut plain = old.snapshot.clone();
                plain.id = unrelated.clone();
                plain.extension_state.remove(LCM_COMPONENT_ID);
                store
                    .seed_legacy_bytes(
                        &unrelated,
                        Some(&serde_json::to_vec(&plain).unwrap()),
                        None,
                        false,
                    )
                    .await
                    .unwrap();
                store
                    .convert_legacy_session(&owner, &unrelated)
                    .await
                    .unwrap();
                if case == "unrelated-retired" {
                    sqlx::query("UPDATE agent_session_journal_session SET retired=1 WHERE runtime_session_id=?").bind(unrelated.as_str()).execute(db.pool()).await.unwrap();
                }
            }
            _ => unreachable!(),
        }
        let before: (String, Option<String>, i64) = sqlx::query_as(
            "SELECT status,replaced_by_session_id,version FROM agent_session WHERE id=?",
        )
        .bind(&source)
        .fetch_one(db.pool())
        .await
        .unwrap();
        let p = provider();
        let (rt, timeline) =
            profile_lcm_runtime(store.clone(), p.clone(), &id, &format!("chat:{id}")).await;
        assert_eq!(timeline, old.timeline_id, "{case}");
        let session = rt
            .start_session(
                StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Adopt),
            )
            .await
            .unwrap();
        assert_eq!(
            session.history(),
            old.checkpoint.snapshot.history.as_slice(),
            "{case}"
        );
        assert!(p.requests().is_empty(), "{case}");
        drop(session);
        drop(rt);
        let receipts: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment WHERE timeline_id=?",
        )
        .bind(&old.timeline_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(receipts, 10, "{case}");
        let after: (String, Option<String>, i64) = sqlx::query_as(
            "SELECT status,replaced_by_session_id,version FROM agent_session WHERE id=?",
        )
        .bind(&source)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            after, before,
            "receipt must retain other-profile status and links: {case}"
        );
    }
}

#[tokio::test]
async fn review_m2_owned_shared_timeline_defers_under_unrelated_selection() {
    use agent_runtime::harness::LCM_COMPONENT_ID;
    use forge_agent_host::test_support::LegacyLcmFixture;
    for empty_profile in [false, true] {
        let id = SessionId::new(format!("review-m2-{empty_profile}"));
        let (f, store) = fixture(&id).await;
        let (old, older) = LegacyLcmFixture::shared_pre_topic_profile_switch(&f, &store, &id).await;
        let db = f.database();
        if empty_profile {
            sqlx::query("INSERT INTO agent_profile(id,identity_id,backend_kind,executor_type,created_at,updated_at) VALUES ('empty','journal-agent','native','embedded','c','u')").execute(db.pool()).await.unwrap();
            sqlx::query(
                "UPDATE agent_identity SET selected_profile_id='empty' WHERE id='journal-agent'",
            )
            .execute(db.pool())
            .await
            .unwrap();
        } else {
            let replacement = old.bind_empty_replacement(&f).await;
            let mut target = old.checkpoint.snapshot.clone();
            target
                .extension_state
                .get_mut(LCM_COMPONENT_ID)
                .unwrap()
                .value["timeline_id"] = serde_json::json!(replacement.timeline_id());
            let cp = TurnCheckpoint::session_boundary(target.clone(), 6).unwrap();
            store
                .seed_legacy_bytes(
                    &id,
                    Some(&serde_json::to_vec(&target).unwrap()),
                    Some(&serde_json::to_vec(&cp).unwrap()),
                    false,
                )
                .await
                .unwrap();
        }
        let expected = SessionStore::load(&*store, &older).await.unwrap().unwrap();
        let owner = f.ensure_session(&older).await.unwrap();
        let error = store
            .convert_legacy_session(&owner, &older)
            .await
            .unwrap_err();
        assert!(error.retryable);
        assert_eq!(
            error.message,
            forge_agent_host::session_journal_conversion::SHARED_LCM_CONVERSION_DEFERRED
        );
        assert_eq!(
            SessionStore::load(&*store, &older).await.unwrap().unwrap(),
            expected
        );
        assert!(
            !db.session_persistence_flags(&owner)
                .await
                .unwrap()
                .ordinary_published
        );
        let facts:(String,i64)=sqlx::query_as("SELECT runtime_session_id,(SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment) FROM agent_lcm_timeline WHERE id=?").bind(&old.timeline_id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(facts, (older.to_string(), 0));
    }
}

#[tokio::test]
async fn review_m3_target_owned_timeline_receipts_allow_switch_back() {
    use agent_runtime::harness::{LCM_COMPONENT_ID, LcmRecoveryPolicy};
    use forge_agent_host::{
        session_journal_conversion::ConversionOutcome, test_support::LegacyLcmFixture,
    };
    for published in [false, true] {
        let id = SessionId::new(format!("review-m3-{published}"));
        let (f, store) = fixture(&id).await;
        let (old, older) =
            LegacyLcmFixture::target_owned_pre_topic_profile_switch(&f, &store, &id).await;
        let db = f.database();
        let mut expected = CheckpointStore::load_latest(&*store, &older)
            .await
            .unwrap()
            .unwrap()
            .snapshot;
        expected.extension_state.remove(LCM_COMPONENT_ID);
        if published {
            // Publish using the importer directly, as a released build did.
            use agent_runtime::harness::{
                LcmCoordinator, LcmCoordinatorPolicy, StaticLcmTimelineResolver,
            };
            use db::AgentLcmRepo;
            let lcm = forge_agent_host::SqliteLcmStore::new(
                db.clone(),
                db.get_lcm_timeline(&old.timeline_id)
                    .await
                    .unwrap()
                    .unwrap(),
            );
            let binding = lcm.runtime_binding(id.clone()).unwrap();
            let coordinator = LcmCoordinator::new(
                Arc::new(lcm),
                Arc::new(forge_agent_host::DeterministicLcmSummaryModel::default()),
                Arc::new(StaticLcmTimelineResolver::new(binding)),
                LcmCoordinatorPolicy {
                    input_budget_tokens: 28_000,
                    ..Default::default()
                },
            )
            .unwrap();
            let rt = runtime_builder(store.clone(), provider())
                .lcm(Arc::new(coordinator))
                .build()
                .unwrap();
            let session = rt
                .start_session(
                    StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Adopt),
                )
                .await
                .unwrap();
            drop(session);
            drop(rt);
        }
        if published {
            // Foreground resolve of the already-native owner must receipt siblings
            // before the user switches back, without reimporting that owner.
            let before = snapshot(&store.journals().protected(), &id).await;
            assert_eq!(
                store
                    .lcm_store_for_runtime_session(id.as_str(), "agent_chat", &format!("chat:{id}"))
                    .await
                    .unwrap()
                    .timeline_id(),
                old.timeline_id
            );
            assert_eq!(snapshot(&store.journals().protected(), &id).await, before);
            let receipts: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment WHERE timeline_id=?",
            )
            .bind(&old.timeline_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
            assert_eq!(receipts, 10);
            sqlx::query("UPDATE agent_identity SET selected_profile_id='glm-profile' WHERE id='journal-agent'").execute(db.pool()).await.unwrap();
        }
        let source = f.ensure_session(&older).await.unwrap();
        assert_eq!(
            store.convert_legacy_session(&source, &older).await.unwrap(),
            ConversionOutcome::Converted
        );
        assert_eq!(
            snapshot(&store.journals().protected(), &older).await,
            expected
        );
        let facts:(String,i64)=sqlx::query_as("SELECT runtime_session_id,(SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment WHERE timeline_id=t.id) FROM agent_lcm_timeline t WHERE id=?").bind(&old.timeline_id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(facts, (id.to_string(), 10));
        sqlx::query(
            "UPDATE agent_identity SET selected_profile_id='glm-profile' WHERE id='journal-agent'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let p = provider();
        let (rt, new_timeline) =
            profile_lcm_runtime(store.clone(), p.clone(), &older, &format!("chat:{id}")).await;
        assert_ne!(new_timeline, old.timeline_id);
        let session = rt
            .start_session(
                StartSession::resume(older.clone()).with_lcm_policy(LcmRecoveryPolicy::Adopt),
            )
            .await
            .unwrap();
        assert_eq!(session.history(), expected.history.as_slice());
        assert!(p.requests().is_empty());
    }
}

#[tokio::test]
async fn foreground_unowned_resume_succeeds_while_sibling_conversion_is_admitted() {
    use agent_runtime::harness::{LCM_COMPONENT_ID, LcmRecoveryPolicy};
    use forge_agent_host::{
        session_journal_conversion::ConversionOutcome, test_support::LegacyLcmFixture,
    };
    let id = SessionId::new("review-foreground-race");
    let (f, store) = fixture(&id).await;
    let (old, older) = LegacyLcmFixture::unowned_pre_topic_profile_switch(&f, &store, &id).await;
    let source = f.ensure_session(&older).await.unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    store.pause_conversion_for_test(barrier.clone());
    let pending = tokio::spawn({
        let store = store.clone();
        let older = older.clone();
        async move { store.convert_legacy_session(&source, &older).await }
    });
    barrier.wait().await;
    let foreground = store.foreground_session(&id).await.unwrap();
    let p = provider();
    let (rt, timeline) =
        profile_lcm_runtime(store.clone(), p.clone(), &id, &format!("chat:{id}")).await;
    assert_eq!(timeline, old.timeline_id);
    let session = rt
        .start_session(StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Adopt))
        .await
        .unwrap();
    assert_eq!(
        session.history(),
        old.checkpoint.snapshot.history.as_slice()
    );
    assert!(p.requests().is_empty());
    drop(session);
    drop(rt);
    drop(foreground);
    barrier.wait().await;
    assert_eq!(
        pending.await.unwrap().unwrap(),
        ConversionOutcome::Converted
    );
    assert!(
        !snapshot(&store.journals().protected(), &older)
            .await
            .extension_state
            .contains_key(LCM_COMPONENT_ID)
    );
    let receipts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment WHERE timeline_id=?",
    )
    .bind(&old.timeline_id)
    .fetch_one(f.database().pool())
    .await
    .unwrap();
    assert_eq!(receipts, 10);
}

#[tokio::test]
async fn unowned_shared_lcm_conversion_is_independent_of_session_order() {
    use agent_runtime::harness::LCM_COMPONENT_ID;
    use forge_agent_host::{
        session_journal_conversion::ConversionOutcome, test_support::LegacyLcmFixture,
    };
    for a_first in [true, false] {
        let id = SessionId::new(format!("unowned-profile-order-{a_first}"));
        let (f, store) = fixture(&id).await;
        let (old, older) =
            LegacyLcmFixture::unowned_pre_topic_profile_switch(&f, &store, &id).await;
        let db = f.database();
        let original_a = CheckpointStore::load_latest(&*store, &older)
            .await
            .unwrap()
            .unwrap();
        let before: Vec<(String, String, String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
            .fetch_all(db.pool()).await.unwrap();
        for runtime in if a_first {
            [&older, &id]
        } else {
            [&id, &older]
        } {
            let owner = f.ensure_session(runtime).await.unwrap();
            assert_eq!(
                store.convert_legacy_session(&owner, runtime).await.unwrap(),
                ConversionOutcome::Converted
            );
            if runtime == &older {
                let imported = snapshot(&store.journals().protected(), &older).await;
                assert!(!imported.extension_state.contains_key(LCM_COMPONENT_ID));
                let mut expected = original_a.snapshot.clone();
                expected.extension_state.remove(LCM_COMPONENT_ID);
                assert_eq!(imported, expected);
                let receipts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment WHERE session_id=? AND timeline_id=? AND transferred_to_runtime_session_id=?")
                    .bind(&owner).bind(&old.timeline_id).bind(id.as_str()).fetch_one(db.pool()).await.unwrap();
                assert_eq!(receipts, 1);
            }
        }
        assert_eq!(
            snapshot(&store.journals().protected(), &id).await,
            old.checkpoint.snapshot
        );
        let facts: (String, i64, i64, i64, i64) = sqlx::query_as("SELECT runtime_session_id,revision,(SELECT COUNT(*) FROM agent_lcm_entry WHERE timeline_id=t.id),(SELECT COUNT(*) FROM agent_lcm_node WHERE timeline_id=t.id),(SELECT COUNT(*) FROM agent_lcm_operation WHERE timeline_id=t.id) FROM agent_lcm_timeline t WHERE id=?")
            .bind(&old.timeline_id).fetch_one(db.pool()).await.unwrap();
        assert_eq!(facts, (id.to_string(), 131, 114, 0, 121));
        let after: Vec<(String, String, String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
            .fetch_all(db.pool()).await.unwrap();
        assert_eq!(before, after);
    }
}

#[tokio::test]
async fn shared_lcm_nonresume_same_profile_converts_without_lcm() {
    use agent_runtime::harness::LCM_COMPONENT_ID;
    use forge_agent_host::{
        session_journal_conversion::ConversionOutcome, test_support::LegacyLcmFixture,
    };
    let id = SessionId::new("same-profile-nontarget");
    let (f, store) = fixture(&id).await;
    let (old, _) = LegacyLcmFixture::unowned_pre_topic_profile_switch(&f, &store, &id).await;
    let sibling = SessionId::new(format!("{id}:gemini:0"));
    let mut expected = CheckpointStore::load_latest(&*store, &sibling)
        .await
        .unwrap()
        .unwrap()
        .snapshot;
    assert_eq!(expected.history.len(), 84);
    expected.extension_state.remove(LCM_COMPONENT_ID);
    let owner = f.ensure_session(&sibling).await.unwrap();
    assert_eq!(
        store
            .convert_legacy_session(&owner, &sibling)
            .await
            .unwrap(),
        ConversionOutcome::Converted
    );
    assert_eq!(
        snapshot(&store.journals().protected(), &sibling).await,
        expected
    );
    assert_eq!(
        store
            .convert_legacy_session(&f.ensure_session(&id).await.unwrap(), &id)
            .await
            .unwrap(),
        ConversionOutcome::Converted
    );
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
    let receipt: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment WHERE session_id=? AND timeline_id=? AND transferred_to_runtime_session_id=?")
        .bind(owner).bind(old.timeline_id).bind(id.as_str()).fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(receipt, 1);
}

#[tokio::test]
async fn selected_profile_owns_shared_pre_topic_lcm_after_profile_switch() {
    use agent_runtime::harness::{LCM_COMPONENT_ID, LcmRecoveryPolicy};
    use forge_agent_host::{
        session_journal_conversion::ConversionOutcome, test_support::LegacyLcmFixture,
    };
    let id = SessionId::new("profile-switch-conversion");
    let (f, store) = fixture(&id).await;
    let (old, older) = LegacyLcmFixture::shared_pre_topic_profile_switch(&f, &store, &id).await;
    let db = f.database();
    let older_snapshot = SessionStore::load(&*store, &older).await.unwrap().unwrap();
    let older_checkpoint = CheckpointStore::load_latest(&*store, &older)
        .await
        .unwrap()
        .unwrap();
    assert!(
        older_checkpoint
            .snapshot
            .extension_state
            .contains_key(LCM_COMPONENT_ID)
    );
    let before: Vec<(String,String,String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
        .fetch_all(db.pool()).await.unwrap();
    let shape: Vec<(String, i64)> = sqlx::query_as("SELECT profile_id,COUNT(*) FROM agent_session WHERE status='suspended' AND backend_kind='native' AND predecessor_session_id IS NULL AND replaced_by_session_id IS NULL GROUP BY profile_id ORDER BY profile_id")
        .fetch_all(db.pool()).await.unwrap();
    assert_eq!(
        shape,
        vec![("gemini-profile".into(), 3), ("glm-profile".into(), 10)]
    );
    let owner = f.ensure_session(&id).await.unwrap();
    assert_eq!(
        db.dormant_native_chat_session_id("journal-agent", "journal-scope", "gemini-profile")
            .await
            .unwrap(),
        Some(owner.clone())
    );
    assert_eq!(
        store.convert_legacy_session(&owner, &id).await.unwrap(),
        ConversionOutcome::Converted
    );
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
    let retained: (String, Option<String>, i64) = sqlx::query_as("SELECT status,replaced_by_session_id,version FROM agent_session WHERE runtime_session_id=?")
        .bind(older.as_str()).fetch_one(db.pool()).await.unwrap();
    assert_eq!(retained, ("suspended".into(), None, 1));
    let older_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_session WHERE profile_id='glm-profile' AND status='suspended' AND replaced_by_session_id IS NULL AND predecessor_session_id IS NULL AND version=1")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(older_rows, 10);
    let replaced: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_session WHERE profile_id='gemini-profile' AND status='replaced' AND replaced_by_session_id=? AND version=2")
        .bind(&owner).fetch_one(db.pool()).await.unwrap();
    assert_eq!(replaced, 2);
    let publications: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_session_journal_session j JOIN agent_session s ON s.id=j.forge_session_id WHERE s.profile_id='glm-profile' AND (j.published=1 OR j.retired=1)")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(publications, 0);
    let receipts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_legacy_lcm_relinquishment WHERE timeline_id=?",
    )
    .bind(&old.timeline_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(receipts, 12);
    assert_eq!(
        store.convert_legacy_session(&owner, &id).await.unwrap(),
        ConversionOutcome::AlreadyNative
    );
    let timeline: (String,i64,i64,i64,i64) = sqlx::query_as("SELECT runtime_session_id,revision,(SELECT COUNT(*) FROM agent_lcm_entry),(SELECT COUNT(*) FROM agent_lcm_operation),(SELECT COUNT(*) FROM agent_lcm_timeline) FROM agent_lcm_timeline WHERE id=?")
        .bind(&old.timeline_id).fetch_one(db.pool()).await.unwrap();
    assert_eq!(timeline, (id.to_string(), 131, 114, 121, 1));
    drop(store);
    let store = Arc::new(SqliteProtectedRuntimeStore::new(
        f.reopen().await.unwrap(),
        [7; 32],
        1,
    ));
    let p = provider();
    let chat = format!("chat:{id}");
    let (rt, b_timeline) = profile_lcm_runtime(store.clone(), p.clone(), &id, &chat).await;
    assert_eq!(b_timeline, old.timeline_id);
    let b = rt
        .start_session(StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Adopt))
        .await
        .unwrap();
    assert_eq!(b.history(), old.snapshot.history.as_slice());
    assert!(p.requests().is_empty());
    drop(b);
    drop(rt);
    sqlx::query("UPDATE agent_identity SET selected_profile_id='glm-profile',version=version+1 WHERE id='journal-agent'")
        .execute(db.pool()).await.unwrap();
    let older_owner = f.ensure_session(&older).await.unwrap();
    assert_eq!(
        db.dormant_native_chat_session_id("journal-agent", "journal-scope", "glm-profile")
            .await
            .unwrap(),
        Some(older_owner.clone())
    );
    assert_eq!(
        store
            .convert_legacy_session(&older_owner, &older)
            .await
            .unwrap(),
        ConversionOutcome::Converted
    );
    let mut expected = older_snapshot;
    expected.extension_state.remove(LCM_COMPONENT_ID);
    assert_eq!(
        snapshot(&store.journals().ordinary(), &older).await,
        expected
    );
    let mut protected = older_checkpoint.snapshot;
    protected.extension_state.remove(LCM_COMPONENT_ID);
    assert_eq!(
        snapshot(&store.journals().protected(), &older).await,
        protected
    );
    assert!(protected.extension_state.contains_key("fixture.protected"));
    assert_eq!(
        store
            .convert_legacy_session(&older_owner, &older)
            .await
            .unwrap(),
        ConversionOutcome::AlreadyNative
    );
    let a_provider = provider();
    let (a_rt, a_timeline) =
        profile_lcm_runtime(store.clone(), a_provider.clone(), &older, &chat).await;
    assert_ne!(a_timeline, old.timeline_id);
    let a = a_rt
        .start_session(StartSession::resume(older.clone()))
        .await
        .unwrap();
    assert_eq!(a.history(), protected.history.as_slice());
    assert!(a_provider.requests().is_empty());
    a.run(agent_runtime::core::content::UserInput::text(
        "Resume GLM with rebuilt LCM",
    ))
    .await
    .unwrap()
    .completed()
    .await;
    assert_eq!(a.history().last().unwrap().joined_text(), "reply");
    assert_eq!(a_provider.requests().len(), 1);
    a.shutdown().await.unwrap();
    let after: Vec<(String,String,String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
        .fetch_all(db.pool()).await.unwrap();
    assert_eq!(before, after);
    let shared: (String,i64,i64) = sqlx::query_as("SELECT runtime_session_id,(SELECT COUNT(*) FROM agent_lcm_entry WHERE timeline_id=t.id),(SELECT COUNT(*) FROM agent_lcm_operation WHERE timeline_id=t.id) FROM agent_lcm_timeline t WHERE id=?")
        .bind(&old.timeline_id).fetch_one(db.pool()).await.unwrap();
    assert_eq!(shared, (id.to_string(), 114, 121));
}

#[tokio::test]
async fn selected_profile_shared_lcm_lazy_resume_without_provider_calls() {
    use agent_runtime::harness::LcmRecoveryPolicy;
    use forge_agent_host::test_support::LegacyLcmFixture;
    let id = SessionId::new("selected-profile-lazy");
    let (f, store) = fixture(&id).await;
    let (old, older) = LegacyLcmFixture::shared_pre_topic_profile_switch(&f, &store, &id).await;
    sqlx::query(
        "UPDATE agent_session SET status='ready',version=version+1 WHERE runtime_session_id=?",
    )
    .bind(id.as_str())
    .execute(f.database().pool())
    .await
    .unwrap();
    let _foreground = store.foreground_session(&id).await.unwrap();
    let p = provider();
    let (rt, timeline) =
        profile_lcm_runtime(store.clone(), p.clone(), &id, &format!("chat:{id}")).await;
    assert_eq!(timeline, old.timeline_id);
    let b = rt
        .start_session(StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Adopt))
        .await
        .unwrap();
    assert_eq!(b.history(), old.snapshot.history.as_slice());
    assert_eq!(b.history().len(), 114);
    assert!(p.requests().is_empty());
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
    let a: (String,Option<String>,i64)=sqlx::query_as("SELECT status,replaced_by_session_id,version FROM agent_session WHERE runtime_session_id=?")
        .bind(older.as_str()).fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(a, ("suspended".into(), None, 1));
}

#[tokio::test]
async fn sibling_owned_pre_topic_lcm_converts_and_retries() {
    use forge_agent_host::{
        session_journal_conversion::ConversionOutcome, test_support::LegacyLcmFixture,
    };
    let id = SessionId::new("sibling-owner-conversion");
    let (f, store) = fixture(&id).await;
    let (old, older) = LegacyLcmFixture::shared_pre_topic_chat(&f, &store, &id).await;
    let owner = f.ensure_session(&id).await.unwrap();
    let selected = f
        .database()
        .dormant_native_chat_session_id("journal-agent", "journal-scope", "journal-profile")
        .await
        .unwrap();
    assert_eq!(selected.as_deref(), Some(owner.as_str()));
    let before: Vec<(String, String, String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
        .fetch_all(f.database().pool()).await.unwrap();
    assert_eq!(
        store.convert_legacy_session(&owner, &id).await.unwrap(),
        ConversionOutcome::Converted
    );
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
    assert_eq!(
        snapshot(&store.journals().ordinary(), &id).await,
        old.snapshot
    );
    assert_eq!(
        store.convert_legacy_session(&owner, &id).await.unwrap(),
        ConversionOutcome::AlreadyNative
    );
    let after: Vec<(String, String, String)> = sqlx::query_as("SELECT session_id,quote(snapshot_ciphertext),quote(checkpoint_ciphertext) FROM protected_agent_session_state ORDER BY session_id")
        .fetch_all(f.database().pool()).await.unwrap();
    assert_eq!(before, after);
    let bindings: Vec<(String, String)> =
        sqlx::query_as("SELECT runtime_session_id,timeline_id FROM agent_runtime_lcm_binding")
            .fetch_all(f.database().pool())
            .await
            .unwrap();
    assert_eq!(bindings, vec![(id.to_string(), old.timeline_id.clone())]);
    let rows: (String, i64, i64, i64, i64) = sqlx::query_as("SELECT runtime_session_id,(SELECT COUNT(*) FROM agent_lcm_entry),(SELECT COUNT(*) FROM agent_lcm_timeline),revision,(SELECT COUNT(*) FROM agent_lcm_operation) FROM agent_lcm_timeline WHERE id=?")
        .bind(&old.timeline_id).fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(rows, (id.to_string(), 114, 1, 131, 121));
    assert_eq!(
        f.ensure_session(&older).await.unwrap(),
        format!("journal-fixture:{older}")
    );
}

#[tokio::test]
async fn sibling_owned_pre_topic_lcm_lazy_resume_without_provider_calls() {
    use agent_runtime::harness::{
        LcmCoordinator, LcmCoordinatorPolicy, LcmRecoveryPolicy, StaticLcmTimelineResolver,
    };
    use forge_agent_host::test_support::LegacyLcmFixture;
    let id = SessionId::new("sibling-owner-lazy");
    let (f, store) = fixture(&id).await;
    let (old, older) = LegacyLcmFixture::shared_pre_topic_chat(&f, &store, &id).await;
    // Foreground activation changes B to ready before preparing its LCM store.
    sqlx::query(
        "UPDATE agent_session SET status='ready',version=version+1 WHERE runtime_session_id=?",
    )
    .bind(id.as_str())
    .execute(f.database().pool())
    .await
    .unwrap();
    let _foreground = store.foreground_session(&id).await.unwrap();
    let lcm = store
        .lcm_store_for_runtime_session(id.as_str(), "agent_chat", &format!("chat:{id}"))
        .await
        .unwrap();
    let binding = lcm.runtime_binding(id.clone()).unwrap();
    assert_eq!(binding.timeline.as_str(), old.timeline_id);
    let coordinator = LcmCoordinator::new(
        Arc::new(lcm),
        Arc::new(forge_agent_host::DeterministicLcmSummaryModel::default()),
        Arc::new(StaticLcmTimelineResolver::new(binding)),
        LcmCoordinatorPolicy {
            input_budget_tokens: 28_000,
            ..Default::default()
        },
    )
    .unwrap();
    let p = provider();
    let rt = runtime_builder(store.clone(), p.clone())
        .lcm(Arc::new(coordinator))
        .build()
        .unwrap();
    let session = rt
        .start_session(StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Adopt))
        .await
        .unwrap();
    assert_eq!(session.id(), &id);
    assert_eq!(session.history(), old.snapshot.history.as_slice());
    assert_eq!(session.history().len(), 114);
    assert!(p.requests().is_empty());
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
    let superseded: (String, Option<String>, i64) = sqlx::query_as("SELECT status,replaced_by_session_id,version FROM agent_session WHERE runtime_session_id=?")
        .bind(older.as_str()).fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(
        superseded,
        ("replaced".into(), Some(format!("journal-fixture:{id}")), 2)
    );
    assert!(SessionStore::load(&*store, &older).await.unwrap().is_some());
}

#[tokio::test]
async fn pre_topic_checkpoint_lcm_converts_without_changing_legacy() {
    use forge_agent_host::{
        session_journal_conversion::ConversionOutcome, test_support::LegacyLcmFixture,
    };
    let id = SessionId::new("pre-topic-conversion");
    let (f, store) = fixture(&id).await;
    let old = LegacyLcmFixture::seed(&f, &store, &id, false).await;
    let before: (String, String) = sqlx::query_as(
        "SELECT quote(snapshot_ciphertext), quote(checkpoint_ciphertext) FROM protected_agent_session_state",
    ).fetch_one(f.database().pool()).await.unwrap();
    let owner = f.ensure_session(&id).await.unwrap();
    let result = store.convert_legacy_session(&owner, &id).await;
    assert_eq!(result.unwrap(), ConversionOutcome::Converted);
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
    assert_eq!(
        snapshot(&store.journals().ordinary(), &id).await,
        old.snapshot
    );
    let after: (String, String) = sqlx::query_as(
        "SELECT quote(snapshot_ciphertext), quote(checkpoint_ciphertext) FROM protected_agent_session_state",
    ).fetch_one(f.database().pool()).await.unwrap();
    assert_eq!(before, after);
    let bound: String = sqlx::query_scalar(
        "SELECT timeline_id FROM agent_runtime_lcm_binding WHERE runtime_session_id=?",
    )
    .bind(id.as_str())
    .fetch_one(f.database().pool())
    .await
    .unwrap();
    assert_eq!(bound, old.timeline_id);
}

#[tokio::test]
async fn pre_topic_checkpoint_lcm_lazy_resume_preserves_session() {
    use agent_runtime::harness::{
        LcmCoordinator, LcmCoordinatorPolicy, LcmRecoveryPolicy, StaticLcmTimelineResolver,
    };
    use forge_agent_host::test_support::LegacyLcmFixture;
    let id = SessionId::new("pre-topic-lazy");
    let (f, store) = fixture(&id).await;
    let old = LegacyLcmFixture::seed(&f, &store, &id, false).await;
    let lcm = store
        .lcm_store_for_runtime_session(id.as_str(), "account", "journal-owner")
        .await
        .unwrap();
    let binding = lcm.runtime_binding(id.clone()).unwrap();
    let coordinator = LcmCoordinator::new(
        Arc::new(lcm),
        Arc::new(forge_agent_host::DeterministicLcmSummaryModel::default()),
        Arc::new(StaticLcmTimelineResolver::new(binding)),
        LcmCoordinatorPolicy {
            input_budget_tokens: 28_000,
            ..Default::default()
        },
    )
    .unwrap();
    let p = provider();
    let rt = runtime_builder(store.clone(), p.clone())
        .lcm(Arc::new(coordinator))
        .build()
        .unwrap();
    let session = rt
        .start_session(StartSession::resume(id.clone()).with_lcm_policy(LcmRecoveryPolicy::Adopt))
        .await
        .unwrap();
    assert_eq!(session.id(), &id);
    assert_eq!(session.history(), old.snapshot.history.as_slice());
    assert!(p.requests().is_empty());
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
}

#[tokio::test]
async fn pre_topic_failed_binding_and_unpublished_reservations_retry_successfully() {
    use agent_runtime::{
        core::error::ErrorKind,
        harness::{LcmCoordinator, LcmCoordinatorPolicy, StaticLcmTimelineResolver},
        runtime::CheckpointRecoveryPolicy,
    };
    use forge_agent_host::{
        session_journal_conversion::ConversionOutcome, test_support::LegacyLcmFixture,
    };
    let id = SessionId::new("pre-topic-retry");
    let (f, store) = fixture(&id).await;
    let old = LegacyLcmFixture::seed(&f, &store, &id, false).await;
    let wrong = old.bind_empty_replacement(&f).await;
    let binding = wrong.runtime_binding(id.clone()).unwrap();
    assert_ne!(binding.timeline.as_str(), old.timeline_id);
    assert_eq!(binding.authorization_revision.as_str(), old.checkpoint.snapshot.extension_state[agent_runtime::harness::LCM_COMPONENT_ID].value["binding_revision"].as_str().unwrap());
    // Reproduce the old background preflight and real runtime failure, leaving
    // both encrypted reservations at epoch 2, without publishing either root.
    for engine in [store.journals().protected(), store.journals().ordinary()] {
        let lease = engine
            .open(&id, open("preflight", JournalOpenIntent::ExclusiveWriter))
            .await
            .unwrap();
        engine.close(lease).await.unwrap();
    }
    let coordinator = LcmCoordinator::new(
        Arc::new(wrong),
        Arc::new(forge_agent_host::DeterministicLcmSummaryModel::default()),
        Arc::new(StaticLcmTimelineResolver::new(binding)),
        LcmCoordinatorPolicy {
            input_budget_tokens: 28_000,
            ..Default::default()
        },
    )
    .unwrap();
    let p = provider();
    let rt = runtime_builder(store.clone(), p.clone())
        .lcm(Arc::new(coordinator))
        .build()
        .unwrap();
    let error = rt
        .start_session(
            StartSession::resume(id.clone())
                .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Conflict);
    assert_eq!(
        error.message,
        "LCM state failed identity, binding, or frontier validation"
    );
    assert!(p.requests().is_empty());
    drop(rt);
    let reservations: Vec<(String, Vec<u8>, bool)> = sqlx::query_as(
        "SELECT role,writer_epoch,published FROM agent_session_journal_session WHERE runtime_session_id=? ORDER BY role",
    ).bind(id.as_str()).fetch_all(f.database().pool()).await.unwrap();
    assert_eq!(reservations.len(), 2);
    for (_, epoch, published) in reservations {
        assert_eq!(u64::from_be_bytes(epoch.try_into().unwrap()), 2);
        assert!(!published);
    }
    let owner = f.ensure_session(&id).await.unwrap();
    assert_eq!(
        store.convert_legacy_session(&owner, &id).await.unwrap(),
        ConversionOutcome::Converted
    );
    assert_eq!(
        snapshot(&store.journals().protected(), &id).await,
        old.checkpoint.snapshot
    );
    assert_eq!(
        snapshot(&store.journals().ordinary(), &id).await,
        old.snapshot
    );
    let binding: String = sqlx::query_scalar(
        "SELECT timeline_id FROM agent_runtime_lcm_binding WHERE runtime_session_id=?",
    )
    .bind(id.as_str())
    .fetch_one(f.database().pool())
    .await
    .unwrap();
    assert_eq!(binding, old.timeline_id);
    let original: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM agent_lcm_entry WHERE timeline_id=?1),
                (SELECT COUNT(*) FROM agent_lcm_node WHERE timeline_id=?1),
                (SELECT COUNT(*) FROM agent_lcm_operation WHERE timeline_id=?1)",
    )
    .bind(&old.timeline_id)
    .fetch_one(f.database().pool())
    .await
    .unwrap();
    assert_eq!(original, (120, 1, 2));
}
