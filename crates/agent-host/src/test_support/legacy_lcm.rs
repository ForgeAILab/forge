//! Synthetic pre-V149 state, following Forge 0685f7b8 / runtime 65a3970.
use super::FileDb;
use crate::{SqliteLcmStore, SqliteProtectedRuntimeStore};
use agent_runtime::{
    context::Sensitivity,
    core::{
        checkpoint::TurnCheckpoint,
        content::{ContentPart, Message},
        ids::SessionId,
        store::{SessionSnapshot, VersionedSessionState},
    },
    harness::LCM_COMPONENT_ID,
    lcm::{
        LcmAppendRequest, LcmClassification, LcmEntry, LcmEntryId, LcmNodeId, LcmOperationId,
        LcmRange, LcmReader, LcmSequence, LcmSourceMetadata, LcmTimelineId, LcmWriter, LeafCommit,
        SummaryProvenance, source_fingerprint_entries,
    },
    registry::{Fingerprint, RegistryRevision, TrustClass},
};
use db::AgentLcmRepo;
use serde_json::json;

pub struct LegacyLcmFixture {
    pub snapshot: SessionSnapshot,
    pub checkpoint: TurnCheckpoint,
    pub timeline_id: String,
}

impl LegacyLcmFixture {
    /// The empty binding the old snapshot-only resolver published before a
    /// checkpoint-only LCM import failed. No journal internals are fabricated.
    pub async fn bind_empty_replacement(&self, fixture: &FileDb) -> SqliteLcmStore {
        let db = fixture.database();
        let replacement = db::new_uuid_v4();
        sqlx::query(
            "INSERT INTO agent_lcm_timeline (id,identity_id,scope_type,scope_id,canonical_scope_id,
                runtime_session_id,authorization_revision,revision,created_at,updated_at)
             SELECT ?,identity_id,scope_type,canonical_scope_id || '#topic:' || ?,canonical_scope_id,
                ?,authorization_revision,0,created_at,updated_at FROM agent_lcm_timeline WHERE id=?",
        ).bind(&replacement).bind(self.snapshot.id.as_str()).bind(self.snapshot.id.as_str())
            .bind(&self.timeline_id).execute(db.pool()).await.unwrap();
        sqlx::query(
            "INSERT INTO agent_runtime_lcm_binding (runtime_session_id,timeline_id) VALUES (?,?)",
        )
        .bind(self.snapshot.id.as_str())
        .bind(&replacement)
        .execute(db.pool())
        .await
        .unwrap();
        SqliteLcmStore::new(
            db.clone(),
            db.get_lcm_timeline(&replacement).await.unwrap().unwrap(),
        )
    }

    pub async fn seed(
        fixture: &FileDb,
        store: &SqliteProtectedRuntimeStore,
        session: &SessionId,
        snapshot_has_lcm: bool,
    ) -> Self {
        Self::seed_history(fixture, store, session, snapshot_has_lcm, 120).await
    }

    /// A pre-topic Main Chat shared by two unlinked suspended runtimes. The
    /// older runtime owns the timeline, but the newer runtime last wrote it.
    pub async fn shared_pre_topic_chat(
        fixture: &FileDb,
        store: &SqliteProtectedRuntimeStore,
        session: &SessionId,
    ) -> (Self, SessionId) {
        let newer = fixture.ensure_session(session).await.unwrap();
        let older_runtime = SessionId::new(format!("{}:older", session.as_str()));
        let older = fixture.ensure_session(&older_runtime).await.unwrap();
        let db = fixture.database();
        sqlx::query("UPDATE agent_identity SET selected_profile_id='journal-profile' WHERE id='journal-agent'")
            .execute(db.pool()).await.unwrap();
        let chat = format!("chat:{}", session.as_str());
        sqlx::query("INSERT INTO user (id,email,password_hash,created_at,updated_at) VALUES (?1,?1,'hash','c','u')")
            .bind(format!("owner:{}", session.as_str())).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO agent_chat (id,kind,account_id,status,created_at,updated_at) VALUES (?,'account_main',?,'ready','c','u')")
            .bind(&chat).bind(format!("owner:{}", session.as_str())).execute(db.pool()).await.unwrap();
        sqlx::query("UPDATE agent_context_scope SET scope_type='agent_chat',scope_id=? WHERE id='journal-scope'")
            .bind(&chat).execute(db.pool()).await.unwrap();
        for (id, created) in [
            (&older, "2026-09-23T02:09:00Z"),
            (&newer, "2026-09-24T08:34:00Z"),
        ] {
            sqlx::query("UPDATE agent_session SET created_at=?,updated_at=? WHERE id=?")
                .bind(created)
                .bind(created)
                .bind(id)
                .execute(db.pool())
                .await
                .unwrap();
        }
        let old = Self::seed_history(fixture, store, session, false, 114).await;
        // V149 leaves runtime owners NULL. This is the later, observed owner
        // assignment, after the older runtime opened the migrated timeline.
        sqlx::query("UPDATE agent_lcm_timeline SET runtime_session_id=?,created_at='2026-09-17T00:00:00Z',updated_at='2026-09-24T08:37:00Z' WHERE id=?")
            .bind(older_runtime.as_str()).bind(&old.timeline_id).execute(db.pool()).await.unwrap();
        sqlx::query(
            "INSERT INTO agent_runtime_lcm_binding(runtime_session_id,timeline_id) VALUES (?,?)",
        )
        .bind(older_runtime.as_str())
        .bind(&old.timeline_id)
        .execute(db.pool())
        .await
        .unwrap();
        let mut older_snapshot = old.snapshot.clone();
        older_snapshot.id = older_runtime.clone();
        store
            .seed_legacy_bytes(
                &older_runtime,
                Some(&serde_json::to_vec(&older_snapshot).unwrap()),
                None,
                false,
            )
            .await
            .unwrap();
        (old, older_runtime)
    }

    /// The reported 10 GLM / 3 Gemini suspended sessions, with no topic or
    /// predecessor links and one pre-topic timeline still bound to GLM's A.
    pub async fn shared_pre_topic_profile_switch(
        fixture: &FileDb,
        store: &SqliteProtectedRuntimeStore,
        session: &SessionId,
    ) -> (Self, SessionId) {
        let (old, older) = Self::shared_pre_topic_chat(fixture, store, session).await;
        let db = fixture.database();
        sqlx::query("INSERT INTO agent_profile(id,identity_id,backend_kind,executor_type,provider,created_at,updated_at) VALUES ('gemini-profile','journal-agent','native','embedded','gemini','2026-09-23T03:58:00Z','2026-09-23T03:58:00Z')")
            .execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO agent_profile(id,identity_id,backend_kind,executor_type,provider,created_at,updated_at) VALUES ('glm-profile','journal-agent','native','embedded','glm','2026-09-17T00:00:00Z','2026-09-17T00:00:00Z')")
            .execute(db.pool()).await.unwrap();
        let mut siblings = vec![(
            older.clone(),
            "glm-profile",
            "2026-09-23T02:09:00Z".to_owned(),
        )];
        for n in 0..9 {
            siblings.push((
                SessionId::new(format!("{}:glm:{n}", session.as_str())),
                "glm-profile",
                format!("2026-09-{:02}T00:00:00Z", 17 + n / 2),
            ));
        }
        for (n, created) in ["2026-09-23T04:01:00Z", "2026-09-23T18:36:00Z"]
            .iter()
            .enumerate()
        {
            siblings.push((
                SessionId::new(format!("{}:gemini:{n}", session.as_str())),
                "gemini-profile",
                (*created).to_owned(),
            ));
        }
        for (runtime, profile, created) in siblings {
            let owner = fixture.ensure_session(&runtime).await.unwrap();
            sqlx::query(
                "UPDATE agent_session SET profile_id=?,created_at=?,updated_at=? WHERE id=?",
            )
            .bind(profile)
            .bind(&created)
            .bind(&created)
            .bind(owner)
            .execute(db.pool())
            .await
            .unwrap();
            let mut snapshot = old.checkpoint.snapshot.clone();
            snapshot.id = runtime.clone();
            snapshot.extension_state.insert(
                "fixture.protected".into(),
                VersionedSessionState::new(
                    RegistryRevision::from_content("protected-fixture"),
                    json!({"keep": runtime.as_str()}),
                ),
            );
            let checkpoint = TurnCheckpoint::session_boundary(snapshot.clone(), 6).unwrap();
            store
                .seed_legacy_bytes(
                    &runtime,
                    Some(&serde_json::to_vec(&snapshot).unwrap()),
                    Some(&serde_json::to_vec(&checkpoint).unwrap()),
                    false,
                )
                .await
                .unwrap();
        }
        sqlx::query(
            "UPDATE agent_session SET profile_id='gemini-profile' WHERE runtime_session_id=?",
        )
        .bind(session.as_str())
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE agent_identity SET selected_profile_id='gemini-profile' WHERE id='journal-agent'")
            .execute(db.pool()).await.unwrap();
        sqlx::query("DELETE FROM agent_profile WHERE id='journal-profile'")
            .execute(db.pool())
            .await
            .unwrap();
        (old, older)
    }

    async fn seed_history(
        fixture: &FileDb,
        store: &SqliteProtectedRuntimeStore,
        session: &SessionId,
        snapshot_has_lcm: bool,
        history_len: usize,
    ) -> Self {
        let db = fixture.database();
        let (identity, scope_type, scope_id): (String, String, String) = sqlx::query_as(
            "SELECT s.identity_id, c.scope_type, c.scope_id FROM agent_session s
             JOIN agent_context_scope c ON c.id=s.context_scope_id WHERE s.runtime_session_id=?",
        )
        .bind(session.as_str())
        .fetch_one(db.pool())
        .await
        .unwrap();
        let authorization = RegistryRevision::from_content(format!(
            "forge-lcm-authorization-v1\n{identity}\n{scope_type}\n{scope_id}"
        ));
        let chat_id = (scope_type == "agent_chat").then(|| scope_id.clone());
        let now = db::now_rfc3339();
        let timeline = db
            .create_or_get_lcm_timeline(db::CreateAgentLcmTimeline {
                id: db::new_uuid_v4(),
                identity_id: identity,
                scope_type,
                scope_id,
                authorization_revision: authorization.to_string(),
                created_at: now.clone(),
                updated_at: now,
            })
            .await
            .unwrap();
        let timeline_id = timeline.id.clone();
        let lcm = SqliteLcmStore::new(db.clone(), timeline);
        let history: Vec<_> = (0..history_len)
            .map(|n| {
                let text = format!(
                    "Synthetic historical message {n}. {}",
                    "context ".repeat(16)
                );
                if n % 2 == 0 {
                    Message::user(text)
                } else {
                    Message::assistant(vec![ContentPart::text(text)])
                }
            })
            .collect();
        if let Some(chat_id) = chat_id {
            for (n, message) in history.iter().enumerate() {
                sqlx::query(
                    "INSERT INTO agent_chat_message
                     (id,chat_id,sequence,author_type,content,status,correlation_id,created_at)
                     VALUES (?,?,?,?,?,'complete','legacy-lcm-fixture','2026-09-28T12:00:00Z')",
                )
                .bind(format!("legacy:{}:{n}", session.as_str()))
                .bind(&chat_id)
                .bind(n as i64)
                .bind(if n % 2 == 0 { "user" } else { "agent" })
                .bind(message.joined_text())
                .execute(db.pool())
                .await
                .unwrap();
            }
        }
        let entries: Vec<_> = history
            .iter()
            .enumerate()
            .map(|(n, message)| {
                let fingerprint =
                    agent_runtime::lcm::Fingerprint::of(serde_json::to_vec(message).unwrap());
                LcmEntry::new(
                    LcmTimelineId::new(&timeline_id),
                    LcmEntryId::new(format!("history:{n}:{fingerprint}")),
                    LcmSequence::new(n as u64),
                    message.clone(),
                    LcmSourceMetadata::new(LcmClassification::new(
                        Sensitivity::Sensitive,
                        if n % 2 == 0 {
                            TrustClass::UserContent
                        } else {
                            TrustClass::ExternalContent
                        },
                    )),
                )
            })
            .collect();
        let view = lcm.view();
        if history_len == 114 {
            for (n, entry) in entries.iter().enumerate() {
                lcm.append(
                    &view,
                    LcmAppendRequest::new(
                        LcmOperationId::new(format!("legacy-append:{n}")),
                        vec![entry.clone()],
                    ),
                )
                .await
                .unwrap();
            }
            for n in 0..6 {
                let start = 106 + n;
                // Ten historical tail truncations plus six re-appends produce
                // revision 131 with 121 operations and 114 retained entries.
                if n < 4 {
                    db.truncate_lcm_entries_from(
                        &timeline_id,
                        start as i64 + 1,
                        &db::now_rfc3339(),
                        None,
                    )
                    .await
                    .unwrap();
                }
                db.truncate_lcm_entries_from(&timeline_id, start as i64, &db::now_rfc3339(), None)
                    .await
                    .unwrap();
                lcm.append(
                    &view,
                    LcmAppendRequest::new(
                        LcmOperationId::new(format!("legacy-reappend:{n}")),
                        entries[start..].to_vec(),
                    ),
                )
                .await
                .unwrap();
            }
        } else {
            lcm.append(
                &view,
                LcmAppendRequest::new(LcmOperationId::new("legacy-append"), entries.clone()),
            )
            .await
            .unwrap();
        }
        let policy = RegistryRevision::from_content("forge-lcm-pressure-3");
        let algorithm = RegistryRevision::from_content("agent-runtime-lcm-1");
        let sizer = RegistryRevision::from_content("forge-lcm-sizer-1");
        let summary = "Synthetic historical continuity.";
        let source_tokens = entries[..2]
            .iter()
            .map(|entry| 4 + (serde_json::to_vec(&entry.content).unwrap().len() as u64).div_ceil(4))
            .sum();
        lcm.commit_leaf(
            &view,
            LeafCommit {
                expected_revision: lcm.current_revision(&view).await.unwrap(),
                operation_id: LcmOperationId::new("legacy-leaf"),
                node_id: LcmNodeId::new("legacy-leaf"),
                range: LcmRange::new(LcmSequence::new(0), LcmSequence::new(1)).unwrap(),
                entry_ids: entries[..2].iter().map(|entry| entry.id.clone()).collect(),
                source_fingerprint: source_fingerprint_entries(&entries[..2]),
                summary: summary.into(),
                token_count: 4 + (summary.len() as u64).div_ceil(4),
                source_token_count: source_tokens,
                policy_revision: policy.clone(),
                algorithm_revision: algorithm.clone(),
                sizer_revision: sizer.clone(),
                provenance: SummaryProvenance::Deterministic {
                    revision: algorithm.clone(),
                },
                classification: LcmClassification::new(
                    Sensitivity::Sensitive,
                    TrustClass::ExternalContent,
                ),
                operation_fingerprint: None,
            },
        )
        .await
        .unwrap();
        let nodes: Vec<_> = lcm.active_nodes(&view).await.unwrap().iter().map(|node| json!({
            "id": node.id, "revision": node.revision, "range": node.range,
            "source_fingerprint": node.source_fingerprint, "summary_revision": node.summary_revision,
            "token_count": node.token_count, "source_token_count": node.source_token_count,
            "policy_revision": node.policy_revision, "algorithm_revision": node.algorithm_revision,
            "sizer_revision": node.sizer_revision, "provenance": node.provenance,
            "classification": node.classification, "operation_id": node.operation_id,
            "operation_fingerprint": node.operation_fingerprint
        })).collect();
        let classifier =
            RegistryRevision::from_content("lcm-source-classifier-v1\nsensitive\nnone\nnone");
        // 65a3970's descriptor with Forge's 4096 output cap / 8192 leaf target.
        let component_revision = RegistryRevision::from_content(
            [
                "1",
                policy.as_str(),
                policy.as_str(),
                "8192",
                "512",
                algorithm.as_str(),
                algorithm.as_str(),
                sizer.as_str(),
                "forge.lcm.deterministic",
                "forge-lcm-summary-2",
                classifier.as_str(),
                crate::FORGE_LCM_STORE_REVISION,
                "none",
                "none",
            ]
            .join("\n"),
        );
        let state = VersionedSessionState::new(
            component_revision,
            json!({
                "schema_version": 1,
                "timeline_id": timeline_id,
                "binding_revision": authorization,
                "store_revision": crate::FORGE_LCM_STORE_REVISION,
                "content_guard_id": null, "content_guard_revision": null,
                "history_len": history.len(), "immutable_frontier": history.len() - 1,
                "history_fingerprint": Fingerprint::of(serde_json::to_vec(&history).unwrap()),
                "dag_revision": lcm.current_revision(&view).await.unwrap(),
                "active_nodes": nodes,
                "policy_revision": policy, "summary_policy_revision": policy,
                "algorithm_revision": algorithm, "sizer_revision": sizer,
                "model_id": "forge.lcm.deterministic", "model_revision": "forge-lcm-summary-2",
                "classifier_revision": classifier,
                "source_classification": LcmClassification::new(Sensitivity::Sensitive, TrustClass::ExternalContent),
                "model_purpose": "context.semantic_summary",
                "operation_watermarks": [], "pending_summary": null, "hard_rounds": 0
            }),
        );
        let mut protected: SessionSnapshot = serde_json::from_value(json!({
            "id": session, "history": history, "updated": 1
        }))
        .unwrap();
        protected
            .extension_state
            .insert(LCM_COMPONENT_ID.into(), state);
        let checkpoint = TurnCheckpoint::session_boundary(protected.clone(), 6).unwrap();
        let mut snapshot = protected;
        if !snapshot_has_lcm {
            snapshot.extension_state.remove(LCM_COMPONENT_ID);
        }
        Self {
            snapshot,
            checkpoint,
            timeline_id,
        }
        .write(store)
        .await
    }

    pub async fn write(self, store: &SqliteProtectedRuntimeStore) -> Self {
        store
            .seed_legacy_bytes(
                &self.snapshot.id,
                Some(&serde_json::to_vec(&self.snapshot).unwrap()),
                Some(&serde_json::to_vec(&self.checkpoint).unwrap()),
                false,
            )
            .await
            .unwrap();
        self
    }
}
