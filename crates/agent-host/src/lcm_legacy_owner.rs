use super::*;
use crate::session_journal_conversion::WriterChecks;
use agent_runtime::core::journal::*;
use tokio::sync::OwnedRwLockWriteGuard;

pub(super) struct LegacyLcmOwnerAdmission {
    writers: WriterChecks,
    _admissions: Vec<OwnedRwLockWriteGuard<()>>,
    runtimes: BTreeSet<String>,
}

impl LegacyLcmOwnerAdmission {
    pub(super) async fn close(mut self) -> Result<(), RuntimeError> {
        self.writers.close().await
    }
}

impl SqliteProtectedRuntimeStore {
    async fn selected_chat_lcm_target(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        identity: &str,
        context: &str,
    ) -> Result<(String, String), RuntimeError> {
        let selected: Option<String> =
            sqlx::query_scalar("SELECT selected_profile_id FROM agent_identity WHERE id=?")
                .bind(identity)
                .fetch_one(&mut **tx)
                .await
                .map_err(protected_database_error)?;
        let selected = selected.ok_or_else(|| {
            RuntimeError::conflict("LCM shared chat identity has no selected profile")
        })?;
        let target: Option<String> = sqlx::query_scalar(
            "SELECT id FROM agent_session WHERE identity_id=? AND context_scope_id=? AND profile_id=?
               AND backend_kind='native' AND replaced_by_session_id IS NULL
               AND status IN ('starting','ready','running','degraded','suspended')
             ORDER BY status='suspended',created_at DESC LIMIT 1",
        ).bind(identity).bind(context).bind(&selected).fetch_optional(&mut **tx).await.map_err(protected_database_error)?;
        let target = target.ok_or_else(|| {
            RuntimeError::conflict(
                "LCM shared chat selected profile has no resumable native session",
            )
        })?;
        Ok((selected, target))
    }

    pub(crate) async fn legacy_lcm_conversion_profile(
        &self,
        runtime: &str,
        timeline: Option<&str>,
    ) -> Result<Option<String>, RuntimeError> {
        let mut tx = db::begin_immediate(self.db.pool())
            .await
            .map_err(protected_database_error)?;
        let session: Option<(String,String,String)> = sqlx::query_as(
            "SELECT s.profile_id,s.identity_id,s.context_scope_id FROM agent_session s
             JOIN agent_context_scope c ON c.id=s.context_scope_id
             WHERE s.runtime_session_id=?1 AND c.scope_type='agent_chat'
               AND (EXISTS(SELECT 1 FROM agent_lcm_timeline t
                   WHERE t.identity_id=s.identity_id AND t.scope_type='agent_chat'
                     AND t.scope_id=c.scope_id AND t.canonical_scope_id=c.scope_id AND t.retired_at IS NULL
                     AND t.runtime_session_id IS NOT NULL
                     AND (t.id=?2 OR EXISTS(SELECT 1 FROM agent_runtime_lcm_binding b
                          WHERE b.runtime_session_id=?1 AND b.timeline_id=t.id)))
                 OR EXISTS(SELECT 1 FROM agent_legacy_lcm_relinquishment r WHERE r.session_id=s.id))",
        ).bind(runtime).bind(timeline).fetch_optional(&mut *tx).await.map_err(protected_database_error)?;
        let result = if let Some((profile, identity, context)) = session {
            let (selected, _) =
                Self::selected_chat_lcm_target(&mut tx, &identity, &context).await?;
            (profile != selected).then_some(selected)
        } else {
            None
        };
        tx.commit().await.map_err(protected_database_error)?;
        Ok(result)
    }

    pub(super) async fn omit_relinquished_legacy_lcm(
        &self,
        forge_session: &str,
        snapshot: &mut SessionSnapshot,
    ) -> Result<(), RuntimeError> {
        let timelines: Vec<String> = sqlx::query_scalar(
            "SELECT timeline_id FROM agent_legacy_lcm_relinquishment WHERE session_id=?",
        )
        .bind(forge_session)
        .fetch_all(self.db.pool())
        .await
        .map_err(protected_database_error)?;
        if timelines.is_empty() {
            return Ok(());
        }
        let state = snapshot
            .extension_state
            .get(agent_runtime::harness::LCM_COMPONENT_ID);
        let relinquished = state.is_none()
            || state
                .and_then(|s| s.value.get("timeline_id"))
                .and_then(serde_json::Value::as_str)
                .is_some_and(|id| timelines.iter().any(|t| t == id));
        if relinquished {
            snapshot
                .extension_state
                .remove(agent_runtime::harness::LCM_COMPONENT_ID);
            snapshot.extension_state.remove("harness.semantic_summary");
        }
        Ok(())
    }

    pub(super) async fn prepare_legacy_chat_lcm_owner(
        &self,
        runtime: &str,
        timeline: Option<&str>,
    ) -> Result<Option<LegacyLcmOwnerAdmission>, RuntimeError> {
        let Some(timeline) = timeline else {
            return Ok(None);
        };
        let old_runtime: Option<String> = sqlx::query_scalar(
            "SELECT t.runtime_session_id FROM agent_lcm_timeline t
             JOIN agent_session s ON s.runtime_session_id=?1
             JOIN agent_context_scope c ON c.id=s.context_scope_id
             WHERE t.id=?2 AND t.identity_id=s.identity_id AND t.scope_type='agent_chat'
               AND c.scope_type=t.scope_type AND c.scope_id=t.canonical_scope_id
               AND t.runtime_session_id IS NOT NULL AND t.runtime_session_id<>?1",
        )
        .bind(runtime)
        .bind(timeline)
        .fetch_optional(self.db.pool())
        .await
        .map_err(protected_database_error)?;
        let Some(old_runtime) = old_runtime else {
            return Ok(None);
        };
        let ineligible: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_session WHERE runtime_session_id=?1 AND status IN ('starting','ready','running','degraded'))
                 OR EXISTS(SELECT 1 FROM agent_session_journal_session WHERE runtime_session_id=?1 AND (published=1 OR retired=1))",
        ).bind(&old_runtime).fetch_one(self.db.pool()).await.map_err(protected_database_error)?;
        if ineligible {
            // The transaction below reports the durable reason without opening
            // or changing a native/active owner's journal writer epoch.
            return Ok(None);
        }
        let siblings: Vec<String> = sqlx::query_scalar(
            "SELECT s.runtime_session_id FROM agent_session s
             JOIN agent_session target ON target.runtime_session_id=?
             WHERE s.identity_id=target.identity_id AND s.context_scope_id=target.context_scope_id
               AND s.id<>target.id AND s.runtime_session_id IS NOT NULL
               AND s.status NOT IN ('starting','ready','running','degraded')
               AND NOT EXISTS(SELECT 1 FROM agent_session_journal_session j
                   WHERE j.runtime_session_id=s.runtime_session_id AND (j.published=1 OR j.retired=1))
             ORDER BY s.runtime_session_id",
        ).bind(runtime).fetch_all(self.db.pool()).await.map_err(protected_database_error)?;
        let mut guard = LegacyLcmOwnerAdmission {
            writers: WriterChecks::default(),
            _admissions: Vec::new(),
            runtimes: BTreeSet::new(),
        };
        // A writer on an unpublished root has no durable pin. Engine admission
        // must precede BEGIN IMMEDIATE because opening/closing leases writes.
        for sibling in siblings {
            guard._admissions.push(
                self.conversion_admission
                    .session(&SessionId::new(&sibling))?
                    .try_write_owned()
                    .map_err(|_| {
                        RuntimeError::conflict(
                            "LCM sibling owner has foreground or conversion work",
                        )
                    })?,
            );
            for engine in [self.journals().protected(), self.journals().ordinary()] {
                let nonce = db::new_uuid_v4();
                match engine
                    .open(
                        &SessionId::new(&sibling),
                        JournalOpen {
                            root: JournalRootSelection::Latest,
                            expected_revision: None,
                            operation: JournalOperationId::new(format!(
                                "lcm-sibling-check:{nonce}"
                            )),
                            pin: JournalPinId::new(nonce),
                            intent: JournalOpenIntent::ExclusiveWriter,
                        },
                    )
                    .await
                {
                    Ok(lease) => guard.writers.0.push((engine, lease)),
                    Err(error) => {
                        guard.writers.close().await?;
                        return Err(if error.kind == ErrorKind::Conflict {
                            RuntimeError::conflict("LCM sibling owner has a journal writer")
                        } else {
                            error
                        });
                    }
                }
            }
            guard.runtimes.insert(sibling);
        }
        Ok(Some(guard))
    }

    /// Pre-topic timelines belonged to the chat. An older runtime may have
    /// opened one first after V149 even though the newer runtime last wrote it.
    pub(super) async fn transfer_legacy_chat_lcm_owner(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        runtime: &str,
        timeline: &str,
        admission: Option<&LegacyLcmOwnerAdmission>,
    ) -> Result<(), RuntimeError> {
        let row = sqlx::query(
            "SELECT t.runtime_session_id,t.scope_type,t.scope_id,t.canonical_scope_id,
                    t.identity_id,t.retired_at,t.claim_owner,t.claim_generation,t.created_at,t.updated_at
             FROM agent_lcm_timeline t WHERE t.id=?",
        )
        .bind(timeline)
        .fetch_optional(&mut **tx)
        .await
        .map_err(protected_database_error)?;
        let Some(t) = row else { return Ok(()) };
        let old_runtime: Option<String> = t
            .try_get("runtime_session_id")
            .map_err(protected_database_error)?;
        let Some(old_runtime) = old_runtime.filter(|id| id != runtime) else {
            return Ok(());
        };
        if t.get::<String, _>("scope_type") != "agent_chat" {
            return Ok(());
        }
        let sessions = sqlx::query(
            "SELECT s.id,s.runtime_session_id,s.identity_id,s.context_scope_id,s.profile_id,
                    s.backend_kind,s.status,s.created_at,s.version,s.replaced_by_session_id,
                    s.predecessor_session_id,c.scope_type,c.scope_id
             FROM agent_session s JOIN agent_context_scope c ON c.id=s.context_scope_id
             WHERE s.runtime_session_id IN (?,?)",
        )
        .bind(runtime)
        .bind(&old_runtime)
        .fetch_all(&mut **tx)
        .await
        .map_err(protected_database_error)?;
        let target: Vec<_> = sessions
            .iter()
            .filter(|s| s.get::<String, _>("runtime_session_id") == runtime)
            .collect();
        let source: Vec<_> = sessions
            .iter()
            .filter(|s| s.get::<String, _>("runtime_session_id") == old_runtime)
            .collect();
        let ([target], [source]) = (target.as_slice(), source.as_slice()) else {
            return Err(RuntimeError::conflict(
                "LCM sibling ownership has an ambiguous runtime mapping",
            ));
        };
        let identity: String = target.get("identity_id");
        let context: String = target.get("context_scope_id");
        let profile: String = target.get("profile_id");
        let scope: String = target.get("scope_id");
        let target_id: String = target.get("id");
        let source_id: String = source.get("id");
        let created: String = target.get("created_at");
        if source.get::<String, _>("identity_id") != identity
            || source.get::<String, _>("context_scope_id") != context
            || target.get::<String, _>("scope_type") != "agent_chat"
            || t.get::<String, _>("identity_id") != identity
            || t.get::<Option<String>, _>("canonical_scope_id").as_deref() != Some(&scope)
            || t.get::<String, _>("scope_id") != scope
            || t.get::<Option<String>, _>("retired_at").is_some()
        {
            return Err(RuntimeError::conflict(
                "persisted LCM timeline is not a shared pre-topic chat timeline",
            ));
        }
        if matches!(
            source.get::<String, _>("status").as_str(),
            "starting" | "ready" | "running" | "degraded"
        ) {
            return Err(RuntimeError::conflict("LCM sibling owner is active"));
        }
        if source.get::<String, _>("created_at") == created {
            return Err(RuntimeError::conflict(
                "LCM chat ownership is ambiguous: suspended sessions have the same creation time",
            ));
        }
        if source.get::<String, _>("backend_kind") != "native"
            || target.get::<String, _>("backend_kind") != "native"
            || source.get::<String, _>("status") != "suspended"
            || source.get::<String, _>("created_at") >= created
            || !matches!(
                target.get::<String, _>("status").as_str(),
                "suspended" | "starting" | "ready" | "running" | "degraded"
            )
            || [*source, *target].iter().any(|s| {
                s.get::<Option<String>, _>("replaced_by_session_id")
                    .is_some()
                    || s.get::<Option<String>, _>("predecessor_session_id")
                        .is_some()
            })
        {
            return Err(RuntimeError::conflict(
                "LCM sibling owner is not superseded by the chat resume target",
            ));
        }
        let (selected_profile, selected_target) =
            Self::selected_chat_lcm_target(tx, &identity, &context).await?;
        if profile != selected_profile {
            return Err(RuntimeError::conflict(
                "LCM shared chat must be converted by the selected profile's resume target",
            ));
        }
        if target_id != selected_target {
            return Err(RuntimeError::conflict(
                "LCM chat ownership is ambiguous: another session is resumable",
            ));
        }
        // Non-selected dormant profiles retain their own resume targets. Only
        // active siblings or a newer/tied selected-profile session compete.
        let competing: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_session s
             WHERE s.identity_id=? AND s.context_scope_id=? AND s.id<>?
               AND (s.status IN ('starting','ready','running','degraded')
                 OR (s.backend_kind='native' AND s.status='suspended' AND s.replaced_by_session_id IS NULL
                     AND s.profile_id=? AND s.created_at>=?)))",
        ).bind(&identity).bind(&context).bind(&target_id).bind(&profile).bind(&created)
            .fetch_one(&mut **tx).await.map_err(protected_database_error)?;
        if competing {
            return Err(RuntimeError::conflict(
                "LCM chat ownership is ambiguous: another session is resumable",
            ));
        }
        let topic: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_chat_topic WHERE chat_id=?1)
                 OR EXISTS(SELECT 1 FROM agent_chat_topic_rotation WHERE chat_id=?1)",
        )
        .bind(&scope)
        .fetch_one(&mut **tx)
        .await
        .map_err(protected_database_error)?;
        if topic {
            return Err(RuntimeError::conflict(
                "LCM shared chat has a topic or pending rotation reference",
            ));
        }
        if t.get::<Option<String>, _>("claim_owner").is_some()
            || t.get::<i64, _>("claim_generation") != 0
        {
            return Err(RuntimeError::conflict(
                "LCM shared timeline has a runtime claim",
            ));
        }
        if t.get::<String, _>("created_at") >= created || t.get::<String, _>("updated_at") < created
        {
            return Err(RuntimeError::conflict(
                "LCM timeline does not establish pre-topic continuity with the chat resume target",
            ));
        }
        let native: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_session_journal_session
             WHERE runtime_session_id IN (?,?) AND (published=1 OR retired=1))",
        )
        .bind(&old_runtime)
        .bind(runtime)
        .fetch_one(&mut **tx)
        .await
        .map_err(protected_database_error)?;
        if native {
            return Err(RuntimeError::conflict(
                "LCM sibling ownership cannot move after native journal publication or retirement",
            ));
        }
        let pinned: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_session_journal_pin WHERE runtime_session_id=?)",
        )
        .bind(&old_runtime)
        .fetch_one(&mut **tx)
        .await
        .map_err(protected_database_error)?;
        if pinned {
            return Err(RuntimeError::conflict(
                "LCM sibling owner has a journal lease",
            ));
        }
        let other_binding: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_runtime_lcm_binding
             WHERE timeline_id=? AND runtime_session_id NOT IN (?,?))",
        )
        .bind(timeline)
        .bind(&old_runtime)
        .bind(runtime)
        .fetch_one(&mut **tx)
        .await
        .map_err(protected_database_error)?;
        if other_binding {
            return Err(RuntimeError::conflict(
                "LCM shared timeline has an additional runtime binding",
            ));
        }
        let other_source_binding: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_runtime_lcm_binding WHERE runtime_session_id=? AND timeline_id<>?)",
        ).bind(&old_runtime).bind(timeline).fetch_one(&mut **tx).await.map_err(protected_database_error)?;
        if other_source_binding {
            return Err(RuntimeError::conflict(
                "LCM sibling owner is bound to a different timeline",
            ));
        }
        let mut relinquishing = self
            .legacy_lcm_references(tx, &identity, &context, &target_id, timeline)
            .await?;
        if !relinquishing
            .iter()
            .any(|s| s.get::<String, _>("id") == source_id)
        {
            relinquishing.push(
                sqlx::query("SELECT * FROM agent_session WHERE id=?")
                    .bind(&source_id)
                    .fetch_one(&mut **tx)
                    .await
                    .map_err(protected_database_error)?,
            );
        }
        if admission.is_none_or(|guard| !guard.runtimes.contains(&old_runtime)) {
            return Err(RuntimeError::conflict(
                "LCM sibling ownership changed before writer admission",
            )
            .retryable());
        }
        sqlx::query(
            "DELETE FROM agent_runtime_lcm_binding WHERE runtime_session_id=? AND timeline_id=?",
        )
        .bind(&old_runtime)
        .bind(timeline)
        .execute(&mut **tx)
        .await
        .map_err(protected_database_error)?;
        sqlx::query("UPDATE agent_lcm_timeline SET runtime_session_id=? WHERE id=? AND runtime_session_id=?")
            .bind(runtime).bind(timeline).bind(&old_runtime).execute(&mut **tx).await.map_err(protected_database_error)?;
        for sibling in relinquishing {
            let sibling_runtime: Option<String> = sibling.get("runtime_session_id");
            let sibling_runtime = sibling_runtime.ok_or_else(|| {
                RuntimeError::conflict("LCM referenced sibling has no runtime mapping")
            })?;
            if sibling.get::<String, _>("status") != "suspended"
                || sibling.get::<String, _>("backend_kind") != "native"
                || sibling.get::<String, _>("created_at") >= created
                || sibling
                    .get::<Option<String>, _>("predecessor_session_id")
                    .is_some()
                || sibling
                    .get::<Option<String>, _>("replaced_by_session_id")
                    .is_some()
                || admission.is_none_or(|guard| !guard.runtimes.contains(&sibling_runtime))
            {
                return Err(RuntimeError::conflict(
                    "LCM referenced sibling is not an inactive legacy predecessor",
                ));
            }
            let pinned: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM agent_session_journal_pin WHERE runtime_session_id=?)",
            )
            .bind(&sibling_runtime)
            .fetch_one(&mut **tx)
            .await
            .map_err(protected_database_error)?;
            if pinned {
                return Err(RuntimeError::conflict(
                    "LCM sibling owner has a journal lease",
                ));
            }
            let bound_elsewhere: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_runtime_lcm_binding WHERE runtime_session_id=? AND timeline_id<>?)")
                .bind(&sibling_runtime).bind(timeline).fetch_one(&mut **tx).await.map_err(protected_database_error)?;
            if bound_elsewhere {
                return Err(RuntimeError::conflict(
                    "LCM sibling owner is bound to a different timeline",
                ));
            }
            let id: String = sibling.get("id");
            let version: i64 = sibling.get("version");
            let recorded = sqlx::query("INSERT INTO agent_legacy_lcm_relinquishment(session_id,timeline_id,transferred_to_runtime_session_id,created_at) SELECT id,?,?,? FROM agent_session WHERE id=? AND version=?")
                .bind(timeline).bind(runtime).bind(db::now_rfc3339()).bind(&id).bind(version)
                .execute(&mut **tx).await.map_err(protected_database_error)?;
            if recorded.rows_affected() != 1 {
                return Err(RuntimeError::conflict(
                    "LCM sibling owner changed during transfer",
                ));
            }
            if sibling.get::<String, _>("profile_id") == profile {
                let changed = sqlx::query("UPDATE agent_session SET status='replaced',replaced_by_session_id=?,version=version+1,updated_at=? WHERE id=? AND version=?")
                    .bind(&target_id).bind(db::now_rfc3339()).bind(&id).bind(version)
                    .execute(&mut **tx).await.map_err(protected_database_error)?;
                if changed.rows_affected() != 1 {
                    return Err(RuntimeError::conflict(
                        "LCM sibling owner changed during transfer",
                    ));
                }
            }
        }
        Ok(())
    }

    async fn legacy_lcm_references(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        identity: &str,
        context: &str,
        target: &str,
        timeline: &str,
    ) -> Result<Vec<sqlx::sqlite::SqliteRow>, RuntimeError> {
        let siblings = sqlx::query(
            "SELECT * FROM agent_session WHERE identity_id=? AND context_scope_id=? AND id<>?",
        )
        .bind(identity)
        .bind(context)
        .bind(target)
        .fetch_all(&mut **tx)
        .await
        .map_err(protected_database_error)?;
        let mut references = Vec::new();
        for sibling in siblings {
            let id: String = sibling.get("id");
            let native: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM agent_session_journal_session WHERE forge_session_id=? AND (published=1 OR retired=1))",
            ).bind(&id).fetch_one(&mut **tx).await.map_err(protected_database_error)?;
            if native {
                return Err(RuntimeError::conflict(
                    "LCM shared chat has another sibling with native journal state",
                ));
            }
            let row = sqlx::query("SELECT snapshot_ciphertext,snapshot_nonce,checkpoint_ciphertext,checkpoint_nonce FROM protected_agent_session_state WHERE session_id=?")
                .bind(&id).fetch_optional(&mut **tx).await.map_err(protected_database_error)?;
            let Some(row) = row else { continue };
            let mut referenced = false;
            for (cipher, nonce, checkpoint) in [
                ("snapshot_ciphertext", "snapshot_nonce", false),
                ("checkpoint_ciphertext", "checkpoint_nonce", true),
            ] {
                let ciphertext: Option<Vec<u8>> = row.get(cipher);
                let nonce: Option<Vec<u8>> = row.get(nonce);
                let snapshot = match (ciphertext, nonce) {
                    (None, None) => continue,
                    (Some(ciphertext), Some(nonce)) => {
                        let bytes = self.open_session_state(ciphertext, nonce).await?;
                        if checkpoint {
                            serde_json::from_slice::<TurnCheckpoint>(&bytes)?.snapshot
                        } else {
                            serde_json::from_slice::<SessionSnapshot>(&bytes)?
                        }
                    }
                    _ => {
                        return Err(RuntimeError::conflict(
                            "LCM sibling has incomplete legacy state; exclusive ownership is unknown",
                        ));
                    }
                };
                if snapshot
                    .extension_state
                    .get(agent_runtime::harness::LCM_COMPONENT_ID)
                    .and_then(|state| state.value.get("timeline_id"))
                    .and_then(serde_json::Value::as_str)
                    == Some(timeline)
                {
                    referenced = true;
                }
            }
            if referenced {
                references.push(sibling);
            }
        }
        Ok(references)
    }
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use super::*;
    use crate::test_support::{FileDb, LegacyLcmFixture};

    async fn ownership(
        f: &FileDb,
    ) -> (
        Vec<(String, String)>,
        Vec<(Option<String>, String, Option<String>, i64)>,
        String,
        Vec<(String, String, String)>,
    ) {
        let db = f.database();
        let bindings = sqlx::query_as("SELECT runtime_session_id,timeline_id FROM agent_runtime_lcm_binding ORDER BY runtime_session_id")
            .fetch_all(db.pool()).await.unwrap();
        let sessions = sqlx::query_as("SELECT runtime_session_id,status,replaced_by_session_id,version FROM agent_session ORDER BY id")
            .fetch_all(db.pool()).await.unwrap();
        let owner = sqlx::query_scalar(
            "SELECT runtime_session_id FROM agent_lcm_timeline WHERE scope_id=canonical_scope_id",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        let receipts = sqlx::query_as("SELECT session_id,timeline_id,transferred_to_runtime_session_id FROM agent_legacy_lcm_relinquishment ORDER BY session_id,timeline_id")
            .fetch_all(db.pool()).await.unwrap();
        (bindings, sessions, owner, receipts)
    }

    #[tokio::test]
    async fn sibling_owner_transfer_rejects_ambiguous_or_used_timelines() {
        for (case, expected) in [
            ("active", "LCM sibling owner is active"),
            (
                "no-selected-profile",
                "LCM shared chat identity has no selected profile",
            ),
            (
                "selected-profile-without-session",
                "LCM shared chat selected profile has no resumable native session",
            ),
            (
                "tie",
                "LCM chat ownership is ambiguous: suspended sessions have the same creation time",
            ),
            (
                "newer",
                "LCM chat ownership is ambiguous: another session is resumable",
            ),
            (
                "binding",
                "LCM shared timeline has an additional runtime binding",
            ),
            (
                "ineligible-legacy-reference",
                "LCM referenced sibling is not an inactive legacy predecessor",
            ),
            (
                "unmapped-legacy-reference",
                "LCM referenced sibling has no runtime mapping",
            ),
            (
                "bound-legacy-reference",
                "LCM sibling owner is bound to a different timeline",
            ),
            (
                "native",
                "LCM sibling ownership cannot move after native journal publication or retirement",
            ),
            ("claim", "LCM shared timeline has a runtime claim"),
            (
                "rotation",
                "LCM shared chat has a topic or pending rotation reference",
            ),
            (
                "foreground",
                "LCM sibling owner has foreground or conversion work",
            ),
            ("lease", "LCM sibling owner has a journal writer"),
        ] {
            let f = FileDb::new().await.unwrap();
            let db = f.database();
            let store = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1));
            let id = SessionId::new(format!("ambiguous-{case}"));
            let (old, older) = LegacyLcmFixture::shared_pre_topic_chat(&f, &store, &id).await;
            let mut foreground = None;
            let mut lease = None;
            match case {
                "active" => {
                    sqlx::query(
                        "UPDATE agent_session SET status='ready' WHERE runtime_session_id=?",
                    )
                    .bind(older.as_str())
                    .execute(db.pool())
                    .await
                    .unwrap();
                }
                "no-selected-profile" => {
                    sqlx::query("UPDATE agent_identity SET selected_profile_id=NULL WHERE id='journal-agent'")
                        .execute(db.pool()).await.unwrap();
                }
                "selected-profile-without-session" => {
                    sqlx::query("INSERT INTO agent_profile(id,identity_id,backend_kind,executor_type,created_at,updated_at) VALUES ('other-profile','journal-agent','native','embedded','c','u')")
                        .execute(db.pool()).await.unwrap();
                    sqlx::query("UPDATE agent_identity SET selected_profile_id='other-profile' WHERE id='journal-agent'")
                        .execute(db.pool()).await.unwrap();
                }
                "tie" => {
                    sqlx::query("UPDATE agent_session SET created_at='2026-09-24T08:34:00Z' WHERE runtime_session_id=?")
                        .bind(older.as_str()).execute(db.pool()).await.unwrap();
                }
                "newer" => {
                    f.ensure_session(&SessionId::new("newest-sibling"))
                        .await
                        .unwrap();
                }
                "binding" => {
                    sqlx::query("INSERT INTO agent_runtime_lcm_binding(runtime_session_id,timeline_id) VALUES ('unrelated-runtime',?)")
                        .bind(&old.timeline_id).execute(db.pool()).await.unwrap();
                }
                "ineligible-legacy-reference"
                | "unmapped-legacy-reference"
                | "bound-legacy-reference" => {
                    let third = SessionId::new("third-legacy-sibling");
                    f.ensure_session(&third).await.unwrap();
                    sqlx::query("UPDATE agent_session SET created_at='2026-09-22T00:00:00Z',status='failed' WHERE runtime_session_id=?")
                        .bind(third.as_str()).execute(db.pool()).await.unwrap();
                    let mut shared = old.checkpoint.snapshot.clone();
                    shared.id = third.clone();
                    store
                        .seed_legacy_bytes(
                            &third,
                            Some(&serde_json::to_vec(&shared).unwrap()),
                            None,
                            false,
                        )
                        .await
                        .unwrap();
                    if case == "unmapped-legacy-reference" {
                        sqlx::query("UPDATE agent_session SET status='suspended',runtime_session_id=NULL WHERE runtime_session_id=?")
                            .bind(third.as_str()).execute(db.pool()).await.unwrap();
                    }
                    if case == "bound-legacy-reference" {
                        sqlx::query("UPDATE agent_session SET status='suspended' WHERE runtime_session_id=?")
                            .bind(third.as_str()).execute(db.pool()).await.unwrap();
                        sqlx::query("INSERT INTO agent_lcm_timeline(id,identity_id,scope_type,scope_id,canonical_scope_id,runtime_session_id,authorization_revision,revision,created_at,updated_at) SELECT 'other-sibling-timeline',identity_id,scope_type,scope_id || '#topic:third',canonical_scope_id,?,authorization_revision,0,created_at,updated_at FROM agent_lcm_timeline WHERE id=?")
                            .bind(third.as_str()).bind(&old.timeline_id).execute(db.pool()).await.unwrap();
                        sqlx::query("INSERT INTO agent_runtime_lcm_binding(runtime_session_id,timeline_id) VALUES (?,'other-sibling-timeline')")
                            .bind(third.as_str()).execute(db.pool()).await.unwrap();
                    }
                }
                "native" => {
                    let mut snapshot = old.checkpoint.snapshot.clone();
                    snapshot.id = older.clone();
                    store
                        .seed_legacy_bytes(
                            &older,
                            Some(&serde_json::to_vec(&snapshot).unwrap()),
                            None,
                            false,
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        store
                            .convert_legacy_session(
                                &f.ensure_session(&older).await.unwrap(),
                                &older
                            )
                            .await
                            .unwrap(),
                        crate::session_journal_conversion::ConversionOutcome::Converted
                    );
                }
                "claim" => {
                    sqlx::query(
                        "UPDATE agent_lcm_timeline SET claim_owner=?,claim_generation=1 WHERE id=?",
                    )
                    .bind(older.as_str())
                    .bind(&old.timeline_id)
                    .execute(db.pool())
                    .await
                    .unwrap();
                }
                "rotation" => {
                    sqlx::query("INSERT INTO agent_chat_topic_rotation(chat_id,id,label,cause,created_at,source_session_id) VALUES (?,'pending-rotation','Pending','idle','c',?)")
                        .bind(format!("chat:{id}")).bind(f.ensure_session(&older).await.unwrap()).execute(db.pool()).await.unwrap();
                }
                "foreground" => {
                    foreground = Some(store.foreground_session(&older).await.unwrap());
                }
                "lease" => {
                    let engine = store.journals().ordinary();
                    let opened = engine
                        .open(
                            &older,
                            JournalOpen {
                                root: JournalRootSelection::Latest,
                                expected_revision: None,
                                operation: JournalOperationId::new("sibling-writer"),
                                pin: JournalPinId::new("sibling-writer"),
                                intent: JournalOpenIntent::ExclusiveWriter,
                            },
                        )
                        .await
                        .unwrap();
                    lease = Some((engine, opened));
                }
                _ => unreachable!(),
            }
            let before = ownership(&f).await;
            let error = store
                .resolve_lcm_store(id.as_str(), "agent_chat", &format!("chat:{id}"), false)
                .await
                .err()
                .unwrap_or_else(|| panic!("{case} unexpectedly transferred ownership"));
            assert_eq!(error.kind, ErrorKind::Conflict, "{case}: {error}");
            assert_eq!(error.message, expected, "{case}");
            assert_eq!(ownership(&f).await, before, "{case} changed ownership");
            let published: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_session_journal_session WHERE runtime_session_id=? AND published=1)")
                .bind(id.as_str()).fetch_one(db.pool()).await.unwrap();
            assert!(!published, "{case} published B");
            if let Some((engine, opened)) = lease {
                engine.close(opened).await.unwrap();
            }
            drop(foreground);
        }
    }

    #[tokio::test]
    async fn sibling_owner_transfer_rolls_back_all_labels_and_retries() {
        let f = FileDb::new().await.unwrap();
        let db = f.database();
        let store = SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1);
        let id = SessionId::new("rollback-sibling");
        let (old, _) = LegacyLcmFixture::shared_pre_topic_chat(&f, &store, &id).await;
        let before = ownership(&f).await;
        sqlx::query("CREATE TRIGGER reject_sibling_binding BEFORE INSERT ON agent_runtime_lcm_binding WHEN NEW.runtime_session_id='rollback-sibling' BEGIN SELECT RAISE(ABORT,'injected binding failure'); END")
            .execute(db.pool()).await.unwrap();
        let result = store
            .resolve_lcm_store(id.as_str(), "agent_chat", &format!("chat:{id}"), false)
            .await;
        assert!(result.is_err());
        assert_eq!(ownership(&f).await, before);
        sqlx::query("DROP TRIGGER reject_sibling_binding")
            .execute(db.pool())
            .await
            .unwrap();
        let lcm = store
            .resolve_lcm_store(id.as_str(), "agent_chat", &format!("chat:{id}"), false)
            .await
            .unwrap();
        assert_eq!(lcm.timeline_id(), old.timeline_id);
        let after = ownership(&f).await;
        let retry = store
            .resolve_lcm_store(id.as_str(), "agent_chat", &format!("chat:{id}"), false)
            .await
            .unwrap();
        assert_eq!(retry.timeline_id(), old.timeline_id);
        assert_eq!(ownership(&f).await, after);
    }

    #[tokio::test]
    async fn profile_switch_transfer_rolls_back_relinquishments_and_retries() {
        let f = FileDb::new().await.unwrap();
        let db = f.database();
        let store = SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1);
        let id = SessionId::new("rollback-profiles");
        let (old, _) = LegacyLcmFixture::shared_pre_topic_profile_switch(&f, &store, &id).await;
        let before = ownership(&f).await;
        sqlx::query("CREATE TRIGGER reject_profile_binding BEFORE INSERT ON agent_runtime_lcm_binding WHEN NEW.runtime_session_id='rollback-profiles' BEGIN SELECT RAISE(ABORT,'injected profile binding failure'); END")
            .execute(db.pool()).await.unwrap();
        let failed = store
            .resolve_lcm_store(id.as_str(), "agent_chat", &format!("chat:{id}"), false)
            .await;
        assert!(failed.is_err());
        assert_eq!(ownership(&f).await, before);
        sqlx::query("DROP TRIGGER reject_profile_binding")
            .execute(db.pool())
            .await
            .unwrap();
        let lcm = store
            .resolve_lcm_store(id.as_str(), "agent_chat", &format!("chat:{id}"), false)
            .await
            .unwrap();
        assert_eq!(lcm.timeline_id(), old.timeline_id);
        let after = ownership(&f).await;
        assert_eq!(after.3.len(), 12);
        assert_eq!(
            store
                .resolve_lcm_store(id.as_str(), "agent_chat", &format!("chat:{id}"), false)
                .await
                .unwrap()
                .timeline_id(),
            old.timeline_id
        );
        assert_eq!(ownership(&f).await, after);
    }

    #[tokio::test]
    async fn profile_switch_transfer_fences_other_referencing_siblings() {
        for foreground in [false, true] {
            let f = FileDb::new().await.unwrap();
            let store = Arc::new(SqliteProtectedRuntimeStore::new(f.database(), [7; 32], 1));
            let id = SessionId::new("sibling-profile-writer");
            LegacyLcmFixture::shared_pre_topic_profile_switch(&f, &store, &id).await;
            let sibling = SessionId::new(format!("{id}:glm:0"));
            let before = ownership(&f).await;
            let held = if foreground {
                Some(store.foreground_session(&sibling).await.unwrap())
            } else {
                None
            };
            let engine = store.journals().ordinary();
            let lease = if foreground {
                None
            } else {
                Some(
                    engine
                        .open(
                            &sibling,
                            JournalOpen {
                                root: JournalRootSelection::Latest,
                                expected_revision: None,
                                operation: JournalOperationId::new("other-sibling-writer"),
                                pin: JournalPinId::new("other-sibling-writer"),
                                intent: JournalOpenIntent::ExclusiveWriter,
                            },
                        )
                        .await
                        .unwrap(),
                )
            };
            let error = store
                .resolve_lcm_store(id.as_str(), "agent_chat", &format!("chat:{id}"), false)
                .await
                .err()
                .unwrap();
            assert_eq!(error.kind, ErrorKind::Conflict);
            assert_eq!(
                error.message,
                if foreground {
                    "LCM sibling owner has foreground or conversion work"
                } else {
                    "LCM sibling owner has a journal writer"
                }
            );
            assert_eq!(ownership(&f).await, before);
            if let Some(lease) = lease {
                engine.close(lease).await.unwrap();
            }
            drop(held);
        }
    }

    #[tokio::test]
    async fn relinquishment_does_not_discard_unrecorded_binding_mismatches() {
        let f = FileDb::new().await.unwrap();
        let db = f.database();
        let store = Arc::new(SqliteProtectedRuntimeStore::new(db.clone(), [7; 32], 1));
        let id = SessionId::new("recorded-lcm-only");
        let (old, older) = LegacyLcmFixture::shared_pre_topic_profile_switch(&f, &store, &id).await;
        let mut shared = SessionStore::load(&*store, &older).await.unwrap().unwrap();
        shared.extension_state.insert(
            "harness.semantic_summary".into(),
            agent_runtime::core::store::VersionedSessionState::new(
                agent_runtime::registry::RegistryRevision::from_content("shared-summary"),
                serde_json::json!({"derived":"shared timeline"}),
            ),
        );
        let checkpoint = TurnCheckpoint::session_boundary(shared.clone(), 6).unwrap();
        store
            .seed_legacy_bytes(
                &older,
                Some(&serde_json::to_vec(&shared).unwrap()),
                Some(&serde_json::to_vec(&checkpoint).unwrap()),
                false,
            )
            .await
            .unwrap();
        store
            .resolve_lcm_store(id.as_str(), "agent_chat", &format!("chat:{id}"), false)
            .await
            .unwrap();
        shared
            .extension_state
            .remove(agent_runtime::harness::LCM_COMPONENT_ID);
        shared.extension_state.remove("harness.semantic_summary");
        assert_eq!(
            SessionStore::load(&*store, &older).await.unwrap().unwrap(),
            shared
        );
        assert_eq!(
            CheckpointStore::load_latest(&*store, &older)
                .await
                .unwrap()
                .unwrap()
                .snapshot,
            shared
        );
        sqlx::query(
            "UPDATE agent_identity SET selected_profile_id='glm-profile' WHERE id='journal-agent'",
        )
        .execute(db.pool())
        .await
        .unwrap();
        let mut unrelated = old.checkpoint.snapshot;
        unrelated.id = older.clone();
        unrelated
            .extension_state
            .get_mut(agent_runtime::harness::LCM_COMPONENT_ID)
            .unwrap()
            .value["timeline_id"] = serde_json::json!("unrecorded-timeline");
        unrelated.extension_state.insert(
            "harness.semantic_summary".into(),
            agent_runtime::core::store::VersionedSessionState::new(
                agent_runtime::registry::RegistryRevision::from_content("unrecorded-summary"),
                serde_json::json!({"retain":"not the relinquished timeline"}),
            ),
        );
        store
            .seed_legacy_bytes(
                &older,
                Some(&serde_json::to_vec(&unrelated).unwrap()),
                None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(
            SessionStore::load(&*store, &older).await.unwrap().unwrap(),
            unrelated
        );
        let owner = f.ensure_session(&older).await.unwrap();
        let error = store
            .convert_legacy_session(&owner, &older)
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Conflict);
        assert_eq!(
            error.message,
            "persisted LCM timeline is unavailable or belongs to another session"
        );
        let published:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_session_journal_session WHERE runtime_session_id=? AND published=1)")
            .bind(older.as_str()).fetch_one(db.pool()).await.unwrap();
        assert!(!published);
    }
}
