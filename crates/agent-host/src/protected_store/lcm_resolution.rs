//! Runtime LCM timeline ownership, admission, and resolution.

use super::*;

impl SqliteProtectedRuntimeStore {
    /// Resolves a replaceable runtime session to the stable identity/scope
    /// LCM timeline. The runtime id and canonical scope must both match the
    /// persisted Forge session; a timeline id alone cannot be used to open
    /// the store.
    ///
    /// A session first resolves to its own durable binding. Without one it
    /// may continue a timeline it already owns (stamped with its runtime id),
    /// or adopt the one pre-V149 owner-less timeline its *own* persisted LCM
    /// state references; that adoption stamps the owner in the same
    /// transaction, so no other runtime session can resolve to it afterwards.
    /// An empty, unclaimed non-Chat timeline may continue across runtimes.
    /// Without a persisted reference, other sessions get a fresh timeline;
    /// A shared pre-topic Chat timeline may move from an inactive legacy sibling
    /// to the selected Profile's Chat resume target. Ownership, binding,
    /// relinquishment and same-Profile replacement commit together; other
    /// Profiles retain their legacy sessions and rebuild relinquished LCM state.
    pub async fn lcm_store_for_runtime_session(
        &self,
        runtime_id: &str,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<crate::SqliteLcmStore, RuntimeError> {
        self.resolve_lcm_store(runtime_id, scope_type, scope_id, false)
            .await
    }

    /// Resolves a topic-rotation successor. A successor never adopts or
    /// shares an existing timeline: it is always bound to a fresh, empty one
    /// (replays reuse that same binding).
    pub async fn lcm_store_for_successor_runtime_session(
        &self,
        runtime_id: &str,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<crate::SqliteLcmStore, RuntimeError> {
        self.resolve_lcm_store(runtime_id, scope_type, scope_id, true)
            .await
    }

    /// The LCM timeline the runtime session's own persisted state is bound
    /// to, if it has any. Only this timeline may be adopted while owner-less.
    pub(super) async fn referenced_lcm_timeline(
        &self,
        runtime_id: &str,
        flags: db::SessionPersistenceFlags,
    ) -> Result<Option<String>, RuntimeError> {
        let id = SessionId::new(runtime_id);
        let mut state = self
            .persisted_extension(&id, flags, agent_runtime::harness::LCM_COMPONENT_ID)
            .await?;
        // Pre-topic stores wrote the two copies independently. Resume overlays
        // checkpoint-only namespaces, so timeline discovery must see them too.
        if state.is_none() && flags.legacy_eligible && flags.checkpoint_present {
            state = CheckpointStore::load_latest(self, &id)
                .await?
                .and_then(|mut checkpoint| {
                    checkpoint
                        .snapshot
                        .extension_state
                        .remove(agent_runtime::harness::LCM_COMPONENT_ID)
                });
        }
        Ok(state.and_then(|state| {
            state
                .value
                .get("timeline_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        }))
    }

    pub(crate) async fn resolve_lcm_store(
        &self,
        runtime_id: &str,
        scope_type: &str,
        scope_id: &str,
        successor: bool,
    ) -> Result<crate::SqliteLcmStore, RuntimeError> {
        self.resolve_lcm_store_admitted(
            runtime_id,
            scope_type,
            scope_id,
            successor,
            LcmResolveAdmission::Foreground,
        )
        .await
    }

    pub(crate) async fn resolve_lcm_store_admitted(
        &self,
        runtime_id: &str,
        scope_type: &str,
        scope_id: &str,
        successor: bool,
        admission: LcmResolveAdmission<'_>,
    ) -> Result<crate::SqliteLcmStore, RuntimeError> {
        let (admitted_runtime, expected_legacy_timeline, foreground) = match admission {
            LcmResolveAdmission::Foreground => (runtime_id, None, true),
            LcmResolveAdmission::Conversion { runtime, timeline } => (runtime, timeline, false),
        };
        let row = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0);
            sqlx::query(
                "SELECT session.identity_id, session.profile_id, scope.scope_type, scope.scope_id
             FROM agent_session AS session
             JOIN agent_context_scope AS scope
               ON scope.id = session.context_scope_id
             WHERE session.runtime_session_id = ?
               AND scope.scope_type = ? AND scope.scope_id = ?
             LIMIT 1",
            )
            .bind(runtime_id)
            .bind(scope_type)
            .bind(scope_id)
            .fetch_optional(self.db.pool())
            .await
        }
        .map_err(protected_database_error)?
        .ok_or_else(|| RuntimeError::not_found("LCM session binding not found"))?;
        let identity_id: String = row
            .try_get("identity_id")
            .map_err(protected_database_error)?;
        let stored_scope_type: String = row
            .try_get("scope_type")
            .map_err(protected_database_error)?;
        let stored_scope_id: String = row.try_get("scope_id").map_err(protected_database_error)?;
        let authorization_revision =
            agent_runtime::registry::RegistryRevision::from_content(format!(
                "forge-lcm-authorization-v1\n{identity_id}\n{stored_scope_type}\n{stored_scope_id}"
            ));
        let (has_binding, owned_shared_timeline): (bool, Option<String>) = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0);
            sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM agent_runtime_lcm_binding WHERE runtime_session_id=?1),
                    (SELECT t.id FROM agent_runtime_lcm_binding b JOIN agent_lcm_timeline t ON t.id=b.timeline_id
                        WHERE b.runtime_session_id=?1 AND t.runtime_session_id=?1 AND t.scope_type='agent_chat'
                          AND t.scope_id=t.canonical_scope_id AND t.retired_at IS NULL)",
        )
        .bind(runtime_id)
        .fetch_one(self.db.pool())
        .await
        }
        .map_err(protected_database_error)?;
        let owns_shared_timeline = owned_shared_timeline.is_some();
        let receipts_complete = if let Some(timeline) = owned_shared_timeline.as_deref() {
            let mut connection = self
                .db
                .pool()
                .acquire()
                .await
                .map_err(protected_database_error)?;
            let stamp =
                Self::legacy_lcm_receipt_stamp(&mut connection, runtime_id, timeline).await?;
            self.legacy_lcm_receipt_stamps
                .lock()
                .ok()
                .is_some_and(|cached| cached.get(runtime_id) == Some(&stamp))
        } else {
            false
        };
        // Reading native/legacy extension state happens outside the write transaction.
        let flags = self.persistence_flags(&SessionId::new(runtime_id)).await?;
        let referenced = if successor
            || (has_binding
                && !flags.legacy_eligible
                && expected_legacy_timeline.is_none()
                && (!owns_shared_timeline || receipts_complete))
        {
            None
        } else {
            self.referenced_lcm_timeline(runtime_id, flags).await?
        };
        if expected_legacy_timeline.is_some_and(|id| referenced.as_deref() != Some(id)) {
            return Err(RuntimeError::conflict(
                "LCM selected chat resume target does not reference the shared timeline",
            ));
        }
        let mut arbitration_deferred = None;
        let needs_arbitration = !receipts_complete
            && (flags.legacy_eligible
                || expected_legacy_timeline.is_some()
                || owns_shared_timeline);
        let sibling_admission = if needs_arbitration {
            let mut prepared = self
                .prepare_legacy_chat_lcm_owner(runtime_id, referenced.as_deref(), admitted_runtime)
                .await;
            if foreground && prepared.as_ref().is_err_and(|e| e.retryable) {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                prepared = self
                    .prepare_legacy_chat_lcm_owner(
                        runtime_id,
                        referenced.as_deref(),
                        admitted_runtime,
                    )
                    .await;
            }
            match prepared {
                Ok(admission) => admission,
                Err(error) if foreground => {
                    arbitration_deferred = Some(error);
                    None
                }
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        let mut tx = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN_IMMEDIATE", 0);
            db::begin_immediate(self.db.pool()).await
        }
        .map_err(protected_database_error)?;
        let existing: Option<String> = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0);
            sqlx::query_scalar(
                "SELECT timeline_id FROM agent_runtime_lcm_binding WHERE runtime_session_id = ?",
            )
            .bind(runtime_id)
            .fetch_optional(&mut *tx)
            .await
        }
        .map_err(protected_database_error)?;
        // An interrupted old resolver could bind an empty replacement before
        // import rejected its timeline mismatch. Only unpublished legacy state
        // may repair that binding, and only while the replacement is untouched.
        let repair_empty = if flags.legacy_eligible
            && referenced.is_some()
            && existing
                .as_ref()
                .is_some_and(|id| Some(id) != referenced.as_ref())
        {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM agent_lcm_timeline t
                 WHERE t.id=? AND t.runtime_session_id=? AND t.retired_at IS NULL
                   AND t.identity_id=? AND t.scope_type=? AND t.canonical_scope_id=?
                   AND t.revision=0 AND t.claim_owner IS NULL AND t.claim_generation=0
                   AND NOT EXISTS(SELECT 1 FROM agent_session_journal_session j
                                  WHERE j.runtime_session_id=t.runtime_session_id
                                    AND (j.published=1 OR j.retired=1))
                   AND NOT EXISTS(SELECT 1 FROM agent_lcm_entry e WHERE e.timeline_id=t.id)
                   AND NOT EXISTS(SELECT 1 FROM agent_lcm_node n WHERE n.timeline_id=t.id)
                   AND NOT EXISTS(SELECT 1 FROM agent_lcm_operation o WHERE o.timeline_id=t.id))",
            )
            .bind(existing.as_deref())
            .bind(runtime_id)
            .bind(&identity_id)
            .bind(&stored_scope_type)
            .bind(&stored_scope_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(protected_database_error)?
        } else {
            false
        };
        if !repair_empty
            && referenced
                .as_deref()
                .is_some_and(|referenced| existing.as_deref().is_some_and(|id| id != referenced))
        {
            return Err(RuntimeError::conflict(
                "LCM selected chat resume target is bound to a different timeline",
            ));
        }
        let mut receipts_completed = false;
        if needs_arbitration
            && (existing.is_none()
                || repair_empty
                || (existing.as_ref() == referenced.as_ref()
                    && (sibling_admission.is_some() || (!foreground && owns_shared_timeline))))
        {
            if let Some(timeline) = referenced.as_deref() {
                let fallback: bool = foreground && sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM agent_lcm_timeline WHERE id=? AND (runtime_session_id IS NULL OR runtime_session_id=?))",
                ).bind(timeline).bind(runtime_id).fetch_one(&mut *tx).await.map_err(protected_database_error)?;
                if !fallback {
                    if let Some(error) = arbitration_deferred.take() {
                        return Err(error);
                    }
                }
                if arbitration_deferred.is_none() {
                    sqlx::query("SAVEPOINT legacy_lcm_arbitration")
                        .execute(&mut *tx)
                        .await
                        .map_err(protected_database_error)?;
                    let transfer = self
                        .transfer_legacy_chat_lcm_owner(
                            &mut tx,
                            runtime_id,
                            timeline,
                            sibling_admission.as_ref(),
                            foreground,
                        )
                        .await;
                    match transfer {
                        Ok(complete) => receipts_completed = complete,
                        Err(_) if fallback => {
                            // Forge already selected this foreground session. Restore the
                            // old adoption path if arbitration cannot prove exclusive receipts.
                            sqlx::query("ROLLBACK TO legacy_lcm_arbitration")
                                .execute(&mut *tx)
                                .await
                                .map_err(protected_database_error)?;
                        }
                        Err(error) => return Err(error),
                    }
                    sqlx::query("RELEASE legacy_lcm_arbitration")
                        .execute(&mut *tx)
                        .await
                        .map_err(protected_database_error)?;
                }
            }
        }
        let timeline_id = if let Some(id) = existing.filter(|_| !repair_empty) {
            id
        } else {
            let candidate: Option<String> = if successor {
                None
            } else {
                {
                    #[cfg(feature = "test-support")]
                    let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0);
                    sqlx::query_scalar(
                        "SELECT id FROM agent_lcm_timeline
                     WHERE identity_id = ?1 AND scope_type = ?2 AND canonical_scope_id = ?3
                       AND (?5 IS NULL OR id = ?5)
                       AND (runtime_session_id = ?4
                            OR (runtime_session_id IS NULL AND retired_at IS NULL AND id = ?5
                                AND (claim_owner IS NULL OR claim_owner = ?4))
                            OR (scope_type <> 'agent_chat' AND claim_owner IS NULL
                                AND retired_at IS NULL
                                AND NOT EXISTS(SELECT 1 FROM agent_lcm_entry e
                                               WHERE e.timeline_id = agent_lcm_timeline.id)))
                     ORDER BY runtime_session_id IS NULL ASC LIMIT 1",
                    )
                    .bind(&identity_id)
                    .bind(&stored_scope_type)
                    .bind(&stored_scope_id)
                    .bind(runtime_id)
                    .bind(referenced.as_deref())
                    .fetch_optional(&mut *tx)
                    .await
                }
                .map_err(protected_database_error)?
            };
            let id = match candidate {
                Some(id) => {
                    // Adopting a legacy owner-less row stamps its owner, so
                    // the lookup above can never hand it to another session
                    // (a rotation successor in particular).
                    {
                        #[cfg(feature = "test-support")]
                        let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0);
                        sqlx::query(
                            "UPDATE agent_lcm_timeline SET runtime_session_id = ?
                         WHERE id = ? AND runtime_session_id IS NULL",
                        )
                        .bind(runtime_id)
                        .bind(&id)
                        .execute(&mut *tx)
                        .await
                    }
                    .map_err(protected_database_error)?;
                    id
                }
                None if referenced.is_some() => {
                    return Err(RuntimeError::conflict(
                        "persisted LCM timeline is unavailable or belongs to another session",
                    ));
                }
                None => {
                    let id = db::new_uuid_v4();
                    let now = db::now_rfc3339();
                    {
                        #[cfg(feature = "test-support")]
                        let _sql_profile = crate::session_journal::profile::timer("sql.INSERT", 0);
                        sqlx::query(
                            "INSERT INTO agent_lcm_timeline (
                            id, identity_id, scope_type, scope_id, canonical_scope_id,
                            runtime_session_id, authorization_revision, revision,
                            created_at, updated_at
                         ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
                        )
                        .bind(&id)
                        .bind(&identity_id)
                        .bind(&stored_scope_type)
                        .bind(format!("{stored_scope_id}#topic:{runtime_id}"))
                        .bind(&stored_scope_id)
                        .bind(runtime_id)
                        .bind(authorization_revision.as_str())
                        .bind(&now)
                        .bind(&now)
                        .execute(&mut *tx)
                        .await
                    }
                    .map_err(protected_database_error)?;
                    id
                }
            };
            {
                #[cfg(feature = "test-support")]
                let _sql_profile = crate::session_journal::profile::timer("sql.INSERT", 0);
                sqlx::query(
                    "INSERT INTO agent_runtime_lcm_binding (runtime_session_id, timeline_id)
                 VALUES (?, ?) ON CONFLICT(runtime_session_id) DO UPDATE SET timeline_id=excluded.timeline_id",
                )
                .bind(runtime_id)
                .bind(&id)
                .execute(&mut *tx)
                .await
            }
            .map_err(protected_database_error)?;
            id
        };
        let receipt_stamp = if receipts_completed {
            Some(Self::legacy_lcm_receipt_stamp(&mut tx, runtime_id, &timeline_id).await?)
        } else {
            None
        };
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
            tx.commit().await
        }
        .map_err(|error| protected_database_error(error).retryable())?;
        if let Some(admission) = sibling_admission {
            admission.close().await.map_err(RuntimeError::retryable)?;
        }
        if let Some(stamp) = receipt_stamp {
            if let Ok(mut cached) = self.legacy_lcm_receipt_stamps.lock() {
                if cached.len() >= 4_096 {
                    cached.clear();
                }
                cached.insert(runtime_id.to_owned(), stamp);
            }
        }
        let timeline = db::AgentLcmRepo::get_lcm_timeline(&*self.db, &timeline_id)
            .await
            .map_err(protected_database_error)?
            .ok_or_else(|| RuntimeError::conflict("LCM binding timeline is missing"))?;
        // V149 retirement frees the canonical scope without revoking the
        // owning runtime's binding or its fenced claim.
        let matches_owner: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_lcm_timeline WHERE id=?
             AND identity_id=? AND scope_type=? AND canonical_scope_id=?
             AND (scope_type <> 'agent_chat' OR runtime_session_id IS NULL OR runtime_session_id=?5)
             AND (claim_owner IS NULL OR claim_owner=?5))",
        )
        .bind(&timeline_id)
        .bind(&identity_id)
        .bind(&stored_scope_type)
        .bind(&stored_scope_id)
        .bind(runtime_id)
        .fetch_one(self.db.pool())
        .await
        .map_err(protected_database_error)?;
        if !matches_owner {
            return Err(RuntimeError::conflict(
                "LCM timeline does not match its session binding",
            ));
        }
        Ok(crate::SqliteLcmStore::new(self.db.clone(), timeline))
    }
}
