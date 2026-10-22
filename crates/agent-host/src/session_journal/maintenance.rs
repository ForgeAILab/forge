//! Optional S2 placement changes. Liveness and physical deletion stay in apply.
use super::{
    cache::Scope,
    codec, corrupt,
    diagnostics::unreadable,
    seal::role,
    storage::{
        ForgeJournalStorage, PackView, Staged, ambiguous_database_error, bit, database_error,
    },
};
use agent_runtime::core::{error::RuntimeError, ids::SessionId, journal::*};
use sqlx::SqliteConnection;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Instant,
};

const SMALL_PACK: usize = 64 * 1024;
const COMPACT_BUDGET: usize = 8 * 1024 * 1024;

impl ForgeJournalStorage {
    /// Apply a committed physical rewrite to the shared cache; on failure the
    /// whole session is forgotten because the outcome may be ambiguous.
    pub(super) fn settle_cache(
        &self,
        session: &SessionId,
        staged: Staged,
        committed: bool,
    ) -> Result<(), RuntimeError> {
        let mut cache = self.cache_lock().map_err(RuntimeError::retryable)?;
        if committed {
            cache.committed_rewrite(session, &staged.removed, staged.written);
        } else {
            cache.invalidate(session);
        }
        Ok(())
    }
    pub(super) fn repacking(&self) -> bool {
        #[cfg(feature = "test-support")]
        {
            self.repack.load(std::sync::atomic::Ordering::Relaxed)
        }
        #[cfg(not(feature = "test-support"))]
        {
            true
        }
    }
    #[cfg(feature = "test-support")]
    pub fn set_repacking(&self, enabled: bool) {
        self.repack
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }
    /// Test-only compact transaction count and largest rewritten logical size.
    #[cfg(feature = "test-support")]
    pub fn compact_stats(&self) -> Result<(u64, usize), RuntimeError> {
        self.compact_observations
            .lock()
            .map(|s| *s)
            .map_err(|_| corrupt())
    }
    async fn merge_packs(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        selected: &[PackView],
        staged: &mut Staged,
    ) -> Result<Vec<i64>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("maintenance.merge_packs", 0);
        if selected.iter().any(|v| !v.fully_live()) {
            return Err(corrupt());
        }
        let owner = &selected.first().ok_or_else(corrupt)?.index.key.owner;
        if selected.iter().any(|v| &v.index.key.owner != owner) {
            return Err(corrupt());
        }
        let wanted = selected.iter().map(|v| v.index.key.id).collect();
        let bodies = self.bodies(conn, selected, &wanted, false).await?;
        // Which domains hold each moved body, from each domain's own bits.
        let mut domains = Vec::<JournalDomain>::new();
        let mut holders = HashMap::<JournalObjectId, Vec<usize>>::new();
        for view in selected {
            let pack = bodies.get(&view.index.key.id).ok_or_else(corrupt)?;
            for (domain, bits) in std::iter::once((&view.domain, &view.bitmap))
                .chain(view.others.iter().map(|(d, b)| (d, b)))
            {
                let index = match domains.iter().position(|d| d == domain) {
                    Some(index) => index,
                    None => {
                        domains.push(domain.clone());
                        domains.len() - 1
                    }
                };
                for (slot, object) in pack.iter().enumerate() {
                    if bit(bits, slot) {
                        holders.entry(object.id.clone()).or_default().push(index);
                    }
                }
            }
        }
        let objects: Vec<_> = bodies
            .into_values()
            .flat_map(|p| p.as_ref().clone())
            .collect();
        let packs = self
            .work(move || {
                let mut objects = objects;
                // Partition the kind-ordered stream before sorting each directory.
                objects.sort_by(|a, b| a.kind.as_str().cmp(b.kind.as_str()).then(a.id.cmp(&b.id)));
                let mut packs = codec::split(objects)?;
                for pack in &mut packs {
                    pack.sort_by(|a, b| a.id.cmp(&b.id));
                }
                Ok(packs)
            })
            .await?;
        for view in selected {
            self.delete_pack(conn, &scope.1, view.index.key.id, staged)
                .await?;
        }
        let mut ids = Vec::new();
        for pack in packs {
            let mut bitmaps = vec![vec![0u8; pack.len().div_ceil(8)]; domains.len()];
            for (slot, object) in pack.iter().enumerate() {
                for &holder in holders.get(&object.id).ok_or_else(corrupt)? {
                    bitmaps[holder][slot / 8] |= 1 << (slot % 8);
                }
            }
            let pack_holders = domains
                .iter()
                .cloned()
                .zip(bitmaps)
                .filter(|(_, bits)| bits.iter().any(|b| *b != 0))
                .collect();
            ids.push(
                self.insert_session_pack(
                    conn,
                    &scope.1,
                    owner,
                    Arc::new(pack),
                    pack_holders,
                    staged,
                )
                .await?,
            );
        }
        Ok(ids)
    }
    pub(super) async fn collection_repack(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        purged: &BTreeSet<i64>,
        staged: &mut Staged,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("maintenance.collection_repack", 0);
        if !self.repacking() {
            return Ok(());
        }
        let views = self.directory(conn, scope, false).await?;
        let selected: Vec<_> = views
            .into_iter()
            // Like S2, only packs the collecting domain holds entirely.
            .filter(|v| {
                v.fully_held()
                    && (v.index.key.retained < SMALL_PACK || purged.contains(&v.index.key.id))
            })
            .collect();
        if selected.len() > 1 {
            let bytes: usize = selected.iter().map(|v| v.index.key.retained).sum();
            let packs = selected.len();
            self.merge_packs(conn, scope, &selected, staged).await?;
            tracing::debug!(target:"forge_session_journal",kind="collection_repack",rewritten_packs=packs,rewritten_bytes=bytes,"journal maintenance");
        }
        Ok(())
    }
    /// Await after successful terminal publication. Failure is retryable
    /// maintenance and cannot roll back the already acknowledged publication.
    pub async fn post_terminal(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("maintenance.post_terminal", 0);
        if !self.repacking() {
            return Ok(());
        }
        let started = Instant::now();
        let scope = (domain.clone(), session.clone());
        let mut tx = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN_IMMEDIATE", 0);
            db::begin_immediate(self.db.pool()).await
        }
        .map_err(database_error)?;
        let locked = Instant::now();
        let mut rewritten_packs = 0usize;
        let mut rewritten_bytes = 0usize;
        let mut staged = Staged::default();
        let result = async {
            // Maintenance may not rewrite an unreadable session's packs.
            self.load_on(&mut tx, domain, session).await?;
            let mut views = self.directory(&mut tx, &scope, false).await?;
            loop {
                let mut tiers = BTreeMap::<u32, Vec<PackView>>::new();
                // A domain's hook moves only bodies that domain holds: a pack
                // with a slot only another domain holds is that domain's to
                // merge. Otherwise one hook pairs the other domain's stable
                // history with its own short-lived bodies, and the next purge
                // must rewrite the whole merged pack.
                for view in views.iter().filter(|v| v.fully_held()) {
                    tiers
                        .entry(view.index.key.retained.max(4096).ilog2())
                        .or_default()
                        .push(view.clone());
                }
                for tier in tiers.values_mut() {
                    tier.sort_by_key(|v| (v.index.key.retained, v.index.key.id));
                }
                let pair = tiers
                    .values()
                    .filter(|v| v.len() > 1)
                    .map(|v| vec![v[0].clone(), v[1].clone()])
                    .find(|p| {
                        p.iter().map(|v| v.index.key.retained).sum::<usize>() <= codec::PACK_CAP
                    });
                let Some(pair) = pair else {
                    break;
                };
                rewritten_bytes += pair.iter().map(|v| v.index.key.retained).sum::<usize>();
                rewritten_packs += 2;
                self.merge_packs(&mut tx, &scope, &pair, &mut staged)
                    .await?;
                views = self.directory(&mut tx, &scope, false).await?;
            }
            Ok::<_, RuntimeError>(())
        }
        .await;
        // No transient directory/body is ever published by physical work.
        let result = match result {
            Ok(()) => {
                #[cfg(feature = "test-support")]
                let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
                tx.commit().await
            }
            .map_err(ambiguous_database_error),
            Err(e) => {
                {
                    #[cfg(feature = "test-support")]
                    let _sql_profile = crate::session_journal::profile::timer("sql.ROLLBACK", 0);
                    tx.rollback().await
                }
                .map_err(ambiguous_database_error)?;
                Err(e)
            }
        };
        #[cfg(feature = "test-support")]
        self.observe_write_hold(locked.elapsed().as_micros() as u64);
        // Unchanged immutable bodies stay cached across S2; merged output is
        // published, and removed pack ids are forgotten.
        self.settle_cache(session, staged, result.is_ok())?;
        tracing::debug!(target:"forge_session_journal",kind="post_terminal",duration_us=started.elapsed().as_micros() as u64,lock_hold_us=locked.elapsed().as_micros() as u64,rewritten_packs,rewritten_bytes,success=result.is_ok(),"journal maintenance");
        result
    }
    pub(super) async fn compact_domain(&self, domain: &JournalDomain) -> Result<(), RuntimeError> {
        // Keyset the original scopes; each write transaction touches only one.
        let mut after: Option<String> = None;
        loop {
            // Sessions in which this domain holds packs; packs are session-owned.
            let session: Option<String> = { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0); sqlx::query_scalar("SELECT runtime_session_id FROM agent_session_journal_membership WHERE storage_id=? AND role=? AND (? IS NULL OR runtime_session_id>?) ORDER BY runtime_session_id LIMIT 1")
                .bind(domain.storage_id.as_str()).bind(role(domain)).bind(&after).bind(&after).fetch_optional(self.db.pool()).await }.map_err(database_error)?;
            let Some(session) = session else {
                break;
            };
            let scope = (domain.clone(), SessionId::new(&session));
            match self.load(domain, &scope.1).await {
                Err(error) if unreadable(&error) => {
                    Self::note_unreadable_session(&scope.1);
                    after = Some(session);
                    continue;
                }
                Err(error) => return Err(error),
                Ok(_) => {}
            }
            let upper: i64 = { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0); sqlx::query_scalar("SELECT coalesce(max(pack_id),0) FROM agent_session_journal_pack WHERE runtime_session_id=?")
                .bind(&session).fetch_one(self.db.pool()).await }.map_err(database_error)?;
            let mut cursor = 0;
            while cursor < upper {
                let started = Instant::now();
                let mut tx = {
                    #[cfg(feature = "test-support")]
                    let _sql_profile =
                        crate::session_journal::profile::timer("sql.BEGIN_IMMEDIATE", 0);
                    db::begin_immediate(self.db.pool()).await
                }
                .map_err(database_error)?;
                let locked = Instant::now();
                let mut bytes = 0;
                let mut count = 0;
                let mut staged = Staged::default();
                let result = async {
                    let views = self.directory(&mut tx, &scope, false).await?;
                    let mut selected = Vec::new();
                    let lower = cursor;
                    for view in views
                        .into_iter()
                        .filter(|v| v.index.key.id > lower && v.index.key.id <= upper)
                    {
                        if view.fully_live() {
                            if bytes + view.index.key.retained > COMPACT_BUDGET {
                                break;
                            }
                            bytes += view.index.key.retained;
                            selected.push(view.clone());
                        }
                        cursor = view.index.key.id;
                    }
                    if selected.len() > 1 {
                        count = selected.len();
                        self.merge_packs(&mut tx, &scope, &selected, &mut staged)
                            .await?;
                    } else {
                        bytes = 0;
                    }
                    Ok::<_, RuntimeError>(())
                }
                .await;
                let result = match result {
                    Ok(()) => {
                        #[cfg(feature = "test-support")]
                        let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
                        tx.commit().await
                    }
                    .map_err(ambiguous_database_error),
                    Err(e) => {
                        {
                            #[cfg(feature = "test-support")]
                            let _sql_profile =
                                crate::session_journal::profile::timer("sql.ROLLBACK", 0);
                            tx.rollback().await
                        }
                        .map_err(ambiguous_database_error)?;
                        Err(e)
                    }
                };
                #[cfg(feature = "test-support")]
                self.observe_write_hold(locked.elapsed().as_micros() as u64);
                self.settle_cache(&scope.1, staged, result.is_ok())?;
                tracing::debug!(target:"forge_session_journal",kind="compact",duration_us=started.elapsed().as_micros() as u64,lock_hold_us=locked.elapsed().as_micros() as u64,rewritten_packs=count,rewritten_bytes=bytes,success=result.is_ok(),"journal maintenance");
                match result {
                    Err(error) if unreadable(&error) => {
                        Self::note_unreadable_session(&scope.1);
                        break;
                    }
                    Err(error) => return Err(error),
                    Ok(()) => {}
                }
                #[cfg(feature = "test-support")]
                {
                    let mut stats = self.compact_observations.lock().map_err(|_| corrupt())?;
                    stats.0 += 1;
                    stats.1 = stats.1.max(bytes);
                }
                // Deletions by an independent writer may remove all remaining
                // original ids. New repack ids are deliberately beyond upper.
                if count == 0 {
                    break;
                }
            }
            after = Some(session);
        }
        db::incremental_vacuum(self.db.pool())
            .await
            // Vacuum can have completed work before losing its acknowledgement.
            .map_err(ambiguous_database_error)
    }
}
