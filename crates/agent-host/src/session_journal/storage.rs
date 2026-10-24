use super::{
    cache::*,
    codec, corrupt,
    seal::{Binding, FORMAT, Keys, Unit, role},
    wire,
};
use agent_runtime::core::{error::RuntimeError, ids::SessionId, journal::*};
use async_trait::async_trait;
use db::SqliteDb;
use sqlx::{QueryBuilder, Row, Sqlite, SqliteConnection, sqlite::SqliteRow};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex, MutexGuard},
};
use tokio::sync::Semaphore;

type Bodies = BTreeMap<i64, Arc<Vec<JournalStoredObject>>>;

/// Encrypted sqlx storage, independent of the runtime's journal semantics.
/// Construct with the same master key and revision as SqliteProtectedRuntimeStore.
#[derive(Clone)]
pub struct ForgeJournalStorage {
    pub(super) db: Arc<SqliteDb>,
    pub(super) keys: Keys,
    pub(super) cache: Arc<Mutex<Cache>>,
    cpu: Arc<Semaphore>,
    #[cfg(feature = "test-support")]
    max_write_hold_us: Arc<std::sync::atomic::AtomicU64>,
    #[cfg(feature = "test-support")]
    record_loads: Arc<Mutex<std::collections::HashMap<(JournalDomain, SessionId), usize>>>,
    #[cfg(feature = "test-support")]
    pub(super) failed_commit_observation: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(feature = "test-support")]
    pub(super) repack: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(feature = "test-support")]
    pub(super) compact_observations: Arc<Mutex<(u64, usize)>>,
    #[cfg(feature = "test-support")]
    pub(super) byte_stats_calls: Arc<std::sync::atomic::AtomicU64>,
    #[cfg(feature = "test-support")]
    pub(super) pin_rank_reads: Arc<std::sync::atomic::AtomicU64>,
}
impl fmt::Debug for ForgeJournalStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForgeJournalStorage")
            .field("key_revision", &self.keys.revision)
            .finish_non_exhaustive()
    }
}
/// One session pack as seen by one domain. `bitmap` is that domain's own
/// membership and alone grants reads; `others` are the other domains' bitmaps
/// of the same session, used only for liveness and atomic remaps.
#[derive(Clone)]
pub(super) struct PackView {
    pub index: IndexEntry,
    pub domain: JournalDomain,
    pub bitmap: Vec<u8>,
    pub others: Vec<(JournalDomain, Vec<u8>)>,
}
pub(super) fn bit(bits: &[u8], slot: usize) -> bool {
    bits.get(slot / 8)
        .is_some_and(|b| b & (1 << (slot % 8)) != 0)
}
pub(super) fn full_bitmap(count: usize) -> Vec<u8> {
    let mut bits = vec![255; count.div_ceil(8)];
    if count % 8 != 0 {
        if let Some(last) = bits.last_mut() {
            *last = (1 << (count % 8)) - 1;
        }
    }
    bits
}
/// Each requested id's four-byte directory prefix, computed once per call.
pub(super) fn prefix(id: &JournalObjectId) -> [u8; 4] {
    let bytes = id.to_bytes();
    [bytes[0], bytes[1], bytes[2], bytes[3]]
}
impl PackView {
    /// Read authority of the viewing domain only.
    pub fn held(&self, slot: usize) -> bool {
        bit(&self.bitmap, slot)
    }
    /// Physically live: some domain of the session holds the slot.
    pub fn live(&self, slot: usize) -> bool {
        self.held(slot) || self.others.iter().any(|(_, bits)| bit(bits, slot))
    }
    pub fn fully_live(&self) -> bool {
        (0..self.index.key.count).all(|s| self.live(s))
    }
    /// Another domain holds every slot and the viewing domain does not.
    pub fn others_pack(&self) -> bool {
        let n = self.index.key.count;
        !(0..n).all(|s| self.held(s))
            && self
                .others
                .iter()
                .any(|(_, bits)| (0..n).all(|s| bit(bits, s)))
    }
}
/// Final surviving bodies keyed by pack id, plus ids invalidated by the transaction.
/// In-place rewrites invalidate the old generation and retain the final output.
/// Applied to the shared cache only after COMMIT succeeds.
#[derive(Default)]
pub(super) struct Staged {
    pub written: BTreeMap<i64, (PackKey, Arc<Vec<JournalStoredObject>>)>,
    pub removed: BTreeSet<i64>,
    pub layout_bumped: bool,
}
pub(super) fn domain_of(storage_id: String, role: &str) -> Result<JournalDomain, RuntimeError> {
    Ok(JournalDomain {
        storage_id: JournalStorageId::new(storage_id),
        role: match role {
            "protected" => JournalRole::Protected,
            "ordinary" => JournalRole::Ordinary,
            _ => return Err(corrupt()),
        },
    })
}
pub(super) fn counter(row: &SqliteRow, name: &str) -> Result<u64, RuntimeError> {
    let value: Vec<u8> = row.try_get(name).map_err(|_| corrupt())?;
    Ok(u64::from_be_bytes(
        value.as_slice().try_into().map_err(|_| corrupt())?,
    ))
}
pub(super) fn flag(row: &SqliteRow, name: &str) -> Result<bool, RuntimeError> {
    match row.try_get::<i64, _>(name).map_err(|_| corrupt())? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(corrupt()),
    }
}
pub(super) fn database_error(error: impl Into<db::DbError>) -> RuntimeError {
    // Inspect sqlx's codes through the DB boundary, never its potentially
    // sensitive message. Includes extended BUSY/LOCKED codes (low byte 5/6),
    // IO/CANTOPEN and pool/worker availability errors.
    let retryable = error.into().is_transient();
    let error = RuntimeError::internal("journal database operation failed");
    if retryable { error.retryable() } else { error }
}

pub(super) fn ambiguous_database_error(error: impl Into<db::DbError>) -> RuntimeError {
    // A failed COMMIT acknowledgement cannot prove absence. The runtime must
    // reload/retry, even when the underlying error normally is deterministic.
    database_error(error).retryable()
}
impl ForgeJournalStorage {
    /// This constructor performs no SQL and decodes no pack units.
    pub fn new(
        db: Arc<SqliteDb>,
        master_key: [u8; 32],
        key_revision: i64,
    ) -> Result<Self, RuntimeError> {
        Ok(Self {
            db,
            keys: Keys::new(&master_key, key_revision)?,
            cache: Arc::new(Mutex::new(Cache::new(DEFAULT_BUDGET))),
            cpu: Arc::new(Semaphore::new(2)),
            #[cfg(feature = "test-support")]
            max_write_hold_us: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "test-support")]
            record_loads: Arc::new(Mutex::new(Default::default())),
            #[cfg(feature = "test-support")]
            failed_commit_observation: Arc::new(std::sync::atomic::AtomicU8::new(0)),
            #[cfg(feature = "test-support")]
            repack: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            #[cfg(feature = "test-support")]
            compact_observations: Arc::new(Mutex::new((0, 0))),
            #[cfg(feature = "test-support")]
            byte_stats_calls: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "test-support")]
            pin_rank_reads: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }
    #[cfg(feature = "test-support")]
    pub(super) fn observe_write_hold(&self, micros: u64) {
        self.max_write_hold_us
            .fetch_max(micros, std::sync::atomic::Ordering::Relaxed);
    }
    #[cfg(feature = "test-support")]
    pub fn max_write_hold_us_for_test(&self) -> u64 {
        self.max_write_hold_us
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    /// Make the first protected load after an injected commit failure fail too,
    /// leaving acceptance unresolved until the next session open.
    #[cfg(feature = "test-support")]
    pub fn fail_commit_observation_for_test(&self) {
        self.failed_commit_observation
            .store(1, std::sync::atomic::Ordering::SeqCst);
    }
    #[cfg(feature = "test-support")]
    pub fn session_record_loads_for_test(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> usize {
        self.record_loads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(domain.clone(), session.clone()))
            .copied()
            .unwrap_or(0)
    }
    /// Call only after publication/host commit, with no pooled connection held.
    pub fn nudge_retention(&self) {
        self.db.session_journal_cleanup_notify().notify_waiters();
    }
    pub(super) fn cache_lock(&self) -> Result<MutexGuard<'_, Cache>, RuntimeError> {
        self.cache
            .lock()
            .map_err(|_| RuntimeError::internal("journal cache unavailable").retryable())
    }
    pub(super) async fn work<T: Send + 'static>(
        &self,
        job: impl FnOnce() -> Result<T, RuntimeError> + Send + 'static,
    ) -> Result<T, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("work.await", 0);
        let permit = self
            .cpu
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RuntimeError::internal("journal codec unavailable").retryable())?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            #[cfg(feature = "test-support")]
            let _profile = super::profile::timer("work.cpu", 0);
            job()
        })
        .await
        .map_err(|_| RuntimeError::internal("journal codec worker failed").retryable())?
    }
    pub(super) fn revision(&self, row: &SqliteRow) -> Result<(), RuntimeError> {
        if row
            .try_get::<i64, _>("key_revision")
            .map_err(|_| corrupt())?
            != self.keys.revision
            || row.try_get::<i64, _>("format").map_err(|_| corrupt())? != FORMAT
        {
            return Err(corrupt());
        }
        Ok(())
    }
    pub(super) async fn record_row(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        row: SqliteRow,
    ) -> Result<JournalSessionRecord, RuntimeError> {
        #[cfg(feature = "test-support")]
        {
            *self
                .record_loads
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry((domain.clone(), session.clone()))
                .or_default() += 1;
        }
        self.revision(&row)?;
        let keys = self.keys.clone();
        let domain = domain.clone();
        let session = session.clone();
        self.work(move || {
            let generation = counter(&row, "generation")?;
            let owner: String = row.try_get("forge_session_id").map_err(|_| corrupt())?;
            let nonce: Vec<u8> = row.try_get("nonce").map_err(|_| corrupt())?;
            let payload: Vec<u8> = row.try_get("payload").map_err(|_| corrupt())?;
            let raw = keys.open(
                &Binding {
                    unit: Unit::Session,
                    domain: Some(&domain),
                    session: &session,
                    owner: &owner,
                    row: b"",
                    generation,
                },
                &nonce,
                &payload,
                wire::MAX_METADATA,
            )?;
            let record = wire::record(&raw)?;
            if record.generation != generation
                || record.revision.0 != counter(&row, "revision")?
                || record.writer_epoch != counter(&row, "writer_epoch")?
                || record.collection_dirty != flag(&row, "collection_dirty")?
                || record.head.is_some() != flag(&row, "published")?
                || record.has_checkpoint != flag(&row, "has_checkpoint")?
                || record.retirement.is_some() != flag(&row, "retired")?
                || (record.has_checkpoint && (record.head.is_none() || record.retirement.is_some()))
            {
                return Err(corrupt());
            }
            if let Some(head) = &record.head {
                if head.kind().map_err(|_| corrupt())? != JournalObjectKind::Head {
                    return Err(corrupt());
                }
            }
            Ok(record)
        })
        .await
    }
    pub(super) async fn pin_row(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        row: SqliteRow,
    ) -> Result<JournalPinRecord, RuntimeError> {
        self.revision(&row)?;
        let keys = self.keys.clone();
        let domain = domain.clone();
        let session = session.clone();
        self.work(move || {
            let owner: String = row.try_get("forge_session_id").map_err(|_| corrupt())?;
            let key: Vec<u8> = row.try_get("pin_key").map_err(|_| corrupt())?;
            let raw = keys.open(
                &Binding {
                    unit: Unit::Pin,
                    domain: Some(&domain),
                    session: &session,
                    owner: &owner,
                    row: &key,
                    generation: 0,
                },
                &row.try_get::<Vec<u8>, _>("nonce").map_err(|_| corrupt())?,
                &row.try_get::<Vec<u8>, _>("payload")
                    .map_err(|_| corrupt())?,
                wire::MAX_METADATA,
            )?;
            let p = wire::pin(&raw)?;
            if keys.lookup(Unit::Pin, &domain, &session, p.pin.as_str().as_bytes()) != key {
                return Err(corrupt());
            }
            Ok(p)
        })
        .await
    }
    pub(super) async fn load_on(
        &self,
        conn: &mut SqliteConnection,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        let row={ #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0); sqlx::query("SELECT * FROM agent_session_journal_session WHERE storage_id=? AND role=? AND runtime_session_id=?")
            .bind(domain.storage_id.as_str()).bind(role(domain)).bind(session.as_str()).fetch_optional(&mut *conn).await }.map_err(database_error)?;
        match row {
            Some(row) => Ok(Some(self.record_row(domain, session, row).await?)),
            None => Ok(None),
        }
    }
    /// Fetch just this scope's headers and membership in the caller's snapshot;
    /// only new/changed sealed indexes are fetched and decoded. No body prefetch.
    pub(super) async fn directory(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        publish: bool,
    ) -> Result<Vec<PackView>, RuntimeError> {
        self.directory_from(conn, scope, publish, None).await
    }
    pub(super) async fn directory_from(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        publish: bool,
        prior: Option<Arc<ScopeDirectory>>,
    ) -> Result<Vec<PackView>, RuntimeError> {
        self.directory_range(conn, scope, publish, prior, 0, i64::MAX)
            .await
    }
    pub(super) async fn directory_range(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        publish: bool,
        prior: Option<Arc<ScopeDirectory>>,
        first_pack: i64,
        limit: i64,
    ) -> Result<Vec<PackView>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("storage.directory_from", 0);
        let (domain, session) = scope;
        let rows={ #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0); sqlx::query("SELECT pack_id,generation,object_count,retained_canonical_bytes,forge_session_id,key_revision,format FROM agent_session_journal_pack WHERE runtime_session_id=? AND pack_id>=? ORDER BY pack_id LIMIT ?")
            .bind(session.as_str()).bind(first_pack).bind(limit).fetch_all(&mut *conn).await }.map_err(database_error)?;
        // Every domain's bitmap in the same snapshot; the composite FK binds
        // each row to its pack's session and Forge owner label.
        let members={ #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0); sqlx::query("SELECT storage_id,role,pack_id,bitmap FROM agent_session_journal_membership WHERE runtime_session_id=? AND pack_id IN (SELECT pack_id FROM agent_session_journal_pack WHERE runtime_session_id=? AND pack_id>=? ORDER BY pack_id LIMIT ?) ORDER BY pack_id,storage_id,role")
            .bind(session.as_str()).bind(session.as_str()).bind(first_pack).bind(limit).fetch_all(&mut *conn).await }.map_err(database_error)?;
        let mut bitmaps = BTreeMap::<i64, (Option<Vec<u8>>, Vec<(JournalDomain, Vec<u8>)>)>::new();
        for row in members {
            let holder = domain_of(
                row.try_get("storage_id").map_err(|_| corrupt())?,
                &row.try_get::<String, _>("role").map_err(|_| corrupt())?,
            )?;
            let id: i64 = row.try_get("pack_id").map_err(|_| corrupt())?;
            let bits: Vec<u8> = row.try_get("bitmap").map_err(|_| corrupt())?;
            let entry = bitmaps.entry(id).or_default();
            if &holder == domain {
                entry.0 = Some(bits);
            } else {
                entry.1.push((holder, bits));
            }
        }
        let cached = match prior {
            Some(p) => p,
            None => self.cache_lock()?.directory(session).unwrap_or_default(),
        };
        let mut headers = BTreeMap::new();
        let mut missing = Vec::new();
        for row in rows {
            self.revision(&row)?;
            let id: i64 = row.try_get("pack_id").map_err(|_| corrupt())?;
            let count = usize::try_from(
                row.try_get::<i64, _>("object_count")
                    .map_err(|_| corrupt())?,
            )
            .map_err(|_| corrupt())?;
            let retained = usize::try_from(
                row.try_get::<i64, _>("retained_canonical_bytes")
                    .map_err(|_| corrupt())?,
            )
            .map_err(|_| corrupt())?;
            if count == 0 || count > retained || retained > codec::PACK_CAP {
                return Err(corrupt());
            }
            let (bitmap, others) = bitmaps.remove(&id).unwrap_or_default();
            let bitmap = bitmap.unwrap_or_else(|| vec![0; count.div_ceil(8)]);
            validate_bitmap(&bitmap, count)?;
            for (_, bits) in &others {
                validate_bitmap(bits, count)?;
            }
            let key = PackKey {
                session: session.clone(),
                id,
                generation: counter(&row, "generation")?,
                count,
                retained,
                owner: row.try_get("forge_session_id").map_err(|_| corrupt())?,
            };
            if !cached.packs.get(&id).is_some_and(|p| p.key == key) {
                missing.push(id);
            }
            headers.insert(id, (key, bitmap, others));
        }
        if !bitmaps.is_empty() {
            return Err(corrupt());
        }
        let mut indexes = BTreeMap::new();
        for chunk in missing.chunks(400) {
            let mut q = QueryBuilder::<Sqlite>::new(
                "SELECT pack_id,index_nonce,index_payload FROM agent_session_journal_pack WHERE runtime_session_id=",
            );
            q.push_bind(session.as_str()).push(" AND pack_id IN (");
            let mut separated = q.separated(",");
            for id in chunk {
                separated.push_bind(*id);
            }
            separated.push_unseparated(")");
            let rows = {
                #[cfg(feature = "test-support")]
                let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0);
                q.build().fetch_all(&mut *conn).await
            }
            .map_err(database_error)?;
            self.cache_lock()?.pack_queries += 1;
            let jobs: Vec<_> = rows
                .into_iter()
                .map(|row| {
                    let id = row.try_get::<i64, _>("pack_id").map_err(|_| corrupt())?;
                    Ok((
                        headers.get(&id).ok_or_else(corrupt)?.0.clone(),
                        row.try_get::<Vec<u8>, _>("index_nonce")
                            .map_err(|_| corrupt())?,
                        row.try_get::<Vec<u8>, _>("index_payload")
                            .map_err(|_| corrupt())?,
                    ))
                })
                .collect::<Result<_, RuntimeError>>()?;
            let keys = self.keys.clone();
            let decoded = self
                .work(move || {
                    jobs.into_iter()
                        .map(|(key, nonce, payload)| {
                            let raw = keys.open(
                                &Binding {
                                    unit: Unit::PackIndex,
                                    domain: None,
                                    session: &key.session,
                                    owner: &key.owner,
                                    row: &key.id.to_be_bytes(),
                                    generation: key.generation,
                                },
                                &nonce,
                                &payload,
                                codec::MAX_INDEX_BYTES,
                            )?;
                            let d = Arc::new(codec::decode_index(&raw, key.count, key.retained)?);
                            Ok((key.id, d))
                        })
                        .collect::<Result<Vec<_>, RuntimeError>>()
                })
                .await?;
            self.cache_lock()?.index_decodes += decoded.len() as u64;
            indexes.extend(decoded);
        }
        let mut snapshot = Vec::with_capacity(headers.len());
        let mut next = ScopeDirectory::default();
        for (id, (key, bitmap, others)) in headers {
            let directory = if let Some(d) = indexes.remove(&id) {
                d
            } else {
                cached
                    .packs
                    .get(&id)
                    .filter(|p| p.key == key)
                    .ok_or_else(corrupt)?
                    .directory
                    .clone()
            };
            let index = IndexEntry { key, directory };
            next.packs.insert(id, index.clone());
            snapshot.push(PackView {
                index,
                domain: domain.clone(),
                bitmap,
                others,
            });
        }
        if publish {
            self.cache_lock()?
                .put_directory(session.clone(), Arc::new(next));
        }
        Ok(snapshot)
    }
    pub(super) async fn bodies(
        &self,
        conn: &mut SqliteConnection,
        views: &[PackView],
        wanted: &BTreeSet<i64>,
        publish: bool,
    ) -> Result<Bodies, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("storage.bodies", 0);
        let mut bodies = BTreeMap::new();
        let mut missing = Vec::new();
        let view_map: BTreeMap<_, _> = views
            .iter()
            .filter(|v| wanted.contains(&v.index.key.id))
            .map(|v| (v.index.key.id, v))
            .collect();
        for (&id, v) in &view_map {
            if let Some(pack) = self.cache_lock()?.body(&v.index.key) {
                bodies.insert(id, pack);
            } else {
                missing.push(id);
            }
        }
        for chunk in missing.chunks(400) {
            let mut q = QueryBuilder::<Sqlite>::new(
                "SELECT pack_id,body_nonce,body_payload FROM agent_session_journal_pack WHERE pack_id IN (",
            );
            let mut separated = q.separated(",");
            for id in chunk {
                separated.push_bind(*id);
            }
            separated.push_unseparated(")");
            let rows = {
                #[cfg(feature = "test-support")]
                let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0);
                q.build().fetch_all(&mut *conn).await
            }
            .map_err(database_error)?;
            self.cache_lock()?.pack_queries += 1;
            let jobs: Vec<_> = rows
                .into_iter()
                .map(|row| {
                    let id = row.try_get::<i64, _>("pack_id").map_err(|_| corrupt())?;
                    Ok((
                        view_map.get(&id).ok_or_else(corrupt)?.index.clone(),
                        row.try_get::<Vec<u8>, _>("body_nonce")
                            .map_err(|_| corrupt())?,
                        row.try_get::<Vec<u8>, _>("body_payload")
                            .map_err(|_| corrupt())?,
                    ))
                })
                .collect::<Result<_, RuntimeError>>()?;
            let keys = self.keys.clone();
            let decoded = self
                .work(move || {
                    jobs.into_iter()
                        .map(|(index, nonce, payload)| {
                            let key = &index.key;
                            let raw = keys.open(
                                &Binding {
                                    unit: Unit::PackBody,
                                    domain: None,
                                    session: &key.session,
                                    owner: &key.owner,
                                    row: &key.id.to_be_bytes(),
                                    generation: key.generation,
                                },
                                &nonce,
                                &payload,
                                codec::MAX_COMPRESSED_BYTES,
                            )?;
                            let body = Arc::new(codec::decode_body(&index.directory, &raw)?);
                            Ok((key.clone(), body))
                        })
                        .collect::<Result<Vec<_>, RuntimeError>>()
                })
                .await?;
            self.cache_lock()?.body_decodes += decoded.len() as u64;
            for (key, body) in decoded {
                if publish {
                    self.cache_lock()?.put_body(key.clone(), body.clone());
                }
                bodies.insert(key.id, body);
            }
        }
        if bodies.len() != wanted.len() {
            return Err(corrupt());
        }
        Ok(bodies)
    }
    pub(super) async fn operation_on(
        &self,
        conn: &mut SqliteConnection,
        domain: &JournalDomain,
        session: &SessionId,
        operation: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        let packs = self
            .operation_packs(conn, &(domain.clone(), session.clone()))
            .await?;
        Ok(packs
            .into_values()
            .flatten()
            .find(|o| &o.operation == operation))
    }
    #[cfg(feature = "test-support")]
    pub fn cache_stats(&self) -> Result<JournalCacheStats, RuntimeError> {
        let c = self.cache_lock()?;
        Ok(JournalCacheStats {
            charged_bytes: c.charge,
            budget: c.budget,
            index_decodes: c.index_decodes,
            body_decodes: c.body_decodes,
            pack_queries: c.pack_queries,
            directory_charged_bytes: c.directory_charge(),
            hits: c.hits,
            misses: c.misses,
            evictions: c.evictions,
            oversize: c.oversize,
        })
    }
    #[cfg(feature = "test-support")]
    pub fn cached_body_packs(&self, session: &SessionId) -> Result<Vec<(i64, u64)>, RuntimeError> {
        Ok(self.cache_lock()?.body_packs(session))
    }
    #[cfg(feature = "test-support")]
    pub fn set_cache_budget(&self, budget: usize) -> Result<(), RuntimeError> {
        if budget > DEFAULT_BUDGET {
            return Err(corrupt());
        }
        self.cache_lock()?.resize(budget);
        Ok(())
    }
}
#[cfg(feature = "test-support")]
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct JournalCacheStats {
    pub charged_bytes: usize,
    pub budget: usize,
    pub index_decodes: u64,
    pub body_decodes: u64,
    pub pack_queries: u64,
    pub directory_charged_bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub oversize: u64,
}

pub(super) fn validate_bitmap(bits: &[u8], count: usize) -> Result<(), RuntimeError> {
    if bits.len() != count.div_ceil(8)
        || (count % 8 != 0 && bits.last().is_some_and(|b| b >> (count % 8) != 0))
    {
        return Err(corrupt());
    }
    Ok(())
}

#[async_trait]
impl JournalStorage for ForgeJournalStorage {
    fn collection_capability(&self) -> JournalCollectionCapability {
        JournalCollectionCapability::Bounded(super::collection::LIMITS)
    }
    async fn collection_page(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        request: JournalCollectionPageRequest,
    ) -> Result<JournalCollectionPage, RuntimeError> {
        self.bounded_page(d, s, request).await
    }
    async fn prepare_collection_apply(
        &self,
        plan: &JournalCollectionPlan,
    ) -> Result<JournalCollectionPreflight, RuntimeError> {
        let mut tx = self.db.pool().begin().await.map_err(database_error)?;
        let result = self
            .collection_work(&mut tx, plan)
            .await?
            .map(|work| JournalCollectionPreflight::Fits { units: work.units });
        tx.commit().await.map_err(database_error)?;
        Ok(result.unwrap_or(JournalCollectionPreflight::Split {
            max_release: (plan.write.release.len() / 2) as u64,
        }))
    }
    async fn apply_collection(
        &self,
        plan: JournalCollectionPlan,
    ) -> Result<JournalCollectionReceipt, RuntimeError> {
        self.apply_bounded(plan.write, Some(plan.max_units)).await
    }
    fn sharing(&self) -> JournalBodySharing {
        JournalBodySharing::PerSession
    }
    async fn retained_bytes(&self, domain: &JournalDomain) -> Result<Option<u64>, RuntimeError> {
        Ok(Some(self.byte_stats(domain).await?.retained_bytes))
    }

    async fn load(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("storage.load", 0);
        #[cfg(feature = "test-support")]
        if domain.role == JournalRole::Protected
            && self
                .failed_commit_observation
                .compare_exchange(
                    2,
                    0,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .is_ok()
        {
            return Err(
                RuntimeError::internal("injected acceptance observation failure").retryable(),
            );
        }
        let mut tx = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN", 0);
            self.db.pool().begin().await
        }
        .map_err(database_error)?;
        let result = self.load_on(&mut tx, domain, session).await?;
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
            tx.commit().await
        }
        .map_err(database_error)?;
        Ok(result)
    }
    async fn objects(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("storage.objects", 0);
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let mut tx = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN", 0);
            self.db.pool().begin().await
        }
        .map_err(database_error)?;
        let views = self
            .directory(&mut tx, &(domain.clone(), session.clone()), true)
            .await?;
        let prefixes: Vec<_> = ids.iter().map(prefix).collect();
        let mut wanted = BTreeSet::new();
        for p in &prefixes {
            for v in &views {
                if v.index
                    .directory
                    .prefix_candidates(p)
                    .any(|slot| v.held(slot))
                {
                    wanted.insert(v.index.key.id);
                }
            }
        }
        let bodies = self.bodies(&mut tx, &views, &wanted, true).await?;
        let mut results = Vec::with_capacity(ids.len());
        for (id, p) in ids.iter().zip(&prefixes) {
            let mut found = None;
            for v in &views {
                for slot in v
                    .index
                    .directory
                    .prefix_candidates(p)
                    .filter(|s| v.held(*s))
                {
                    let object = bodies
                        .get(&v.index.key.id)
                        .and_then(|p| p.get(slot))
                        .ok_or_else(corrupt)?;
                    if object.id == *id {
                        if found.is_some() {
                            return Err(corrupt());
                        }
                        found = Some(object.bytes.clone());
                    }
                }
            }
            results.push(found);
        }
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
            tx.commit().await
        }
        .map_err(database_error)?;
        Ok(results)
    }
    async fn members(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("storage.members", 0);
        let mut tx = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN", 0);
            self.db.pool().begin().await
        }
        .map_err(database_error)?;
        let views = self
            .directory(&mut tx, &(domain.clone(), session.clone()), true)
            .await?;
        let wanted = views
            .iter()
            .filter(|v| v.bitmap.iter().any(|b| *b != 0))
            .map(|v| v.index.key.id)
            .collect();
        let bodies = self.bodies(&mut tx, &views, &wanted, true).await?;
        let mut members = BTreeSet::new();
        for v in &views {
            if let Some(pack) = bodies.get(&v.index.key.id) {
                for (slot, o) in pack.iter().enumerate() {
                    if v.held(slot) && !members.insert(o.id.clone()) {
                        return Err(corrupt());
                    }
                }
            }
        }
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
            tx.commit().await
        }
        .map_err(database_error)?;
        Ok(members.into_iter().collect())
    }
    async fn operation(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        operation: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("storage.operation", 0);
        let mut tx = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN", 0);
            self.db.pool().begin().await
        }
        .map_err(database_error)?;
        let result = self
            .operation_on(&mut tx, domain, session, operation)
            .await?;
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
            tx.commit().await
        }
        .map_err(database_error)?;
        Ok(result)
    }
    async fn pins(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Vec<JournalPinRecord>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("storage.pins", 0);
        let mut tx = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN", 0);
            self.db.pool().begin().await
        }
        .map_err(database_error)?;
        let rows={ #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0); sqlx::query("SELECT * FROM agent_session_journal_pin WHERE storage_id=? AND role=? AND runtime_session_id=?")
            .bind(domain.storage_id.as_str()).bind(role(domain)).bind(session.as_str()).fetch_all(&mut *tx).await }.map_err(database_error)?;
        let mut pins = Vec::new();
        for row in rows {
            pins.push(self.pin_row(domain, session, row).await?);
        }
        pins.sort_by(|a, b| a.pin.cmp(&b.pin));
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
            tx.commit().await
        }
        .map_err(database_error)?;
        Ok(pins)
    }
    async fn catalog(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        head: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("storage.catalog", 0);
        let mut tx = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN", 0);
            self.db.pool().begin().await
        }
        .map_err(database_error)?;
        let key = self
            .keys
            .lookup(Unit::Catalog, domain, session, &head.to_bytes());
        let row={ #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0); sqlx::query("SELECT * FROM agent_session_journal_catalog WHERE storage_id=? AND role=? AND runtime_session_id=? AND head_key=?")
            .bind(domain.storage_id.as_str()).bind(role(domain)).bind(session.as_str()).bind(&key).fetch_optional(&mut *tx).await }.map_err(database_error)?;
        let result = if let Some(row) = row {
            self.revision(&row)?;
            let keys = self.keys.clone();
            let domain = domain.clone();
            let session = session.clone();
            let head = head.clone();
            Some(
                self.work(move || {
                    let owner: String = row.try_get("forge_session_id").map_err(|_| corrupt())?;
                    let raw = keys.open(
                        &Binding {
                            unit: Unit::Catalog,
                            domain: Some(&domain),
                            session: &session,
                            owner: &owner,
                            row: &key,
                            generation: 0,
                        },
                        &row.try_get::<Vec<u8>, _>("nonce").map_err(|_| corrupt())?,
                        &row.try_get::<Vec<u8>, _>("payload")
                            .map_err(|_| corrupt())?,
                        wire::MAX_METADATA,
                    )?;
                    let entry = wire::catalog(&raw)?;
                    if entry.head != head {
                        return Err(corrupt());
                    }
                    Ok(entry)
                })
                .await?,
            )
        } else {
            None
        };
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
            tx.commit().await
        }
        .map_err(database_error)?;
        Ok(result)
    }
    async fn dirty_sessions(
        &self,
        domain: &JournalDomain,
        after: Option<&SessionId>,
        limit: usize,
    ) -> Result<Vec<SessionId>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("storage.dirty_sessions", 0);
        if limit == 0 {
            return Ok(vec![]);
        }
        let mut tx = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN", 0);
            self.db.pool().begin().await
        }
        .map_err(database_error)?;
        let rows={ #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0); sqlx::query("SELECT runtime_session_id FROM agent_session_journal_session WHERE storage_id=? AND role=? AND collection_dirty=1 AND (? IS NULL OR runtime_session_id>?) ORDER BY runtime_session_id LIMIT ?")
            .bind(domain.storage_id.as_str()).bind(role(domain)).bind(after.map(SessionId::as_str)).bind(after.map(SessionId::as_str)).bind(i64::try_from(limit).unwrap_or(i64::MAX)).fetch_all(&mut *tx).await }.map_err(database_error)?;
        let mut sessions = Vec::new();
        for row in rows {
            let s = SessionId::new(
                row.try_get::<String, _>("runtime_session_id")
                    .map_err(|_| corrupt())?,
            );
            // Discovery uses only the clear dirty index. The engine loads and
            // authenticates each id once, isolating an unreadable session.
            sessions.push(s);
        }
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
            tx.commit().await
        }
        .map_err(database_error)?;
        Ok(sessions)
    }
    async fn apply(&self, write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        self.apply_write(write).await
    }
    async fn compact(&self, domain: &JournalDomain) -> Result<(), RuntimeError> {
        // Earlier per-session batches may already have committed. Returned
        // domain-wide errors must not claim that the whole operation had no
        // effect; physical maintenance is safe to retry from the beginning.
        self.compact_domain(domain)
            .await
            .map_err(RuntimeError::retryable)
    }
}

#[cfg(test)]
mod error_contract_tests {
    use super::*;
    use sqlx::Connection;
    use std::time::Duration;

    #[tokio::test]
    async fn sqlite_busy_is_retryable_and_rolls_back() {
        {
            let temp = tempfile::tempdir().unwrap();
            let options = sqlx::sqlite::SqliteConnectOptions::new()
                .filename(temp.path().join("contention.db"))
                .create_if_missing(true)
                .shared_cache(false)
                .busy_timeout(Duration::ZERO);
            let mut owner = sqlx::SqliteConnection::connect_with(&options)
                .await
                .unwrap();
            let mut other = sqlx::SqliteConnection::connect_with(&options)
                .await
                .unwrap();
            sqlx::query("CREATE TABLE contention (id INTEGER)")
                .execute(&mut owner)
                .await
                .unwrap();
            sqlx::query("BEGIN IMMEDIATE")
                .execute(&mut owner)
                .await
                .unwrap();
            sqlx::query("INSERT INTO contention VALUES (1)")
                .execute(&mut owner)
                .await
                .unwrap();
            let error = tokio::time::timeout(
                Duration::from_secs(2),
                sqlx::query("INSERT INTO contention VALUES (2)").execute(&mut other),
            )
            .await
            .expect("busy timeout is disabled")
            .unwrap_err();
            let code = error
                .as_database_error()
                .unwrap()
                .code()
                .unwrap()
                .parse::<i32>()
                .unwrap();
            assert_eq!(code & 0xff, 5);
            assert!(database_error(error).retryable);
            sqlx::query("ROLLBACK").execute(&mut owner).await.unwrap();
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM contention")
                .fetch_one(&mut other)
                .await
                .unwrap();
            assert_eq!(count, 0);
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("private database diagnostic")]
    struct CodedError(&'static str);
    impl sqlx::error::DatabaseError for CodedError {
        fn message(&self) -> &str {
            "private database diagnostic"
        }
        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            Some(self.0.into())
        }
        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }
    #[test]
    fn sqlite_primary_and_extended_contention_codes_are_retryable() {
        for code in ["5", "261", "517", "6", "262", "518"] {
            assert!(
                database_error(sqlx::Error::Database(Box::new(CodedError(code)))).retryable,
                "{code}"
            );
        }
        assert!(!database_error(sqlx::Error::Database(Box::new(CodedError("19")))).retryable);
    }

    #[test]
    fn ambiguous_acknowledgements_and_availability_are_retryable() {
        assert!(!database_error(sqlx::Error::RowNotFound).retryable);
        assert!(!database_error(sqlx::Error::Protocol("private SQL data".into())).retryable);
        assert!(
            ambiguous_database_error(sqlx::Error::Protocol("private SQL data".into())).retryable
        );
        for error in [
            sqlx::Error::PoolClosed,
            sqlx::Error::PoolTimedOut,
            sqlx::Error::WorkerCrashed,
            sqlx::Error::Io(std::io::Error::other("private IO data")),
        ] {
            let mapped = database_error(error);
            assert!(mapped.retryable);
            assert_eq!(mapped.message, "journal database operation failed");
        }
    }
}
