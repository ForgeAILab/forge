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
    pub(super) repack: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(feature = "test-support")]
    pub(super) compact_observations: Arc<Mutex<(u64, usize)>>,
}
impl fmt::Debug for ForgeJournalStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForgeJournalStorage")
            .field("key_revision", &self.keys.revision)
            .finish_non_exhaustive()
    }
}
#[derive(Clone)]
pub(super) struct PackView {
    pub index: IndexEntry,
    pub bitmap: Vec<u8>,
}
impl PackView {
    pub fn held(&self, slot: usize) -> bool {
        self.bitmap
            .get(slot / 8)
            .is_some_and(|b| b & (1 << (slot % 8)) != 0)
    }
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
pub(super) fn database_error(_: impl fmt::Display) -> RuntimeError {
    // sqlx errors may include bound data; only neutral diagnostics escape.
    RuntimeError::internal("journal database operation failed")
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
            repack: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            #[cfg(feature = "test-support")]
            compact_observations: Arc::new(Mutex::new((0, 0))),
        })
    }
    pub(super) fn cache_lock(&self) -> Result<MutexGuard<'_, Cache>, RuntimeError> {
        self.cache
            .lock()
            .map_err(|_| RuntimeError::internal("journal cache unavailable"))
    }
    pub(super) async fn work<T: Send + 'static>(
        &self,
        job: impl FnOnce() -> Result<T, RuntimeError> + Send + 'static,
    ) -> Result<T, RuntimeError> {
        let permit = self
            .cpu
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| corrupt())?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            job()
        })
        .await
        .map_err(|_| RuntimeError::internal("journal codec worker failed"))?
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
                    domain: &domain,
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
    pub(super) async fn load_on(
        &self,
        conn: &mut SqliteConnection,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        let row=sqlx::query("SELECT * FROM agent_session_journal_session WHERE storage_id=? AND role=? AND runtime_session_id=?")
            .bind(domain.storage_id.as_str()).bind(role(domain)).bind(session.as_str()).fetch_optional(&mut *conn).await.map_err(database_error)?;
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
        let (domain, session) = scope;
        let rows=sqlx::query("SELECT p.pack_id,p.generation,p.object_count,p.retained_canonical_bytes,p.forge_session_id,p.key_revision,p.format,m.bitmap FROM agent_session_journal_pack p LEFT JOIN agent_session_journal_membership m ON m.pack_id=p.pack_id AND m.storage_id=p.storage_id AND m.role=p.role AND m.runtime_session_id=p.runtime_session_id AND m.forge_session_id=p.forge_session_id WHERE p.storage_id=? AND p.role=? AND p.runtime_session_id=? ORDER BY p.pack_id")
            .bind(domain.storage_id.as_str()).bind(role(domain)).bind(session.as_str()).fetch_all(&mut *conn).await.map_err(database_error)?;
        let cached = match prior {
            Some(p) => p,
            None => self.cache_lock()?.directory(scope).unwrap_or_default(),
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
            let bitmap: Option<Vec<u8>> = row.try_get("bitmap").map_err(|_| corrupt())?;
            let bitmap = bitmap.unwrap_or_else(|| vec![0; count.div_ceil(8)]);
            validate_bitmap(&bitmap, count)?;
            let key = PackKey {
                scope: scope.clone(),
                id,
                generation: counter(&row, "generation")?,
                count,
                retained,
                owner: row.try_get("forge_session_id").map_err(|_| corrupt())?,
            };
            if !cached.packs.get(&id).is_some_and(|p| p.key == key) {
                missing.push(id);
            }
            headers.insert(id, (key, bitmap));
        }
        let mut indexes = BTreeMap::new();
        for chunk in missing.chunks(400) {
            let mut q = QueryBuilder::<Sqlite>::new(
                "SELECT pack_id,index_nonce,index_payload FROM agent_session_journal_pack WHERE storage_id=",
            );
            q.push_bind(domain.storage_id.as_str())
                .push(" AND role=")
                .push_bind(role(domain))
                .push(" AND runtime_session_id=")
                .push_bind(session.as_str())
                .push(" AND pack_id IN (");
            let mut separated = q.separated(",");
            for id in chunk {
                separated.push_bind(*id);
            }
            separated.push_unseparated(")");
            let rows = q
                .build()
                .fetch_all(&mut *conn)
                .await
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
                                    domain: &key.scope.0,
                                    session: &key.scope.1,
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
        for (id, (key, bitmap)) in headers {
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
            snapshot.push(PackView { index, bitmap });
        }
        if publish {
            self.cache_lock()?
                .put_directory(scope.clone(), Arc::new(next));
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
            let rows = q
                .build()
                .fetch_all(&mut *conn)
                .await
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
                                    domain: &key.scope.0,
                                    session: &key.scope.1,
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
        })
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
#[derive(Debug, Clone, Copy)]
pub struct JournalCacheStats {
    pub charged_bytes: usize,
    pub budget: usize,
    pub index_decodes: u64,
    pub body_decodes: u64,
    pub pack_queries: u64,
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
    fn sharing(&self) -> JournalBodySharing {
        JournalBodySharing::PerSession
    }
    async fn retained_bytes(&self, domain: &JournalDomain) -> Result<Option<u64>, RuntimeError> {
        let mut tx = self.db.pool().begin().await.map_err(database_error)?;
        let mut after = 0i64;
        let mut total = 0u64;
        loop {
            // This domain-wide diagnostic authenticates scalar counts against
            // index units in bounded pages. It never opens body units or builds
            // a global directory, including for orphan/deferred packs.
            let rows=sqlx::query("SELECT pack_id,runtime_session_id,forge_session_id,generation,object_count,retained_canonical_bytes,key_revision,format,index_nonce,index_payload FROM agent_session_journal_pack WHERE storage_id=? AND role=? AND pack_id>? ORDER BY pack_id LIMIT 400")
                .bind(domain.storage_id.as_str()).bind(role(domain)).bind(after).fetch_all(&mut *tx).await.map_err(database_error)?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                self.revision(row)?;
            }
            let count = rows.len() as u64;
            let keys = self.keys.clone();
            let domain = domain.clone();
            let (last, bytes) = self
                .work(move || {
                    let mut last = 0;
                    let mut bytes = 0u64;
                    for row in rows {
                        let id: i64 = row.try_get("pack_id").map_err(|_| corrupt())?;
                        let session = SessionId::new(
                            row.try_get::<String, _>("runtime_session_id")
                                .map_err(|_| corrupt())?,
                        );
                        let owner: String =
                            row.try_get("forge_session_id").map_err(|_| corrupt())?;
                        let generation = counter(&row, "generation")?;
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
                        let raw = keys.open(
                            &Binding {
                                unit: Unit::PackIndex,
                                domain: &domain,
                                session: &session,
                                owner: &owner,
                                row: &id.to_be_bytes(),
                                generation,
                            },
                            &row.try_get::<Vec<u8>, _>("index_nonce")
                                .map_err(|_| corrupt())?,
                            &row.try_get::<Vec<u8>, _>("index_payload")
                                .map_err(|_| corrupt())?,
                            codec::MAX_INDEX_BYTES,
                        )?;
                        codec::decode_index(&raw, count, retained)?;
                        bytes = bytes.checked_add(retained as u64).ok_or_else(corrupt)?;
                        last = id;
                    }
                    Ok((last, bytes))
                })
                .await?;
            self.cache_lock()?.index_decodes += count;
            after = last;
            total = total.checked_add(bytes).ok_or_else(corrupt)?;
        }
        tx.commit().await.map_err(database_error)?;
        Ok(Some(total))
    }
    async fn load(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        let mut tx = self.db.pool().begin().await.map_err(database_error)?;
        let result = self.load_on(&mut tx, domain, session).await?;
        tx.commit().await.map_err(database_error)?;
        Ok(result)
    }
    async fn objects(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let mut tx = self.db.pool().begin().await.map_err(database_error)?;
        let views = self
            .directory(&mut tx, &(domain.clone(), session.clone()), true)
            .await?;
        let mut wanted = BTreeSet::new();
        for id in ids {
            for v in &views {
                if v.index.directory.candidates(id).any(|slot| v.held(slot)) {
                    wanted.insert(v.index.key.id);
                }
            }
        }
        let bodies = self.bodies(&mut tx, &views, &wanted, true).await?;
        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
            let mut found = None;
            for v in &views {
                for slot in v.index.directory.candidates(id).filter(|s| v.held(*s)) {
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
        tx.commit().await.map_err(database_error)?;
        Ok(results)
    }
    async fn members(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        let mut tx = self.db.pool().begin().await.map_err(database_error)?;
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
        tx.commit().await.map_err(database_error)?;
        Ok(members.into_iter().collect())
    }
    async fn operation(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        operation: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        let mut tx = self.db.pool().begin().await.map_err(database_error)?;
        let result = self
            .operation_on(&mut tx, domain, session, operation)
            .await?;
        tx.commit().await.map_err(database_error)?;
        Ok(result)
    }
    async fn pins(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
    ) -> Result<Vec<JournalPinRecord>, RuntimeError> {
        let mut tx = self.db.pool().begin().await.map_err(database_error)?;
        let rows=sqlx::query("SELECT * FROM agent_session_journal_pin WHERE storage_id=? AND role=? AND runtime_session_id=?")
            .bind(domain.storage_id.as_str()).bind(role(domain)).bind(session.as_str()).fetch_all(&mut *tx).await.map_err(database_error)?;
        let mut pins = Vec::new();
        for row in rows {
            self.revision(&row)?;
            let keys = self.keys.clone();
            let domain = domain.clone();
            let session = session.clone();
            pins.push(
                self.work(move || {
                    let owner: String = row.try_get("forge_session_id").map_err(|_| corrupt())?;
                    let key: Vec<u8> = row.try_get("pin_key").map_err(|_| corrupt())?;
                    let raw = keys.open(
                        &Binding {
                            unit: Unit::Pin,
                            domain: &domain,
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
                .await?,
            );
        }
        pins.sort_by(|a, b| a.pin.cmp(&b.pin));
        tx.commit().await.map_err(database_error)?;
        Ok(pins)
    }
    async fn catalog(
        &self,
        domain: &JournalDomain,
        session: &SessionId,
        head: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError> {
        let mut tx = self.db.pool().begin().await.map_err(database_error)?;
        let key = self
            .keys
            .lookup(Unit::Catalog, domain, session, &head.to_bytes());
        let row=sqlx::query("SELECT * FROM agent_session_journal_catalog WHERE storage_id=? AND role=? AND runtime_session_id=? AND head_key=?")
            .bind(domain.storage_id.as_str()).bind(role(domain)).bind(session.as_str()).bind(&key).fetch_optional(&mut *tx).await.map_err(database_error)?;
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
                            domain: &domain,
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
        tx.commit().await.map_err(database_error)?;
        Ok(result)
    }
    async fn dirty_sessions(
        &self,
        domain: &JournalDomain,
        after: Option<&SessionId>,
        limit: usize,
    ) -> Result<Vec<SessionId>, RuntimeError> {
        if limit == 0 {
            return Ok(vec![]);
        }
        let mut tx = self.db.pool().begin().await.map_err(database_error)?;
        let rows=sqlx::query("SELECT * FROM agent_session_journal_session WHERE storage_id=? AND role=? AND collection_dirty=1 AND (? IS NULL OR runtime_session_id>?) ORDER BY runtime_session_id LIMIT ?")
            .bind(domain.storage_id.as_str()).bind(role(domain)).bind(after.map(SessionId::as_str)).bind(after.map(SessionId::as_str)).bind(i64::try_from(limit).unwrap_or(i64::MAX)).fetch_all(&mut *tx).await.map_err(database_error)?;
        let mut sessions = Vec::new();
        for row in rows {
            let s = SessionId::new(
                row.try_get::<String, _>("runtime_session_id")
                    .map_err(|_| corrupt())?,
            );
            self.record_row(domain, &s, row).await?;
            sessions.push(s);
        }
        tx.commit().await.map_err(database_error)?;
        Ok(sessions)
    }
    async fn apply(&self, write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        self.apply_write(write).await
    }
    async fn compact(&self, domain: &JournalDomain) -> Result<(), RuntimeError> {
        self.compact_domain(domain).await
    }
}
