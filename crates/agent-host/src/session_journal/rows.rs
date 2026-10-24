//! Row encoders shared by atomic apply and explicit test fixtures.
#[cfg(feature = "test-support")]
use super::storage::ambiguous_database_error;
use super::{
    cache::{PackKey, Scope},
    codec, conflict, corrupt,
    seal::{Binding, FORMAT, Sealed, Unit, role},
    storage::{ForgeJournalStorage, PackView, Staged, database_error, full_bitmap, prefix},
    wire,
};
use agent_runtime::core::{error::RuntimeError, ids::SessionId, journal::*};
use sqlx::{QueryBuilder, Sqlite, SqliteConnection};
use std::{collections::BTreeSet, sync::Arc};

impl ForgeJournalStorage {
    /// Seal a domain-bound metadata unit of `scope`.
    pub(super) async fn seal_row(
        &self,
        scope: &Scope,
        owner: &str,
        unit: Unit,
        row: &[u8],
        generation: u64,
        raw: Vec<u8>,
    ) -> Result<Sealed, RuntimeError> {
        self.seal_unit(Some(&scope.0), &scope.1, owner, unit, row, generation, raw)
            .await
    }
    /// Seal a unit; pack units pass no domain (session-scoped AAD).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn seal_unit(
        &self,
        domain: Option<&JournalDomain>,
        session: &SessionId,
        owner: &str,
        unit: Unit,
        row: &[u8],
        generation: u64,
        raw: Vec<u8>,
    ) -> Result<Sealed, RuntimeError> {
        let keys = self.keys.clone();
        let domain = domain.cloned();
        let session = session.clone();
        let owner = owner.to_owned();
        let row = row.to_vec();
        self.work(move || {
            keys.seal(
                &Binding {
                    unit,
                    domain: domain.as_ref(),
                    session: &session,
                    owner: &owner,
                    row: &row,
                    generation,
                },
                &raw,
            )
        })
        .await
    }
    pub(super) async fn mapping(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
    ) -> Result<String, RuntimeError> {
        let ids: Vec<String> = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0);
            sqlx::query_scalar(
                "SELECT id FROM agent_session WHERE runtime_session_id=? ORDER BY id LIMIT 2",
            )
            .bind(scope.1.as_str())
            .fetch_all(&mut *conn)
            .await
        }
        .map_err(database_error)?;
        let [owner] = ids.as_slice() else {
            return Err(RuntimeError::internal(
                "journal host session mapping missing or ambiguous",
            ));
        };
        let stored:Option<String>={ #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0); sqlx::query_scalar("SELECT forge_session_id FROM agent_session_journal_session WHERE storage_id=? AND role=? AND runtime_session_id=?")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).fetch_optional(&mut *conn).await }.map_err(database_error)?;
        if stored.as_ref().is_some_and(|id| id != owner) {
            return Err(RuntimeError::internal(
                "journal host session mapping changed",
            ));
        }
        Ok(owner.clone())
    }
    pub(super) async fn insert_record(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        record: &JournalSessionRecord,
    ) -> Result<(), RuntimeError> {
        let sealed = self
            .seal_row(
                scope,
                owner,
                Unit::Session,
                b"",
                record.generation,
                wire::record_bytes(record)?,
            )
            .await?;
        { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.INSERT", 0); sqlx::query("INSERT INTO agent_session_journal_session (storage_id,role,runtime_session_id,forge_session_id,generation,revision,writer_epoch,collection_dirty,published,has_checkpoint,retired,key_revision,format,nonce,payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(owner)
            .bind(record.generation.to_be_bytes().as_slice()).bind(record.revision.0.to_be_bytes().as_slice()).bind(record.writer_epoch.to_be_bytes().as_slice())
            .bind(record.collection_dirty).bind(record.head.is_some()).bind(record.has_checkpoint).bind(record.retirement.is_some())
            .bind(self.keys.revision).bind(FORMAT).bind(sealed.nonce).bind(sealed.payload).execute(&mut *conn).await }.map_err(database_error)?;
        Ok(())
    }
    pub(super) async fn replace_record(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        record: &JournalSessionRecord,
        expect: JournalFence,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("rows.replace_record", 0);
        let sealed = self
            .seal_row(
                scope,
                owner,
                Unit::Session,
                b"",
                record.generation,
                wire::record_bytes(record)?,
            )
            .await?;
        let changed={ #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0); sqlx::query("UPDATE agent_session_journal_session SET generation=?,revision=?,writer_epoch=?,collection_dirty=?,published=?,has_checkpoint=?,retired=?,key_revision=?,format=?,nonce=?,payload=? WHERE storage_id=? AND role=? AND runtime_session_id=? AND forge_session_id=? AND revision=? AND writer_epoch=? AND generation=?")
            .bind(record.generation.to_be_bytes().as_slice()).bind(record.revision.0.to_be_bytes().as_slice()).bind(record.writer_epoch.to_be_bytes().as_slice())
            .bind(record.collection_dirty).bind(record.head.is_some()).bind(record.has_checkpoint).bind(record.retirement.is_some())
            .bind(self.keys.revision).bind(FORMAT).bind(sealed.nonce).bind(sealed.payload)
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(owner)
            .bind(expect.revision.0.to_be_bytes().as_slice()).bind(expect.writer_epoch.to_be_bytes().as_slice()).bind(expect.generation.to_be_bytes().as_slice())
            .execute(&mut *conn).await }.map_err(database_error)?.rows_affected();
        if changed != 1 {
            return Err(conflict());
        }
        Ok(())
    }
    pub(super) async fn put_bits(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        id: i64,
        bits: &[u8],
    ) -> Result<(), RuntimeError> {
        if bits.iter().any(|b| *b != 0) {
            { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.INSERT", 0); sqlx::query("INSERT INTO agent_session_journal_membership (storage_id,role,runtime_session_id,forge_session_id,pack_id,bitmap) VALUES (?,?,?,?,?,?) ON CONFLICT(storage_id,role,runtime_session_id,pack_id) DO UPDATE SET bitmap=excluded.bitmap")
                .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(owner).bind(id).bind(bits).execute(&mut *conn).await }.map_err(database_error)?;
        } else {
            { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.DELETE", 0); sqlx::query("DELETE FROM agent_session_journal_membership WHERE storage_id=? AND role=? AND runtime_session_id=? AND pack_id=?")
                .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(id).execute(&mut *conn).await }.map_err(database_error)?;
        }
        Ok(())
    }
    /// A new pack held by exactly the writing domain.
    pub(super) async fn insert_pack(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        objects: Vec<JournalStoredObject>,
        staged: &mut Staged,
    ) -> Result<i64, RuntimeError> {
        let count = objects.len();
        self.insert_session_pack(
            conn,
            &scope.1,
            owner,
            Arc::new(objects),
            vec![(scope.0.clone(), full_bitmap(count))],
            staged,
        )
        .await
    }
    /// Insert a session-owned pack with each holder domain's own bitmap. A
    /// holder whose bitmap is empty gets no membership row.
    pub(super) async fn insert_session_pack(
        &self,
        conn: &mut SqliteConnection,
        session: &SessionId,
        owner: &str,
        objects: Arc<Vec<JournalStoredObject>>,
        holders: Vec<(JournalDomain, Vec<u8>)>,
        staged: &mut Staged,
    ) -> Result<i64, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("rows.insert_pack", 0);
        let encode = objects.clone();
        let encoded = self.work(move || codec::encode_validated(&encode)).await?;
        // Reserve the AUTOINCREMENT identity inside this transaction; this
        // placeholder is replaced by authenticated units before any commit.
        let id:i64={ #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.INSERT", 0); sqlx::query_scalar("INSERT INTO agent_session_journal_pack (runtime_session_id,forge_session_id,generation,object_count,retained_canonical_bytes,key_revision,format,index_nonce,index_payload,body_nonce,body_payload) VALUES (?,?,?, ?,?,?,?,zeroblob(24),zeroblob(16),zeroblob(24),zeroblob(16)) RETURNING pack_id")
            .bind(session.as_str()).bind(owner).bind(0u64.to_be_bytes().as_slice())
            .bind(encoded.count as i64).bind(encoded.canonical_bytes as i64).bind(self.keys.revision).bind(FORMAT).fetch_one(&mut *conn).await }.map_err(database_error)?;
        let index = self
            .seal_unit(
                None,
                session,
                owner,
                Unit::PackIndex,
                &id.to_be_bytes(),
                0,
                encoded.index,
            )
            .await?;
        let body = self
            .seal_unit(
                None,
                session,
                owner,
                Unit::PackBody,
                &id.to_be_bytes(),
                0,
                encoded.body,
            )
            .await?;
        { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0); sqlx::query("UPDATE agent_session_journal_pack SET index_nonce=?,index_payload=?,body_nonce=?,body_payload=? WHERE pack_id=?")
            .bind(index.nonce).bind(index.payload).bind(body.nonce).bind(body.payload).bind(id).execute(&mut *conn).await }.map_err(database_error)?;
        if holders.iter().all(|(_, bits)| bits.iter().all(|b| *b == 0)) {
            return Err(corrupt());
        }
        for (domain, bits) in holders {
            super::storage::validate_bitmap(&bits, encoded.count)?;
            self.put_bits(conn, &(domain, session.clone()), owner, id, &bits)
                .await?;
        }
        staged.written.insert(
            id,
            (
                PackKey {
                    session: session.clone(),
                    id,
                    generation: 0,
                    count: encoded.count,
                    retained: encoded.canonical_bytes,
                    owner: owner.to_owned(),
                },
                objects,
            ),
        );
        Ok(id)
    }
    /// Compare only prefix candidates in every pack of the session, including
    /// deferred slots and slots only another domain holds. New bodies are
    /// returned to the caller's staged pack; an identical stored body gains the
    /// writing domain's bit after a full byte comparison.
    pub(super) async fn admit(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        views: &mut [PackView],
        objects: &[JournalStoredObject],
    ) -> Result<Vec<JournalStoredObject>, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("rows.admit", 0);
        if views.iter().any(|v| v.index.key.owner != owner) {
            return Err(RuntimeError::internal("journal pack owner mapping changed"));
        }
        let prefixes: Vec<_> = objects.iter().map(|o| prefix(&o.id)).collect();
        let wanted = prefixes
            .iter()
            .flat_map(|p| {
                views
                    .iter()
                    .filter(|v| !v.index.directory.prefix_candidates(p).is_empty())
                    .map(|v| v.index.key.id)
            })
            .collect::<BTreeSet<_>>();
        let bodies = self.bodies(conn, views, &wanted, false).await?;
        let mut changed = BTreeSet::new();
        let mut new = Vec::new();
        for (object, p) in objects.iter().zip(&prefixes) {
            let mut found = false;
            for view in &mut *views {
                for slot in view.index.directory.prefix_candidates(p) {
                    let old = bodies
                        .get(&view.index.key.id)
                        .and_then(|p| p.get(slot))
                        .ok_or_else(corrupt)?;
                    if old.id == object.id {
                        if found || old.bytes != object.bytes || old.kind != object.kind {
                            return Err(conflict());
                        }
                        view.bitmap[slot / 8] |= 1 << (slot % 8);
                        changed.insert(view.index.key.id);
                        found = true;
                    }
                }
            }
            if !found {
                new.push(object.clone());
            }
        }
        for id in changed {
            let v = views
                .iter()
                .find(|v| v.index.key.id == id)
                .ok_or_else(corrupt)?;
            self.put_bits(conn, scope, owner, id, &v.bitmap).await?;
        }
        Ok(new)
    }
    pub(super) async fn pin_changes(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        changes: &[JournalPinChange],
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("rows.pin_changes", 0);
        let mut removals = Vec::new();
        for change in changes {
            let pin = match change {
                JournalPinChange::Put(p) => &p.pin,
                JournalPinChange::Remove(p) => p,
            };
            let key = self
                .keys
                .lookup(Unit::Pin, &scope.0, &scope.1, pin.as_str().as_bytes());
            match change {
                JournalPinChange::Put(pin) => {
                    // Removals before this Put keep their order.
                    self.remove_keyed(
                        conn,
                        "agent_session_journal_pin",
                        "pin_key",
                        scope,
                        &mut removals,
                    )
                    .await?;
                    let sealed = self
                        .seal_row(scope, owner, Unit::Pin, &key, 0, wire::pin_bytes(pin)?)
                        .await?;
                    { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.INSERT", 0); sqlx::query("INSERT INTO agent_session_journal_pin (storage_id,role,runtime_session_id,forge_session_id,pin_key,key_revision,format,nonce,payload) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(storage_id,role,runtime_session_id,pin_key) DO UPDATE SET key_revision=excluded.key_revision,format=excluded.format,nonce=excluded.nonce,payload=excluded.payload")
                        .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(owner).bind(key).bind(self.keys.revision).bind(FORMAT).bind(sealed.nonce).bind(sealed.payload).execute(&mut *conn).await }.map_err(database_error)?;
                }
                JournalPinChange::Remove(_) => removals.push(key),
            }
        }
        self.remove_keyed(
            conn,
            "agent_session_journal_pin",
            "pin_key",
            scope,
            &mut removals,
        )
        .await?;
        if !changes.is_empty() {
            self.rank_pins(conn, scope).await?;
        }
        Ok(())
    }
    /// One DELETE per chunk of keyed pin/catalog removals in this scope.
    async fn remove_keyed(
        &self,
        conn: &mut SqliteConnection,
        table: &'static str,
        column: &'static str,
        scope: &Scope,
        keys: &mut Vec<Vec<u8>>,
    ) -> Result<(), RuntimeError> {
        for chunk in keys.chunks(400) {
            let mut q =
                QueryBuilder::<Sqlite>::new(format!("DELETE FROM {table} WHERE storage_id="));
            q.push_bind(scope.0.storage_id.as_str())
                .push(" AND role=")
                .push_bind(role(&scope.0))
                .push(" AND runtime_session_id=")
                .push_bind(scope.1.as_str())
                .push(format!(" AND {column} IN ("));
            let mut separated = q.separated(",");
            for key in chunk {
                separated.push_bind(key.as_slice());
            }
            separated.push_unseparated(")");
            {
                #[cfg(feature = "test-support")]
                let _sql_profile = crate::session_journal::profile::timer("sql.DELETE", 0);
                q.build().execute(&mut *conn).await
            }
            .map_err(database_error)?;
        }
        keys.clear();
        Ok(())
    }
    pub(super) async fn catalog_changes(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        changes: &[JournalCatalogChange],
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("rows.catalog_changes", 0);
        let mut removals = Vec::new();
        for change in changes {
            let head = match change {
                JournalCatalogChange::Put(c) => &c.head,
                JournalCatalogChange::Remove(h) => h,
            };
            let key = self
                .keys
                .lookup(Unit::Catalog, &scope.0, &scope.1, &head.to_bytes());
            match change {
                JournalCatalogChange::Put(entry) => {
                    self.remove_keyed(
                        conn,
                        "agent_session_journal_catalog",
                        "head_key",
                        scope,
                        &mut removals,
                    )
                    .await?;
                    let sealed = self
                        .seal_row(
                            scope,
                            owner,
                            Unit::Catalog,
                            &key,
                            0,
                            wire::catalog_bytes(entry)?,
                        )
                        .await?;
                    { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.INSERT", 0); sqlx::query("INSERT INTO agent_session_journal_catalog (storage_id,role,runtime_session_id,forge_session_id,head_key,key_revision,format,nonce,payload) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(storage_id,role,runtime_session_id,head_key) DO UPDATE SET key_revision=excluded.key_revision,format=excluded.format,nonce=excluded.nonce,payload=excluded.payload")
                        .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(owner).bind(key).bind(self.keys.revision).bind(FORMAT).bind(sealed.nonce).bind(sealed.payload).execute(&mut *conn).await }.map_err(database_error)?;
                }
                JournalCatalogChange::Remove(_) => removals.push(key),
            }
        }
        self.remove_keyed(
            conn,
            "agent_session_journal_catalog",
            "head_key",
            scope,
            &mut removals,
        )
        .await
    }
    #[cfg(feature = "test-support")]
    pub(crate) async fn seed(&self, write: JournalWrite) -> Result<(), RuntimeError> {
        let scope = (write.domain.clone(), write.session.clone());
        let mut tx = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN_IMMEDIATE", 0);
            db::begin_immediate(self.db.pool()).await
        }
        .map_err(database_error)?;
        let owner = self.mapping(&mut tx, &scope).await?;
        let old = self.load_on(&mut tx, &scope.0, &scope.1).await?;
        if let Some(old) = old {
            self.replace_record(&mut tx, &scope, &owner, &write.record, old.fence())
                .await?;
        } else {
            self.insert_record(&mut tx, &scope, &owner, &write.record)
                .await?;
        }
        let mut views = self.directory(&mut tx, &scope, false).await?;
        for o in &write.objects.objects {
            codec::validate(o)?;
        }
        let new = self
            .admit(&mut tx, &scope, &owner, &mut views, &write.objects.objects)
            .await?;
        let mut staged = Staged::default();
        for pack in codec::split(new)? {
            self.insert_pack(&mut tx, &scope, &owner, pack, &mut staged)
                .await?;
        }
        self.pin_changes(&mut tx, &scope, &owner, &write.pins)
            .await?;
        self.catalog_changes(&mut tx, &scope, &owner, &write.catalog)
            .await?;
        if let Some(op) = &write.operation {
            self.append_operation(&mut tx, &scope, &owner, op).await?;
        }
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
            tx.commit().await
        }
        .map_err(ambiguous_database_error)?;
        self.cache_lock()
            .map_err(RuntimeError::retryable)?
            .invalidate(&scope.1);
        Ok(())
    }
    /// Test-only: open one sealed unit with its real binding, let `mutate`
    /// change the plaintext (decompressed for canonical bodies and operation
    /// packs), and reseal it under the same binding. AEAD then accepts the
    /// unit, so only the bounded decoders stand between the change and a read.
    #[cfg(feature = "test-support")]
    pub async fn reseal_unit_for_test(
        &self,
        domain: &JournalDomain,
        session: &agent_runtime::core::ids::SessionId,
        unit: super::TestSealedUnit,
        mutate: impl FnOnce(&mut Vec<u8>) + Send + 'static,
    ) -> Result<(), RuntimeError> {
        use super::TestSealedUnit as T;
        use sqlx::Row;
        let scope = (domain.clone(), session.clone());
        let (kind, id, nonce, payload, inflate) = match unit {
            T::PackIndex(id) => (Unit::PackIndex, id, "index_nonce", "index_payload", false),
            T::PackBody(id) => (Unit::PackBody, id, "body_nonce", "body_payload", false),
            T::PackBodyCanonical(id) => (Unit::PackBody, id, "body_nonce", "body_payload", true),
            T::Operations(tier) => (Unit::Operations, tier, "nonce", "payload", true),
            T::OperationsFrame(tier) => (Unit::Operations, tier, "nonce", "payload", false),
        };
        let (table, key) = match kind {
            Unit::Operations => ("agent_session_journal_operation_pack", "tier"),
            _ => ("agent_session_journal_pack", "pack_id"),
        };
        let mut tx = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN_IMMEDIATE", 0);
            db::begin_immediate(self.db.pool()).await
        }
        .map_err(database_error)?;
        // Pack units are session-owned; `domain` selects only operation packs.
        let scoped = matches!(kind, Unit::Operations);
        // Unused bind slots compare the domain with itself.
        let filter = if scoped {
            "storage_id=?1 AND role=?2 AND runtime_session_id=?3"
        } else {
            "?1=?1 AND ?2=?2 AND runtime_session_id=?3"
        };
        let row = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0);
            sqlx::query(&format!(
                "SELECT * FROM {table} WHERE {filter} AND {key}=?4"
            ))
            .bind(domain.storage_id.as_str())
            .bind(role(domain))
            .bind(session.as_str())
            .bind(id)
            .fetch_one(&mut *tx)
            .await
        }
        .map_err(database_error)?;
        let owner: String = row.try_get("forge_session_id").map_err(|_| corrupt())?;
        let generation = match kind {
            Unit::Operations => 0,
            _ => super::storage::counter(&row, "generation")?,
        };
        let row_key = id.to_be_bytes();
        let binding = Binding {
            unit: kind,
            domain: scoped.then_some(domain),
            session,
            owner: &owner,
            row: &row_key,
            generation,
        };
        let mut plain = self.keys.open(
            &binding,
            &row.try_get::<Vec<u8>, _>(nonce).map_err(|_| corrupt())?,
            &row.try_get::<Vec<u8>, _>(payload).map_err(|_| corrupt())?,
            usize::MAX,
        )?;
        if inflate {
            plain = zstd::stream::decode_all(plain.as_slice()).map_err(|_| corrupt())?;
        }
        mutate(&mut plain);
        if inflate {
            plain = zstd::stream::encode_all(plain.as_slice(), 3).map_err(|_| corrupt())?;
        }
        let sealed = self.keys.seal(&binding, &plain)?;
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0);
            sqlx::query(&format!(
                "UPDATE {table} SET {nonce}=?5,{payload}=?6 WHERE {filter} AND {key}=?4"
            ))
            .bind(domain.storage_id.as_str())
            .bind(role(domain))
            .bind(session.as_str())
            .bind(id)
            .bind(sealed.nonce)
            .bind(sealed.payload)
            .execute(&mut *tx)
            .await
        }
        .map_err(database_error)?;
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
            tx.commit().await
        }
        .map_err(ambiguous_database_error)?;
        self.cache_lock()
            .map_err(RuntimeError::retryable)?
            .invalidate(&scope.1);
        Ok(())
    }
    /// Test-only generation rewrite, without changing bodies or membership.
    #[cfg(feature = "test-support")]
    pub(crate) async fn reseal_scope(
        &self,
        domain: &JournalDomain,
        session: &agent_runtime::core::ids::SessionId,
    ) -> Result<(), RuntimeError> {
        let scope = (domain.clone(), session.clone());
        let mut tx = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN_IMMEDIATE", 0);
            db::begin_immediate(self.db.pool()).await
        }
        .map_err(database_error)?;
        let result=async {
            let views=self.directory(&mut tx,&scope,false).await?;
            let wanted=views.iter().map(|v|v.index.key.id).collect();
            let bodies=self.bodies(&mut tx,&views,&wanted,false).await?;
            for view in views {
                let key=&view.index.key;
                let generation=key.generation.checked_add(1).ok_or_else(corrupt)?;
                let objects=bodies.get(&key.id).ok_or_else(corrupt)?.clone();
                let encoded=self.work(move||codec::encode(&objects)).await?;
                let index=self.seal_unit(None,&scope.1,&key.owner,Unit::PackIndex,&key.id.to_be_bytes(),generation,encoded.index).await?;
                let body=self.seal_unit(None,&scope.1,&key.owner,Unit::PackBody,&key.id.to_be_bytes(),generation,encoded.body).await?;
                { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.UPDATE", 0); sqlx::query("UPDATE agent_session_journal_pack SET generation=?,index_nonce=?,index_payload=?,body_nonce=?,body_payload=? WHERE pack_id=?")
                    .bind(generation.to_be_bytes().as_slice()).bind(index.nonce).bind(index.payload).bind(body.nonce).bind(body.payload).bind(key.id).execute(&mut *tx).await }.map_err(database_error)?;
            }
            Ok::<_,RuntimeError>(())
        }.await;
        match result {
            Ok(()) => {
                #[cfg(feature = "test-support")]
                let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
                tx.commit().await
            }
            .map_err(ambiguous_database_error)?,
            Err(e) => {
                {
                    #[cfg(feature = "test-support")]
                    let _sql_profile = crate::session_journal::profile::timer("sql.ROLLBACK", 0);
                    tx.rollback().await
                }
                .map_err(ambiguous_database_error)?;
                return Err(e);
            }
        }
        self.cache_lock()
            .map_err(RuntimeError::retryable)?
            .invalidate(&scope.1);
        Ok(())
    }
}
