//! Row encoders shared by atomic apply and explicit test fixtures.
use super::{
    cache::Scope,
    codec, conflict, corrupt,
    seal::{Binding, FORMAT, Sealed, Unit, role},
    storage::{ForgeJournalStorage, PackView, database_error},
    wire,
};
use agent_runtime::core::{error::RuntimeError, journal::*};
use sqlx::SqliteConnection;
use std::collections::BTreeSet;

impl ForgeJournalStorage {
    pub(super) async fn seal_row(
        &self,
        scope: &Scope,
        owner: &str,
        unit: Unit,
        row: &[u8],
        generation: u64,
        raw: Vec<u8>,
    ) -> Result<Sealed, RuntimeError> {
        let keys = self.keys.clone();
        let scope = scope.clone();
        let owner = owner.to_owned();
        let row = row.to_vec();
        self.work(move || {
            keys.seal(
                &Binding {
                    unit,
                    domain: &scope.0,
                    session: &scope.1,
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
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM agent_session WHERE runtime_session_id=? ORDER BY id LIMIT 2",
        )
        .bind(scope.1.as_str())
        .fetch_all(&mut *conn)
        .await
        .map_err(database_error)?;
        let [owner] = ids.as_slice() else {
            return Err(RuntimeError::internal(
                "journal host session mapping missing or ambiguous",
            ));
        };
        let stored:Option<String>=sqlx::query_scalar("SELECT forge_session_id FROM agent_session_journal_session WHERE storage_id=? AND role=? AND runtime_session_id=?")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).fetch_optional(&mut *conn).await.map_err(database_error)?;
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
        sqlx::query("INSERT INTO agent_session_journal_session (storage_id,role,runtime_session_id,forge_session_id,generation,revision,writer_epoch,collection_dirty,published,has_checkpoint,retired,key_revision,format,nonce,payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(owner)
            .bind(record.generation.to_be_bytes().as_slice()).bind(record.revision.0.to_be_bytes().as_slice()).bind(record.writer_epoch.to_be_bytes().as_slice())
            .bind(record.collection_dirty).bind(record.head.is_some()).bind(record.has_checkpoint).bind(record.retirement.is_some())
            .bind(self.keys.revision).bind(FORMAT).bind(sealed.nonce).bind(sealed.payload).execute(&mut *conn).await.map_err(database_error)?;
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
        let changed=sqlx::query("UPDATE agent_session_journal_session SET generation=?,revision=?,writer_epoch=?,collection_dirty=?,published=?,has_checkpoint=?,retired=?,key_revision=?,format=?,nonce=?,payload=? WHERE storage_id=? AND role=? AND runtime_session_id=? AND forge_session_id=? AND revision=? AND writer_epoch=? AND generation=?")
            .bind(record.generation.to_be_bytes().as_slice()).bind(record.revision.0.to_be_bytes().as_slice()).bind(record.writer_epoch.to_be_bytes().as_slice())
            .bind(record.collection_dirty).bind(record.head.is_some()).bind(record.has_checkpoint).bind(record.retirement.is_some())
            .bind(self.keys.revision).bind(FORMAT).bind(sealed.nonce).bind(sealed.payload)
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(owner)
            .bind(expect.revision.0.to_be_bytes().as_slice()).bind(expect.writer_epoch.to_be_bytes().as_slice()).bind(expect.generation.to_be_bytes().as_slice())
            .execute(&mut *conn).await.map_err(database_error)?.rows_affected();
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
            sqlx::query("INSERT INTO agent_session_journal_membership (storage_id,role,runtime_session_id,forge_session_id,pack_id,bitmap) VALUES (?,?,?,?,?,?) ON CONFLICT(storage_id,role,runtime_session_id,pack_id) DO UPDATE SET bitmap=excluded.bitmap")
                .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(owner).bind(id).bind(bits).execute(&mut *conn).await.map_err(database_error)?;
        } else {
            sqlx::query("DELETE FROM agent_session_journal_membership WHERE storage_id=? AND role=? AND runtime_session_id=? AND pack_id=?")
                .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(id).execute(&mut *conn).await.map_err(database_error)?;
        }
        Ok(())
    }
    pub(super) async fn insert_pack(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        objects: Vec<JournalStoredObject>,
    ) -> Result<i64, RuntimeError> {
        let encoded = self.work(move || codec::encode(&objects)).await?;
        // Reserve the AUTOINCREMENT identity inside this transaction; this
        // placeholder is replaced by authenticated units before any commit.
        let id:i64=sqlx::query_scalar("INSERT INTO agent_session_journal_pack (storage_id,role,runtime_session_id,forge_session_id,generation,object_count,retained_canonical_bytes,key_revision,format,index_nonce,index_payload,body_nonce,body_payload) VALUES (?,?,?,?,?, ?,?,?,?,zeroblob(24),zeroblob(16),zeroblob(24),zeroblob(16)) RETURNING pack_id")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(owner).bind(0u64.to_be_bytes().as_slice())
            .bind(encoded.count as i64).bind(encoded.canonical_bytes as i64).bind(self.keys.revision).bind(FORMAT).fetch_one(&mut *conn).await.map_err(database_error)?;
        let index = self
            .seal_row(
                scope,
                owner,
                Unit::PackIndex,
                &id.to_be_bytes(),
                0,
                encoded.index,
            )
            .await?;
        let body = self
            .seal_row(
                scope,
                owner,
                Unit::PackBody,
                &id.to_be_bytes(),
                0,
                encoded.body,
            )
            .await?;
        sqlx::query("UPDATE agent_session_journal_pack SET index_nonce=?,index_payload=?,body_nonce=?,body_payload=? WHERE pack_id=?")
            .bind(index.nonce).bind(index.payload).bind(body.nonce).bind(body.payload).bind(id).execute(&mut *conn).await.map_err(database_error)?;
        let mut bits = vec![255; encoded.count.div_ceil(8)];
        if encoded.count % 8 != 0 {
            *bits.last_mut().ok_or_else(corrupt)? = (1 << (encoded.count % 8)) - 1;
        }
        self.put_bits(conn, scope, owner, id, &bits).await?;
        Ok(id)
    }
    /// Compare only prefix candidates, including deferred slots. New bodies are
    /// returned to the caller's staged pack; existing slots regain membership.
    pub(super) async fn admit(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        views: &mut [PackView],
        objects: &[JournalStoredObject],
    ) -> Result<Vec<JournalStoredObject>, RuntimeError> {
        let wanted = objects
            .iter()
            .flat_map(|o| {
                views
                    .iter()
                    .filter(|v| !v.index.directory.candidates(&o.id).is_empty())
                    .map(|v| v.index.key.id)
            })
            .collect::<BTreeSet<_>>();
        let bodies = self.bodies(conn, views, &wanted, false).await?;
        let mut changed = BTreeSet::new();
        let mut new = Vec::new();
        for object in objects {
            let mut found = false;
            for view in &mut *views {
                if view.index.key.owner != owner {
                    return Err(RuntimeError::internal("journal pack owner mapping changed"));
                }
                for slot in view.index.directory.candidates(&object.id) {
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
                    let sealed = self
                        .seal_row(scope, owner, Unit::Pin, &key, 0, wire::pin_bytes(pin)?)
                        .await?;
                    sqlx::query("INSERT INTO agent_session_journal_pin (storage_id,role,runtime_session_id,forge_session_id,pin_key,key_revision,format,nonce,payload) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(storage_id,role,runtime_session_id,pin_key) DO UPDATE SET key_revision=excluded.key_revision,format=excluded.format,nonce=excluded.nonce,payload=excluded.payload")
                        .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(owner).bind(key).bind(self.keys.revision).bind(FORMAT).bind(sealed.nonce).bind(sealed.payload).execute(&mut *conn).await.map_err(database_error)?;
                }
                JournalPinChange::Remove(_) => {
                    sqlx::query("DELETE FROM agent_session_journal_pin WHERE storage_id=? AND role=? AND runtime_session_id=? AND pin_key=?")
                    .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(key).execute(&mut *conn).await.map_err(database_error)?;
                }
            }
        }
        Ok(())
    }
    pub(super) async fn catalog_changes(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        changes: &[JournalCatalogChange],
    ) -> Result<(), RuntimeError> {
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
                    sqlx::query("INSERT INTO agent_session_journal_catalog (storage_id,role,runtime_session_id,forge_session_id,head_key,key_revision,format,nonce,payload) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(storage_id,role,runtime_session_id,head_key) DO UPDATE SET key_revision=excluded.key_revision,format=excluded.format,nonce=excluded.nonce,payload=excluded.payload")
                        .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(owner).bind(key).bind(self.keys.revision).bind(FORMAT).bind(sealed.nonce).bind(sealed.payload).execute(&mut *conn).await.map_err(database_error)?;
                }
                JournalCatalogChange::Remove(_) => {
                    sqlx::query("DELETE FROM agent_session_journal_catalog WHERE storage_id=? AND role=? AND runtime_session_id=? AND head_key=?")
                    .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(key).execute(&mut *conn).await.map_err(database_error)?;
                }
            }
        }
        Ok(())
    }
    #[cfg(feature = "test-support")]
    pub(crate) async fn seed(&self, write: JournalWrite) -> Result<(), RuntimeError> {
        let scope = (write.domain.clone(), write.session.clone());
        let mut tx = db::begin_immediate(self.db.pool())
            .await
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
        for pack in codec::split(new)? {
            self.insert_pack(&mut tx, &scope, &owner, pack).await?;
        }
        self.pin_changes(&mut tx, &scope, &owner, &write.pins)
            .await?;
        self.catalog_changes(&mut tx, &scope, &owner, &write.catalog)
            .await?;
        if let Some(op) = &write.operation {
            self.append_operation(&mut tx, &scope, &owner, op).await?;
        }
        tx.commit().await.map_err(database_error)?;
        self.cache_lock()?.invalidate(&scope);
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
        };
        let (table, key) = match kind {
            Unit::Operations => ("agent_session_journal_operation_pack", "tier"),
            _ => ("agent_session_journal_pack", "pack_id"),
        };
        let mut tx = db::begin_immediate(self.db.pool())
            .await
            .map_err(database_error)?;
        let row = sqlx::query(&format!(
            "SELECT * FROM {table} WHERE storage_id=? AND role=? AND runtime_session_id=? AND {key}=?"
        ))
        .bind(domain.storage_id.as_str())
        .bind(role(domain))
        .bind(session.as_str())
        .bind(id)
        .fetch_one(&mut *tx)
        .await
        .map_err(database_error)?;
        let owner: String = row.try_get("forge_session_id").map_err(|_| corrupt())?;
        let generation = match kind {
            Unit::Operations => 0,
            _ => super::storage::counter(&row, "generation")?,
        };
        let row_key = id.to_be_bytes();
        let binding = Binding {
            unit: kind,
            domain,
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
        sqlx::query(&format!(
            "UPDATE {table} SET {nonce}=?,{payload}=? WHERE storage_id=? AND role=? AND runtime_session_id=? AND {key}=?"
        ))
        .bind(sealed.nonce)
        .bind(sealed.payload)
        .bind(domain.storage_id.as_str())
        .bind(role(domain))
        .bind(session.as_str())
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(database_error)?;
        tx.commit().await.map_err(database_error)?;
        self.cache_lock()?.invalidate(&scope);
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
        let mut tx = db::begin_immediate(self.db.pool())
            .await
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
                let index=self.seal_row(&scope,&key.owner,Unit::PackIndex,&key.id.to_be_bytes(),generation,encoded.index).await?;
                let body=self.seal_row(&scope,&key.owner,Unit::PackBody,&key.id.to_be_bytes(),generation,encoded.body).await?;
                sqlx::query("UPDATE agent_session_journal_pack SET generation=?,index_nonce=?,index_payload=?,body_nonce=?,body_payload=? WHERE pack_id=?")
                    .bind(generation.to_be_bytes().as_slice()).bind(index.nonce).bind(index.payload).bind(body.nonce).bind(body.payload).bind(key.id).execute(&mut *tx).await.map_err(database_error)?;
            }
            Ok::<_,RuntimeError>(())
        }.await;
        match result {
            Ok(()) => tx.commit().await.map_err(database_error)?,
            Err(e) => {
                tx.rollback().await.map_err(database_error)?;
                return Err(e);
            }
        }
        self.cache_lock()?.invalidate(&scope);
        Ok(())
    }
}
