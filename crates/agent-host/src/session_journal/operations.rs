//! S4 bounded binary carry and S5's explicit, scope-local pruning directive.
use super::{
    cache::Scope,
    codec, conflict, corrupt,
    seal::{Binding, FORMAT, Unit, role},
    storage::{ForgeJournalStorage, SealedInput, database_error},
    wire,
};
use agent_runtime::core::{error::RuntimeError, journal::*};
use sqlx::{Row, SqliteConnection};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
};

type Packs = BTreeMap<i64, Vec<JournalOperationRecord>>;

pub(super) struct OperationPrune {
    scope: Scope,
    floor: JournalRevision,
    epoch: u64,
    inputs: BTreeMap<i64, SealedInput>,
    packs: Packs,
    pub tiers: BTreeSet<i64>,
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use super::*;
    use crate::test_support::FileDb;
    use agent_runtime::core::{clock::Timestamp, ids::SessionId};

    async fn inventory() -> (
        FileDb,
        ForgeJournalStorage,
        JournalDomain,
        SessionId,
        JournalObjectId,
    ) {
        let f = FileDb::new().await.unwrap();
        let storage = ForgeJournalStorage::new(f.database(), [7; 32], 1).unwrap();
        storage.set_repacking(false);
        let domain = JournalDomain {
            storage_id: JournalStorageId::new("operation-budget"),
            role: JournalRole::Protected,
        };
        let session = SessionId::new("single-release-many-operations");
        f.ensure_session(&session).await.unwrap();
        let object = JournalObject::encode(JournalObjectKind::JsonInline, &"release me").unwrap();
        let object_id = object.id.clone();
        let record = JournalSessionRecord {
            generation: 1,
            revision: JournalRevision(100),
            writer_epoch: 1,
            head: None,
            has_checkpoint: false,
            retirement: None,
            collection_dirty: true,
            engine: JournalEngineState {
                version: u32::MAX,
                bytes: vec![],
            },
        };
        storage
            .apply(JournalWrite {
                domain: domain.clone(),
                session: session.clone(),
                kind: JournalWriteKind::Commit,
                expect: Default::default(),
                record,
                objects: JournalObjectBatch {
                    objects: vec![JournalStoredObject {
                        id: object.id,
                        kind: JournalObjectKind::JsonInline,
                        bytes: object.bytes.into(),
                    }],
                },
                operation: None,
                pins: vec![],
                catalog: vec![],
                release: vec![],
                prune_operations_before: None,
                inject: None,
            })
            .await
            .unwrap();
        let scope = (domain.clone(), session.clone());
        let mut tx = db::begin_immediate(f.database().pool()).await.unwrap();
        let owner = storage.mapping(&mut tx, &scope).await.unwrap();
        // Overflow tiers allow a synthetic >32-segment inventory without
        // constructing the astronomical binary-carry history of normal tiers.
        for tier in 64..97 {
            let head = JournalHead {
                schema_version: JOURNAL_SCHEMA_VERSION,
                storage_id: domain.storage_id.clone(),
                role: domain.role,
                session: session.clone(),
                revision: JournalRevision(1),
                writer_epoch: 0,
                batch_sequence: tier as u64,
                batch: object_id.clone(),
                snapshot: JournalSnapshotRoots {
                    history: Default::default(),
                    history_chain: journal_history_chain(std::iter::empty()),
                    usage: Default::default(),
                    extensions: Default::default(),
                    manifests: Default::default(),
                    planned_step_count: 0,
                    identity: Default::default(),
                    updated: Timestamp(1),
                },
                checkpoint: None,
                superseded_by: None,
                retirement: None,
                created: Timestamp(1),
                updated: Timestamp(1),
            };
            storage
                .save_operations(
                    &mut tx,
                    &scope,
                    &owner,
                    tier,
                    vec![JournalOperationRecord {
                        operation: JournalOperationId::new(format!("synthetic-{tier}")),
                        digest: journal_history_chain(std::iter::empty()),
                        revision: JournalRevision(1),
                        result: head,
                    }],
                )
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
        (f, storage, domain, session, object_id)
    }

    #[tokio::test]
    async fn single_release_caps_operation_units_and_finishes() {
        let (f, storage, domain, session, object_id) = inventory().await;
        for (call, units, remaining) in [(0, 32, 2), (1, 2, 0)] {
            let mut record = storage.load(&domain, &session).await.unwrap().unwrap();
            let expect = record.fence();
            record.generation += 1;
            record.collection_dirty = false;
            let plan = JournalCollectionPlan {
                max_units: 32,
                write: JournalWrite {
                    domain: domain.clone(),
                    session: session.clone(),
                    kind: JournalWriteKind::Collect,
                    expect,
                    record,
                    objects: Default::default(),
                    operation: None,
                    pins: vec![],
                    catalog: vec![],
                    release: if call == 0 {
                        vec![object_id.clone()]
                    } else {
                        vec![]
                    },
                    prune_operations_before: Some(JournalRevision(2)),
                    inject: None,
                },
            };
            assert_eq!(
                storage.prepare_collection_apply(&plan).await.unwrap(),
                JournalCollectionPreflight::Fits { units }
            );
            let receipt = storage.apply_collection(plan).await.unwrap();
            assert_eq!(receipt.rewritten_units, units);
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_session_journal_operation_pack WHERE runtime_session_id=?")
                .bind(session.as_str()).fetch_one(f.database().pool()).await.unwrap();
            assert_eq!(count, remaining);
            assert_eq!(
                storage
                    .load(&domain, &session)
                    .await
                    .unwrap()
                    .unwrap()
                    .collection_dirty,
                call == 0
            );
        }
        assert!(storage.members(&domain, &session).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn zero_cap_rejects_pending_operation_pruning() {
        let (f, storage, domain, session, _) = inventory().await;
        let before = storage.load(&domain, &session).await.unwrap().unwrap();
        let mut record = before.clone();
        record.generation += 1;
        record.collection_dirty = false;
        let plan = JournalCollectionPlan {
            max_units: 0,
            write: JournalWrite {
                domain: domain.clone(),
                session: session.clone(),
                kind: JournalWriteKind::Collect,
                expect: before.fence(),
                record,
                objects: Default::default(),
                operation: None,
                pins: vec![],
                catalog: vec![],
                release: vec![],
                prune_operations_before: Some(JournalRevision(2)),
                inject: None,
            },
        };
        assert_eq!(
            storage.prepare_collection_apply(&plan).await.unwrap(),
            JournalCollectionPreflight::Split { max_release: 0 }
        );
        assert_eq!(
            storage.apply_collection(plan).await.unwrap_err().kind,
            agent_runtime::core::error::ErrorKind::Conflict
        );
        assert_eq!(storage.load(&domain, &session).await.unwrap(), Some(before));
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM agent_session_journal_operation_pack WHERE runtime_session_id=?",
        )
        .bind(session.as_str())
        .fetch_one(f.database().pool())
        .await
        .unwrap();
        assert_eq!(count, 33);
    }

    #[tokio::test]
    async fn prepared_prune_rejects_changed_ciphertext_key_and_tier() {
        let (f, storage, domain, session, _) = inventory().await;
        let scope = (domain, session);
        for change in [
            "nonce=zeroblob(24)",
            "payload=zeroblob(length(payload))",
            "key_revision=2",
            "format=2",
            "tier=100",
        ] {
            let mut tx = db::begin_immediate(f.database().pool()).await.unwrap();
            let prepared = storage
                .prepare_operation_prune(&mut tx, &scope, Some(JournalRevision(2)), 1)
                .await
                .unwrap()
                .unwrap();
            let owner = storage.mapping(&mut tx, &scope).await.unwrap();
            sqlx::query(&format!("UPDATE agent_session_journal_operation_pack SET {change} WHERE runtime_session_id=? AND tier=64"))
                .bind(scope.1.as_str()).execute(&mut *tx).await.unwrap();
            let error = storage
                .prune_operations(
                    &mut tx,
                    &scope,
                    &owner,
                    JournalRevision(2),
                    1,
                    Some(&prepared),
                )
                .await
                .unwrap_err();
            assert_eq!(
                error.kind,
                agent_runtime::core::error::ErrorKind::Conflict,
                "{change}"
            );
            tx.rollback().await.unwrap();
        }
    }
}

impl ForgeJournalStorage {
    pub(super) async fn operation_packs(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
    ) -> Result<Packs, RuntimeError> {
        self.operation_packs_with_inputs(conn, scope, None).await
    }

    async fn operation_packs_with_inputs(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        mut inputs: Option<&mut BTreeMap<i64, SealedInput>>,
    ) -> Result<Packs, RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("operations.operation_packs", 0);
        let rows = { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.SELECT", 0); sqlx::query("SELECT * FROM agent_session_journal_operation_pack WHERE storage_id=? AND role=? AND runtime_session_id=? ORDER BY tier")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).fetch_all(&mut *conn).await }.map_err(database_error)?;
        let mut packs = BTreeMap::new();
        let mut ids = BTreeSet::new();
        for row in rows {
            self.revision(&row)?;
            if let Some(inputs) = inputs.as_mut() {
                inputs.insert(
                    row.try_get("tier").map_err(|_| corrupt())?,
                    SealedInput::read(&row, &["nonce", "payload"])?,
                );
            }
            let keys = self.keys.clone();
            let scope = scope.clone();
            let (tier, ops) = self
                .work(move || {
                    let tier: i64 = row.try_get("tier").map_err(|_| corrupt())?;
                    let owner: String = row.try_get("forge_session_id").map_err(|_| corrupt())?;
                    let compressed = keys.open(
                        &Binding {
                            unit: Unit::Operations,
                            domain: Some(&scope.0),
                            session: &scope.1,
                            owner: &owner,
                            row: &tier.to_be_bytes(),
                            generation: 0,
                        },
                        &row.try_get::<Vec<u8>, _>("nonce").map_err(|_| corrupt())?,
                        &row.try_get::<Vec<u8>, _>("payload")
                            .map_err(|_| corrupt())?,
                        codec::MAX_COMPRESSED_BYTES,
                    )?;
                    #[cfg(feature = "test-support")]
                    let zstd_profile = super::profile::timer("zstd.decode", compressed.len());
                    let mut decoder = zstd::stream::read::Decoder::new(compressed.as_slice())
                        .map_err(|_| corrupt())?;
                    decoder.window_log_max(21).map_err(|_| corrupt())?;
                    let mut raw = Vec::new();
                    decoder
                        .take((codec::PACK_CAP + 1) as u64)
                        .read_to_end(&mut raw)
                        .map_err(|_| corrupt())?;
                    #[cfg(feature = "test-support")]
                    drop(zstd_profile);
                    #[cfg(feature = "test-support")]
                    super::profile::event("zstd.decompressed", raw.len());
                    let ops = wire::operations(&raw)?;
                    if tier < 0
                        || ops.is_empty()
                        || (tier < 64 && (ops.len() as u128) != (1u128 << tier))
                    {
                        return Err(corrupt());
                    }
                    Ok((tier, ops))
                })
                .await?;
            for op in &ops {
                if !ids.insert(op.operation.clone()) {
                    return Err(corrupt());
                }
            }
            packs.insert(tier, ops);
        }
        Ok(packs)
    }
    #[cfg(feature = "test-support")]
    pub async fn operation_pack_layout(
        &self,
        domain: &JournalDomain,
        session: &agent_runtime::core::ids::SessionId,
    ) -> Result<Vec<(i64, usize, usize)>, RuntimeError> {
        let mut tx = {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.BEGIN", 0);
            self.db.pool().begin().await
        }
        .map_err(database_error)?;
        let packs = self
            .operation_packs(&mut tx, &(domain.clone(), session.clone()))
            .await?;
        let layout = packs
            .into_iter()
            .map(|(tier, ops)| Ok((tier, ops.len(), wire::operations_bytes(&ops)?.len())))
            .collect::<Result<_, RuntimeError>>()?;
        {
            #[cfg(feature = "test-support")]
            let _sql_profile = crate::session_journal::profile::timer("sql.COMMIT", 0);
            tx.commit().await
        }
        .map_err(database_error)?;
        Ok(layout)
    }
    async fn save_operations(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        tier: i64,
        mut ops: Vec<JournalOperationRecord>,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("operations.save_operations", 0);
        ops.sort_by(|a, b| {
            a.revision
                .cmp(&b.revision)
                .then(a.operation.cmp(&b.operation))
        });
        let raw = self
            .work(move || {
                let raw = wire::operations_bytes(&ops).map_err(|_| conflict())?;
                {
                    #[cfg(feature = "test-support")]
                    let _zstd_profile = super::profile::timer("zstd.encode", raw.len());
                    let compressed = zstd::stream::encode_all(raw.as_slice(), 3);
                    #[cfg(feature = "test-support")]
                    if let Ok(bytes) = &compressed {
                        super::profile::event("zstd.compressed", bytes.len());
                    }
                    compressed
                }
                .map_err(|_| corrupt())
            })
            .await?;
        let sealed = self
            .seal_row(scope, owner, Unit::Operations, &tier.to_be_bytes(), 0, raw)
            .await?;
        { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.INSERT", 0); sqlx::query("INSERT INTO agent_session_journal_operation_pack (storage_id,role,runtime_session_id,forge_session_id,tier,key_revision,format,nonce,payload) VALUES (?,?,?,?,?,?,?,?,?)")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(owner).bind(tier).bind(self.keys.revision).bind(FORMAT).bind(sealed.nonce).bind(sealed.payload).execute(&mut *conn).await }.map_err(database_error)?;
        Ok(())
    }
    async fn delete_operations(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        tier: i64,
    ) -> Result<(), RuntimeError> {
        { #[cfg(feature = "test-support")] let _sql_profile = crate::session_journal::profile::timer("sql.DELETE", 0); sqlx::query("DELETE FROM agent_session_journal_operation_pack WHERE storage_id=? AND role=? AND runtime_session_id=? AND tier=?")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(tier).execute(&mut *conn).await }.map_err(database_error)?;
        Ok(())
    }
    fn overflow(packs: &Packs) -> Result<i64, RuntimeError> {
        packs
            .keys()
            .next_back()
            .copied()
            .unwrap_or(63)
            .max(63)
            .checked_add(1)
            .ok_or_else(conflict)
    }
    pub(super) async fn append_operation(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        operation: &JournalOperationRecord,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("operations.append_operation", 0);
        // Validate one complete serialized record before touching any tier.
        wire::operations_bytes(std::slice::from_ref(operation)).map_err(|_| conflict())?;
        let packs = self.operation_packs(conn, scope).await?;
        if let Some(old) = packs
            .values()
            .flatten()
            .find(|o| o.operation == operation.operation)
        {
            return if old == operation {
                Ok(())
            } else {
                Err(conflict())
            };
        }
        let mut records = vec![operation.clone()];
        for tier in 0..64 {
            if let Some(old) = packs.get(&tier) {
                let mut combined = records.clone();
                combined.extend(old.iter().cloned());
                if wire::operations_bytes(&combined).is_err() {
                    return self
                        .save_operations(conn, scope, owner, Self::overflow(&packs)?, records)
                        .await;
                }
                self.delete_operations(conn, scope, tier).await?;
                records = combined;
            } else {
                return self
                    .save_operations(conn, scope, owner, tier, records)
                    .await;
            }
        }
        Err(conflict())
    }
    pub(super) async fn prune_operations(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        owner: &str,
        floor: JournalRevision,
        epoch: u64,
        prepared: Option<&OperationPrune>,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "test-support")]
        let _profile = crate::session_journal::profile::timer("operations.prune_operations", 0);
        let mut packs = if let Some(prepared) = prepared {
            let rows = sqlx::query("SELECT * FROM agent_session_journal_operation_pack WHERE storage_id=? AND role=? AND runtime_session_id=? ORDER BY tier")
                .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str())
                .fetch_all(&mut *conn).await.map_err(database_error)?;
            let mut inputs = BTreeMap::new();
            for row in &rows {
                inputs.insert(
                    row.try_get("tier").map_err(|_| corrupt())?,
                    SealedInput::read(row, &["nonce", "payload"])?,
                );
            }
            if prepared.scope != *scope
                || prepared.floor != floor
                || prepared.epoch != epoch
                || prepared.inputs != inputs
            {
                return Err(conflict());
            }
            for row in &rows {
                self.revision(row)?;
            }
            prepared.packs.clone()
        } else {
            self.operation_packs(conn, scope).await?
        };
        let last = packs
            .values()
            .flatten()
            .filter(|o| o.result.writer_epoch == epoch)
            .map(|o| o.revision)
            .max();
        // Rebuild only changed packs. A partial power-of-two tier becomes a
        // bounded overflow segment; unaffected tiers keep their exact ciphertext.
        let mut survivors = Vec::new();
        let changed: Vec<_> = packs
            .iter()
            .filter(|(tier, _)| prepared.is_none_or(|p| p.tiers.contains(tier)))
            .filter_map(|(&tier, ops)| {
                let kept: Vec<_> = ops
                    .iter()
                    .filter(|o| {
                        o.revision >= floor
                            || (o.result.writer_epoch == epoch && Some(o.revision) == last)
                    })
                    .cloned()
                    .collect();
                if kept.len() == ops.len() {
                    None
                } else {
                    Some((tier, kept))
                }
            })
            .collect();
        for (tier, kept) in changed {
            self.delete_operations(conn, scope, tier).await?;
            packs.remove(&tier);
            if !kept.is_empty() {
                survivors.push(kept);
            }
        }
        for kept in survivors {
            // A subset of an already capped input is itself capped.
            let tier = Self::overflow(&packs)?;
            self.save_operations(conn, scope, owner, tier, kept.clone())
                .await?;
            packs.insert(tier, kept);
        }
        Ok(())
    }

    pub(super) async fn prepare_operation_prune(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        floor: Option<JournalRevision>,
        epoch: u64,
    ) -> Result<Option<OperationPrune>, RuntimeError> {
        let Some(floor) = floor else {
            return Ok(None);
        };
        let mut inputs = BTreeMap::new();
        let packs = self
            .operation_packs_with_inputs(conn, scope, Some(&mut inputs))
            .await?;
        let last = packs
            .values()
            .flatten()
            .filter(|o| o.result.writer_epoch == epoch)
            .map(|o| o.revision)
            .max();
        let tiers = packs
            .iter()
            .filter(|(_, ops)| {
                ops.iter().any(|o| {
                    o.revision < floor
                        && !(o.result.writer_epoch == epoch && Some(o.revision) == last)
                })
            })
            .map(|(&tier, _)| tier)
            .collect();
        Ok(Some(OperationPrune {
            scope: scope.clone(),
            floor,
            epoch,
            inputs,
            packs,
            tiers,
        }))
    }
}
