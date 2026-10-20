//! S4 bounded binary carry and S5's explicit, scope-local pruning directive.
use super::{
    cache::Scope,
    codec, conflict, corrupt,
    seal::{Binding, FORMAT, Unit, role},
    storage::{ForgeJournalStorage, database_error},
    wire,
};
use agent_runtime::core::{error::RuntimeError, journal::*};
use sqlx::{Row, SqliteConnection};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
};

type Packs = BTreeMap<i64, Vec<JournalOperationRecord>>;

impl ForgeJournalStorage {
    pub(super) async fn operation_packs(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
    ) -> Result<Packs, RuntimeError> {
        let rows = sqlx::query("SELECT * FROM agent_session_journal_operation_pack WHERE storage_id=? AND role=? AND runtime_session_id=? ORDER BY tier")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).fetch_all(&mut *conn).await.map_err(database_error)?;
        let mut packs = BTreeMap::new();
        let mut ids = BTreeSet::new();
        for row in rows {
            self.revision(&row)?;
            let keys = self.keys.clone();
            let scope = scope.clone();
            let (tier, ops) = self
                .work(move || {
                    let tier: i64 = row.try_get("tier").map_err(|_| corrupt())?;
                    let owner: String = row.try_get("forge_session_id").map_err(|_| corrupt())?;
                    let compressed = keys.open(
                        &Binding {
                            unit: Unit::Operations,
                            domain: &scope.0,
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
                    let mut decoder = zstd::stream::read::Decoder::new(compressed.as_slice())
                        .map_err(|_| corrupt())?;
                    decoder.window_log_max(21).map_err(|_| corrupt())?;
                    let mut raw = Vec::new();
                    decoder
                        .take((codec::PACK_CAP + 1) as u64)
                        .read_to_end(&mut raw)
                        .map_err(|_| corrupt())?;
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
        let mut tx = self.db.pool().begin().await.map_err(database_error)?;
        let packs = self
            .operation_packs(&mut tx, &(domain.clone(), session.clone()))
            .await?;
        let layout = packs
            .into_iter()
            .map(|(tier, ops)| Ok((tier, ops.len(), wire::operations_bytes(&ops)?.len())))
            .collect::<Result<_, RuntimeError>>()?;
        tx.commit().await.map_err(database_error)?;
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
        ops.sort_by(|a, b| {
            a.revision
                .cmp(&b.revision)
                .then(a.operation.cmp(&b.operation))
        });
        let raw = self
            .work(move || {
                let raw = wire::operations_bytes(&ops).map_err(|_| conflict())?;
                zstd::stream::encode_all(raw.as_slice(), 3).map_err(|_| corrupt())
            })
            .await?;
        let sealed = self
            .seal_row(scope, owner, Unit::Operations, &tier.to_be_bytes(), 0, raw)
            .await?;
        sqlx::query("INSERT INTO agent_session_journal_operation_pack (storage_id,role,runtime_session_id,forge_session_id,tier,key_revision,format,nonce,payload) VALUES (?,?,?,?,?,?,?,?,?)")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(owner).bind(tier).bind(self.keys.revision).bind(FORMAT).bind(sealed.nonce).bind(sealed.payload).execute(&mut *conn).await.map_err(database_error)?;
        Ok(())
    }
    async fn delete_operations(
        &self,
        conn: &mut SqliteConnection,
        scope: &Scope,
        tier: i64,
    ) -> Result<(), RuntimeError> {
        sqlx::query("DELETE FROM agent_session_journal_operation_pack WHERE storage_id=? AND role=? AND runtime_session_id=? AND tier=?")
            .bind(scope.0.storage_id.as_str()).bind(role(&scope.0)).bind(scope.1.as_str()).bind(tier).execute(&mut *conn).await.map_err(database_error)?;
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
    ) -> Result<(), RuntimeError> {
        let mut packs = self.operation_packs(conn, scope).await?;
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
}
