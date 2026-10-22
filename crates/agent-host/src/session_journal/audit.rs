//! Exhaustive sealed-unit inspection, compiled only for integration tests.
use super::{
    codec, corrupt,
    seal::{Binding, Unit},
    storage::{ForgeJournalStorage, counter, database_error},
    wire,
};
use agent_runtime::core::{error::RuntimeError, ids::SessionId, journal::*};
use sqlx::Row;
use std::io::Read;

fn role_of(role: &str) -> Result<JournalRole, RuntimeError> {
    match role {
        "ordinary" => Ok(JournalRole::Ordinary),
        "protected" => Ok(JournalRole::Protected),
        _ => Err(corrupt()),
    }
}

impl ForgeJournalStorage {
    /// Includes unheld/deferred bodies, not merely objects reachable by a reader.
    /// Uses one SQL snapshot and the production AEAD binding for every unit.
    #[doc(hidden)]
    pub async fn decrypted_units_for_test(
        &self,
    ) -> Result<Vec<(JournalRole, &'static str, Vec<u8>)>, RuntimeError> {
        let mut tx = self.db.pool().begin().await.map_err(database_error)?;
        let mut out = Vec::new();
        for (table, kind) in [
            ("session", Unit::Session),
            ("pin", Unit::Pin),
            ("catalog", Unit::Catalog),
            ("operation_pack", Unit::Operations),
            ("pack", Unit::PackIndex),
        ] {
            let rows = sqlx::query(&format!("SELECT * FROM agent_session_journal_{table}"))
                .fetch_all(&mut *tx)
                .await
                .map_err(database_error)?;
            for row in rows {
                self.revision(&row)?;
                // Session packs are shared: report their units under every
                // holder domain's role (both roles for an unheld pack).
                let (domain, roles) = if table == "pack" {
                    let held: Vec<String> = sqlx::query_scalar(
                        "SELECT DISTINCT role FROM agent_session_journal_membership WHERE pack_id=? ORDER BY role",
                    )
                    .bind(row.try_get::<i64, _>("pack_id").map_err(|_| corrupt())?)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(database_error)?;
                    let mut roles = Vec::new();
                    for role in held {
                        roles.push(role_of(&role)?);
                    }
                    if roles.is_empty() {
                        roles = vec![JournalRole::Ordinary, JournalRole::Protected];
                    }
                    (None, roles)
                } else {
                    let domain = JournalDomain {
                        storage_id: JournalStorageId::new(
                            row.try_get::<String, _>("storage_id")
                                .map_err(|_| corrupt())?,
                        ),
                        role: role_of(&row.try_get::<String, _>("role").map_err(|_| corrupt())?)?,
                    };
                    let role = domain.role;
                    (Some(domain), vec![role])
                };
                let session = SessionId::new(
                    row.try_get::<String, _>("runtime_session_id")
                        .map_err(|_| corrupt())?,
                );
                let owner: String = row.try_get("forge_session_id").map_err(|_| corrupt())?;
                let row_key: Vec<u8> = match kind {
                    Unit::Session => vec![],
                    Unit::Pin => row.try_get("pin_key").map_err(|_| corrupt())?,
                    Unit::Catalog => row.try_get("head_key").map_err(|_| corrupt())?,
                    Unit::Operations => row
                        .try_get::<i64, _>("tier")
                        .map_err(|_| corrupt())?
                        .to_be_bytes()
                        .to_vec(),
                    _ => row
                        .try_get::<i64, _>("pack_id")
                        .map_err(|_| corrupt())?
                        .to_be_bytes()
                        .to_vec(),
                };
                let generation = if matches!(kind, Unit::Session | Unit::PackIndex) {
                    counter(&row, "generation")?
                } else {
                    0
                };
                let units: &[(Unit, &str, &str, &str, bool)] = if table == "pack" {
                    &[
                        (
                            Unit::PackIndex,
                            "index_nonce",
                            "index_payload",
                            "index",
                            false,
                        ),
                        (Unit::PackBody, "body_nonce", "body_payload", "body", true),
                    ]
                } else {
                    &[(kind, "nonce", "payload", table, table == "operation_pack")]
                };
                let mut directory = None;
                for &(unit, nonce_col, payload_col, label, compressed) in units {
                    let bytes = self.keys.open(
                        &Binding {
                            unit,
                            domain: domain.as_ref(),
                            session: &session,
                            owner: &owner,
                            row: &row_key,
                            generation,
                        },
                        &row.try_get::<Vec<u8>, _>(nonce_col)
                            .map_err(|_| corrupt())?,
                        &row.try_get::<Vec<u8>, _>(payload_col)
                            .map_err(|_| corrupt())?,
                        wire::MAX_METADATA.max(codec::MAX_INDEX_BYTES),
                    )?;
                    if matches!(unit, Unit::PackIndex) {
                        directory = Some(codec::decode_index(
                            &bytes,
                            usize::try_from(
                                row.try_get::<i64, _>("object_count")
                                    .map_err(|_| corrupt())?,
                            )
                            .map_err(|_| corrupt())?,
                            usize::try_from(
                                row.try_get::<i64, _>("retained_canonical_bytes")
                                    .map_err(|_| corrupt())?,
                            )
                            .map_err(|_| corrupt())?,
                        )?);
                    }
                    if compressed {
                        let mut decoder = zstd::stream::read::Decoder::new(bytes.as_slice())
                            .map_err(|_| corrupt())?;
                        decoder.window_log_max(21).map_err(|_| corrupt())?;
                        let mut raw = Vec::new();
                        decoder
                            .take((codec::PACK_CAP + 1) as u64)
                            .read_to_end(&mut raw)
                            .map_err(|_| corrupt())?;
                        if raw.len() > codec::PACK_CAP {
                            return Err(corrupt());
                        }
                        // Each canonical object is separately inspectable JSON;
                        // the whole body is a concatenation of those objects.
                        // This is Forge framing, not runtime engine decoding.
                        if matches!(unit, Unit::PackBody) {
                            for slot in &directory.as_ref().ok_or_else(corrupt)?.slots {
                                let end = slot.offset.checked_add(slot.len).ok_or_else(corrupt)?;
                                let object = raw.get(slot.offset..end).ok_or_else(corrupt)?;
                                for role in &roles {
                                    out.push((*role, "object", object.to_vec()));
                                }
                            }
                        }
                        for role in &roles {
                            out.push((*role, label, raw.clone()));
                        }
                    }
                    for role in &roles {
                        out.push((*role, label, bytes.clone()));
                    }
                }
            }
        }
        tx.commit().await.map_err(database_error)?;
        Ok(out)
    }
}
