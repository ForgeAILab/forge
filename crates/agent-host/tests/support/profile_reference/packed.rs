use super::{
    codec::{self, Pack},
    commit_failure, conflict, failure as storage_failure, invalid_data, metadata,
    sqlite_failure as failure,
};
use agent_runtime::core::{error::RuntimeError, ids::SessionId, journal::*};
use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

/// Maximum sum of canonical object body lengths in a sealed object pack.
pub const LOGICAL_PACK_CAP: usize = 1024 * 1024;
const SMALL_PACK_CAP: usize = 64 * 1024;
type Directory = BTreeMap<i64, Vec<u8>>;
type OperationKey = (String, u8, String, JournalOperationId);
type Updates = BTreeMap<i64, Option<Arc<Pack>>>;
struct State {
    conn: Connection,
    packs: BTreeMap<i64, Arc<Pack>>,
    locations: HashMap<(String, JournalObjectId), (i64, usize)>,
    loaded_sessions: BTreeSet<String>,
    version: i64,
    operations: BTreeMap<OperationKey, JournalOperationRecord>,
}

impl State {
    fn refresh(&mut self) -> Result<(), RuntimeError> {
        let version = self
            .conn
            .query_row("PRAGMA data_version", [], |r| r.get(0))
            .map_err(failure)?;
        if self.version == version {
            return Ok(());
        }
        self.packs.clear();
        self.locations.clear();
        self.operations.clear();
        self.loaded_sessions.clear();
        self.version = version;
        Ok(())
    }
    // A session's index is lazy and independent. Opening a storage or reading
    // metadata never decodes body packs, and never builds a global body index.
    fn session(&mut self, session: &SessionId) -> Result<(), RuntimeError> {
        if self.loaded_sessions.contains(session.as_str()) {
            return Ok(());
        }
        let mut statement = self
            .conn
            .prepare("SELECT pack_id,payload FROM pack WHERE owner=? ORDER BY pack_id")
            .map_err(failure)?;
        let rows = statement
            .query_map([session.as_str()], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
            })
            .map_err(failure)?;
        for row in rows {
            let (pid, bytes) = row.map_err(failure)?;
            let pack = Arc::new(Pack::decoded(&bytes)?);
            for (slot, object) in pack.objects.iter().enumerate() {
                if self
                    .locations
                    .insert(
                        (session.as_str().to_owned(), object.id.clone()),
                        (pid, slot),
                    )
                    .is_some()
                {
                    return Err(invalid_data());
                }
            }
            self.packs.insert(pid, pack);
        }
        let mut statement = self
            .conn
            .prepare("SELECT storage,role,payload FROM operation_pack WHERE session=?")
            .map_err(failure)?;
        let rows = statement
            .query_map([session.as_str()], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, u8>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(failure)?;
        for row in rows {
            let (storage, role, payload) = row.map_err(failure)?;
            for op in codec::decode::<Vec<metadata::Operation>>(&payload)? {
                let op: JournalOperationRecord = op.into();
                let key = (
                    storage.clone(),
                    role,
                    session.as_str().to_owned(),
                    op.operation.clone(),
                );
                if self.operations.insert(key, op).is_some() {
                    return Err(invalid_data());
                }
            }
        }
        self.consistent()?;
        self.loaded_sessions.insert(session.as_str().to_owned());
        Ok(())
    }
    fn consistent(&self) -> Result<(), RuntimeError> {
        let version: i64 = self
            .conn
            .query_row("PRAGMA data_version", [], |r| r.get(0))
            .map_err(failure)?;
        if version != self.version {
            return Err(conflict());
        }
        Ok(())
    }
    fn update(&mut self, session: &SessionId, updates: Updates) {
        for id in updates.keys() {
            if let Some(old) = self.packs.remove(id) {
                for object in &old.objects {
                    self.locations
                        .remove(&(session.as_str().to_owned(), object.id.clone()));
                }
            }
        }
        for (id, pack) in updates {
            if let Some(pack) = pack {
                for (slot, object) in pack.objects.iter().enumerate() {
                    self.locations
                        .insert((session.as_str().to_owned(), object.id.clone()), (id, slot));
                }
                self.packs.insert(id, pack);
            }
        }
    }
}
/// Durable bench-only storage. Each apply uses BEGIN IMMEDIATE and rechecks the full
/// fence. Full-id maps are rebuilt lazily per session, never across sessions.
/// All diagnostics and errors redact stored bytes. Clones share one connection.
#[derive(Clone)]
pub struct PackedSqliteJournalStorage {
    state: Arc<Mutex<State>>,
    config: PackedSqliteConfig,
}
/// Storage-only maintenance policy; no journal format or replay contract change.
#[derive(Debug, Clone, Copy)]
pub struct PackedSqliteConfig {
    pub repack_on_collection: bool,
}
impl Default for PackedSqliteConfig {
    fn default() -> Self {
        Self {
            repack_on_collection: true,
        }
    }
}
impl fmt::Debug for PackedSqliteJournalStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PackedSqliteJournalStorage")
            .finish_non_exhaustive()
    }
}
impl PackedSqliteJournalStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RuntimeError> {
        Self::open_with_config(path, PackedSqliteConfig::default())
    }
    pub fn open_with_config(
        path: impl AsRef<Path>,
        config: PackedSqliteConfig,
    ) -> Result<Self, RuntimeError> {
        let conn = Connection::open(path).map_err(failure)?;
        conn.busy_timeout(std::time::Duration::from_secs(30))
            .map_err(failure)?;
        conn.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;").map_err(failure)?;
        conn.execute_batch(include_str!("schema.sql"))
            .map_err(failure)?;
        conn.execute_batch(
            "CREATE TEMP TABLE write_meter(bytes INTEGER NOT NULL);
             INSERT INTO write_meter VALUES (0);
             CREATE TEMP TRIGGER object_pack_insert AFTER INSERT ON main.pack BEGIN
               UPDATE write_meter SET bytes=bytes+length(new.payload)+length(new.nonce); END;
             CREATE TEMP TRIGGER object_pack_update AFTER UPDATE OF payload ON main.pack BEGIN
               UPDATE write_meter SET bytes=bytes+length(new.payload)+length(new.nonce); END;
             CREATE TEMP TRIGGER operation_pack_insert AFTER INSERT ON main.operation_pack BEGIN
               UPDATE write_meter SET bytes=bytes+length(new.payload)+length(new.nonce); END;
             CREATE TEMP TRIGGER operation_pack_update AFTER UPDATE OF payload ON main.operation_pack BEGIN
               UPDATE write_meter SET bytes=bytes+length(new.payload)+length(new.nonce); END;",
        ).map_err(failure)?;
        let storage = Self {
            config,
            state: Arc::new(Mutex::new(State {
                conn,
                packs: BTreeMap::new(),
                locations: HashMap::new(),
                loaded_sessions: BTreeSet::new(),
                version: -1,
                operations: BTreeMap::new(),
            })),
        };
        storage.lock()?.refresh()?;
        Ok(storage)
    }
    /// Host calls this after terminal publication (and, if desired, engine
    /// collection). Equal size tiers merge; it never guesses liveness or releases
    /// members. No hook is automatically wired into production runtime code.
    pub fn post_terminal(&self, session: &SessionId) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        state.session(session)?;
        let State {
            conn,
            packs,
            version,
            ..
        } = &mut *state;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failure)?;
        let current: i64 = tx
            .query_row("PRAGMA data_version", [], |r| r.get(0))
            .map_err(failure)?;
        if current != *version {
            return Err(conflict());
        }
        let mut updates = Updates::new();
        repack(&tx, packs, &mut updates, session, false)?;
        tx.commit().map_err(commit_failure)?;
        state.update(session, updates);
        Ok(())
    }
    /// Cumulative committed object/operation pack payload + nonce bytes written
    /// by this connection, including intermediate rewrites. Excludes metadata,
    /// SQLite page overhead and VACUUM; rolled-back writes do not contribute.
    pub fn pack_write_bytes(&self) -> Result<u64, RuntimeError> {
        self.lock()?
            .conn
            .query_row("SELECT bytes FROM write_meter", [], |r| r.get(0))
            .map_err(failure)
    }
    fn lock(&self) -> Result<MutexGuard<'_, State>, RuntimeError> {
        let mut state = self.state.lock().map_err(storage_failure)?;
        state.refresh()?;
        Ok(state)
    }
}
fn role(d: &JournalDomain) -> u8 {
    d.role as u8
}
fn bit(bits: &[u8], slot: usize) -> bool {
    bits.get(slot / 8)
        .is_some_and(|b| b & (1 << (slot % 8)) != 0)
}
fn set(bits: &mut [u8], slot: usize, value: bool) {
    if value {
        bits[slot / 8] |= 1 << (slot % 8);
    } else {
        bits[slot / 8] &= !(1 << (slot % 8));
    }
}
fn directory(
    conn: &Connection,
    d: &JournalDomain,
    s: &SessionId,
) -> Result<Directory, RuntimeError> {
    let mut statement = conn.prepare("SELECT pack_id,bits FROM membership WHERE storage=? AND role=? AND session=? ORDER BY pack_id").map_err(failure)?;
    statement
        .query_map(params![d.storage_id.as_str(), role(d), s.as_str()], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .map_err(failure)?
        .map(|r| r.map_err(failure))
        .collect()
}
fn put_bits(
    tx: &Connection,
    d: &JournalDomain,
    s: &SessionId,
    pid: i64,
    bits: &[u8],
) -> Result<(), RuntimeError> {
    if bits.iter().all(|b| *b == 0) {
        tx.execute(
            "DELETE FROM membership WHERE storage=? AND role=? AND session=? AND pack_id=?",
            params![d.storage_id.as_str(), role(d), s.as_str(), pid],
        )
        .map_err(failure)?;
    } else {
        tx.execute(
            "INSERT OR REPLACE INTO membership VALUES (?,?,?,?,?)",
            params![d.storage_id.as_str(), role(d), s.as_str(), pid, bits],
        )
        .map_err(failure)?;
    }
    Ok(())
}
fn load(
    conn: &Connection,
    d: &JournalDomain,
    s: &SessionId,
) -> Result<Option<JournalSessionRecord>, RuntimeError> {
    let raw: Option<(Vec<u8>, [u8; 8])> = conn
        .query_row(
            "SELECT payload,generation FROM session WHERE storage=? AND role=? AND session=?",
            params![d.storage_id.as_str(), role(d), s.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(failure)?;
    raw.map(|(b, generation)| {
        let mut record: JournalSessionRecord = codec::decode::<metadata::Record>(&b)?.into();
        record.generation = u64::from_le_bytes(generation);
        Ok(record)
    })
    .transpose()
}
fn fail(write: &JournalWrite, cut: JournalCommitFailurePoint) -> Result<(), RuntimeError> {
    if write.inject == Some(cut) {
        Err(storage_failure(()).retryable())
    } else {
        Ok(())
    }
}
fn split_packs(objects: Vec<JournalStoredObject>) -> Result<Vec<Pack>, RuntimeError> {
    let mut out = Vec::new();
    let mut pack = Pack::default();
    let mut bytes = 0;
    for object in objects {
        if object.bytes.len() > LOGICAL_PACK_CAP {
            return Err(conflict());
        }
        if bytes + object.bytes.len() > LOGICAL_PACK_CAP {
            out.push(std::mem::take(&mut pack));
            bytes = 0;
        }
        bytes += object.bytes.len();
        pack.objects.push(object);
    }
    if !pack.objects.is_empty() {
        out.push(pack);
    }
    Ok(out)
}
fn pack_bytes(pack: &Pack) -> usize {
    pack.objects.iter().map(|o| o.bytes.len()).sum()
}
fn save_pack(tx: &Connection, pid: i64, owner: &str, pack: &Pack) -> Result<(), RuntimeError> {
    if pack_bytes(pack) > LOGICAL_PACK_CAP {
        return Err(conflict());
    }
    tx.execute(
        "INSERT OR REPLACE INTO pack(pack_id,owner,payload,nonce) VALUES (?,?,?,?)",
        params![pid, owner, pack.encoded()?, [0u8; 24].as_slice()],
    )
    .map_err(failure)?;
    Ok(())
}
// Replacing a pack uses UPDATE: REPLACE would cascade-delete other memberships.
fn rewrite_pack(tx: &Connection, pid: i64, pack: &Pack) -> Result<(), RuntimeError> {
    if pack_bytes(pack) > LOGICAL_PACK_CAP {
        return Err(conflict());
    }
    tx.execute(
        "UPDATE pack SET payload=? WHERE pack_id=?",
        params![pack.encoded()?, pid],
    )
    .map_err(failure)?;
    Ok(())
}
// Rewrite at 25% dead canonical bytes, even when S2 is off. Retired session
// collection forces a purge. Membership bits hide deferred bytes immediately.
fn purge(
    tx: &Transaction<'_>,
    packs: &BTreeMap<i64, Arc<Pack>>,
    updates: &mut Updates,
    touched: &BTreeSet<i64>,
    force: bool,
) -> Result<JournalApplyReceipt, RuntimeError> {
    let mut receipt = JournalApplyReceipt::default();
    for &pid in touched {
        let pack = updates
            .get(&pid)
            .and_then(|p| p.as_ref())
            .or_else(|| packs.get(&pid))
            .ok_or_else(conflict)?;
        let holders: Vec<(String, u8, String, Vec<u8>)> = {
            let mut st = tx
                .prepare("SELECT storage,role,session,bits FROM membership WHERE pack_id=?")
                .map_err(failure)?;
            st.query_map([pid], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
                .map_err(failure)?
                .map(|r| r.map_err(failure))
                .collect::<Result<_, _>>()?
        };
        let kept: Vec<_> = (0..pack.objects.len())
            .filter(|&slot| holders.iter().any(|h| bit(&h.3, slot)))
            .collect();
        if kept.len() == pack.objects.len() {
            continue;
        }
        let live_bytes: usize = kept
            .iter()
            .map(|&slot| pack.objects[slot].bytes.len())
            .sum();
        let dead_bytes = pack_bytes(pack) - live_bytes;
        if !force && dead_bytes * 4 < pack_bytes(pack) {
            continue;
        }
        for (slot, object) in pack.objects.iter().enumerate() {
            if !holders.iter().any(|h| bit(&h.3, slot)) {
                receipt.reclaimed_objects += 1;
                receipt.reclaimed_bytes += object.bytes.len() as u64;
            }
        }
        if kept.is_empty() {
            tx.execute("DELETE FROM pack WHERE pack_id=?", [pid])
                .map_err(failure)?;
            updates.insert(pid, None);
        } else {
            let next = Pack {
                objects: kept
                    .iter()
                    .map(|&slot| pack.objects[slot].clone())
                    .collect(),
            };
            rewrite_pack(tx, pid, &next)?;
            for (storage, role, session, bits) in holders {
                let mut next_bits = vec![0; kept.len().div_ceil(8)];
                for (slot, &old) in kept.iter().enumerate() {
                    set(&mut next_bits, slot, bit(&bits, old));
                }
                tx.execute("UPDATE membership SET bits=? WHERE storage=? AND role=? AND session=? AND pack_id=?",params![next_bits,storage,role,session,pid]).map_err(failure)?;
            }
            updates.insert(pid, Some(Arc::new(next)));
        }
    }
    Ok(receipt)
}
// Move all domain bitmaps together within one session. No cross-session
// lookup or body sharing participates in admission, purge or repacking.
fn current_pack<'a>(
    packs: &'a BTreeMap<i64, Arc<Pack>>,
    updates: &'a Updates,
    pid: i64,
) -> Result<&'a Pack, RuntimeError> {
    match updates.get(&pid) {
        Some(Some(p)) => Ok(p),
        Some(None) => Err(conflict()),
        None => packs.get(&pid).map(AsRef::as_ref).ok_or_else(conflict),
    }
}
fn merge(
    tx: &Transaction<'_>,
    packs: &BTreeMap<i64, Arc<Pack>>,
    updates: &mut Updates,
    session: &SessionId,
    selected: &[i64],
) -> Result<Vec<i64>, RuntimeError> {
    let mut objects = BTreeMap::new();
    let mut holders = BTreeMap::<(String, u8, String), BTreeSet<JournalObjectId>>::new();
    for &pid in selected {
        let pack = current_pack(packs, updates, pid)?;
        for o in &pack.objects {
            objects.insert(o.id.clone(), o.clone());
        }
        let mut st = tx
            .prepare("SELECT storage,role,session,bits FROM membership WHERE pack_id=?")
            .map_err(failure)?;
        let rows = st
            .query_map([pid], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, u8>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Vec<u8>>(3)?,
                ))
            })
            .map_err(failure)?;
        for row in rows {
            let (storage, role, session, bits) = row.map_err(failure)?;
            let ids = holders.entry((storage, role, session)).or_default();
            ids.extend(
                pack.objects
                    .iter()
                    .enumerate()
                    .filter(|(slot, _)| bit(&bits, *slot))
                    .map(|(_, o)| o.id.clone()),
            );
        }
    }
    let mut next_pid: i64 = tx
        .query_row("SELECT coalesce(max(pack_id),0)+1 FROM pack", [], |r| {
            r.get(0)
        })
        .map_err(failure)?;
    for old in selected {
        tx.execute("DELETE FROM pack WHERE pack_id=?", [old])
            .map_err(failure)?;
        updates.insert(*old, None);
    }
    let mut result = Vec::new();
    for (index, pack) in split_packs(objects.into_values().collect())?
        .into_iter()
        .enumerate()
    {
        let pid = selected.get(index).copied().unwrap_or_else(|| {
            let id = next_pid;
            next_pid += 1;
            id
        });
        save_pack(tx, pid, session.as_str(), &pack)?;
        for ((storage, role, holder_session), ids) in &holders {
            if holder_session != session.as_str() {
                return Err(conflict());
            }
            let mut bits = vec![0; pack.objects.len().div_ceil(8)];
            for (slot, o) in pack.objects.iter().enumerate() {
                set(&mut bits, slot, ids.contains(&o.id));
            }
            if bits.iter().any(|b| *b != 0) {
                tx.execute(
                    "INSERT INTO membership VALUES (?,?,?,?,?)",
                    params![storage, role, holder_session, pid, bits],
                )
                .map_err(failure)?;
            }
        }
        updates.insert(pid, Some(Arc::new(pack)));
        result.push(pid);
    }
    Ok(result)
}

fn repack(
    tx: &Transaction<'_>,
    packs: &BTreeMap<i64, Arc<Pack>>,
    updates: &mut Updates,
    session: &SessionId,
    base: bool,
) -> Result<(), RuntimeError> {
    let mut ids: Vec<i64> = {
        let mut st = tx
            .prepare("SELECT pack_id FROM pack WHERE owner=? ORDER BY pack_id")
            .map_err(failure)?;
        st.query_map([session.as_str()], |r| r.get(0))
            .map_err(failure)?
            .map(|r| r.map_err(failure))
            .collect::<Result<_, _>>()?
    };
    if ids.is_empty() {
        return Ok(());
    }
    // Repartitioning deferred dead bodies could concentrate them above the
    // bound in one output pack. Leave those packs sealed until purge rewrites
    // them; all selected merge inputs must be fully live.
    let mut live = Vec::new();
    for pid in ids {
        let mut st = tx
            .prepare("SELECT bits FROM membership WHERE pack_id=?")
            .map_err(failure)?;
        let holders = st
            .query_map([pid], |r| r.get::<_, Vec<u8>>(0))
            .map_err(failure)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(failure)?;
        let pack = current_pack(packs, updates, pid)?;
        if (0..pack.objects.len()).all(|slot| holders.iter().any(|bits| bit(bits, slot))) {
            live.push(pid);
        }
    }
    ids = live;
    if base {
        // Stable large packs stay sealed. Only small packs and packs this
        // transaction already purged participate in the bounded base rewrite.
        ids.retain(|pid| {
            updates.contains_key(pid)
                || packs
                    .get(pid)
                    .is_some_and(|p| pack_bytes(p) < SMALL_PACK_CAP)
        });
        if ids.len() > 1 {
            merge(tx, packs, updates, session, &ids)?;
        }
    } else {
        loop {
            let mut tiers = BTreeMap::<u32, Vec<i64>>::new();
            for &pid in &ids {
                let bytes = current_pack(packs, updates, pid)?
                    .objects
                    .iter()
                    .map(|o| o.bytes.len())
                    .sum::<usize>()
                    .max(4096);
                tiers.entry(bytes.ilog2()).or_default().push(pid);
            }
            let Some(pair) = tiers
                .values()
                .filter(|v| v.len() > 1)
                .map(|v| [v[0], v[1]])
                .find(|pair| {
                    pair.iter()
                        .map(|pid| current_pack(packs, updates, *pid).map(pack_bytes))
                        .collect::<Result<Vec<_>, _>>()
                        .is_ok_and(|sizes| sizes.iter().sum::<usize>() <= LOGICAL_PACK_CAP)
                })
            else {
                break;
            };
            let merged = merge(tx, packs, updates, session, &pair)?;
            ids.retain(|id| !pair.contains(id));
            ids.extend(merged);
        }
    }
    Ok(())
}

// Binary-carry operation packs contain 2^tier complete original results.
// Appending merges equal counts while a pack fits the logical byte cap;
// capped segments stay sealed. Each operation is recompressed O(log N)
// times. Collection changes only packs losing operations, preserving the
// inclusive replay floor and the current writer epoch's last operation.
fn append_operation(
    tx: &Transaction<'_>,
    d: &JournalDomain,
    s: &SessionId,
    op: &JournalOperationRecord,
) -> Result<(), RuntimeError> {
    let mut records = vec![metadata::Operation::from(op)];
    for tier in 0..64 {
        let old: Option<Vec<u8>> = tx.query_row(
            "SELECT payload FROM operation_pack WHERE storage=? AND role=? AND session=? AND tier=?",
            params![d.storage_id.as_str(), role(d), s.as_str(), tier], |r| r.get(0)
        ).optional().map_err(failure)?;
        if let Some(old) = old {
            let mut combined = records.clone();
            combined.extend(codec::decode::<Vec<metadata::Operation>>(&old)?);
            if serde_json::to_vec(&combined)
                .map_err(storage_failure)?
                .len()
                <= LOGICAL_PACK_CAP
            {
                records = combined;
                tx.execute("DELETE FROM operation_pack WHERE storage=? AND role=? AND session=? AND tier=?",
                    params![d.storage_id.as_str(), role(d), s.as_str(), tier]).map_err(failure)?;
                continue;
            }
            // A full bounded segment stays sealed. Overflow keys never take
            // part in binary carry, so no operation pack can grow past the cap.
            let segment: u32 = tx.query_row(
                "SELECT max(64,coalesce(max(tier),63)+1) FROM operation_pack WHERE storage=? AND role=? AND session=?",
                params![d.storage_id.as_str(), role(d), s.as_str()], |r| r.get(0)
            ).map_err(failure)?;
            return save_operations(tx, d, s, segment, records);
        }
        return save_operations(tx, d, s, tier, records);
    }
    Err(conflict())
}
fn save_operations(
    tx: &Transaction<'_>,
    d: &JournalDomain,
    s: &SessionId,
    tier: u32,
    mut records: Vec<metadata::Operation>,
) -> Result<(), RuntimeError> {
    records.sort_by(|a, b| a.2.cmp(&b.2).then(a.0.cmp(&b.0)));
    let raw = serde_json::to_vec(&records).map_err(storage_failure)?;
    if raw.len() > LOGICAL_PACK_CAP {
        return Err(conflict());
    }
    tx.execute(
        "INSERT INTO operation_pack VALUES (?,?,?,?,?,?)",
        params![
            d.storage_id.as_str(),
            role(d),
            s.as_str(),
            tier,
            codec::seal(&raw)?,
            [0u8; 24].as_slice()
        ],
    )
    .map_err(failure)?;
    Ok(())
}

// Prune the durable payloads in the same fenced transaction as collection.
// The RAM replay index is updated only after that transaction commits.
fn prune_operations(
    tx: &Transaction<'_>,
    d: &JournalDomain,
    s: &SessionId,
    before: JournalRevision,
) -> Result<BTreeSet<JournalOperationId>, RuntimeError> {
    let epoch = load(tx, d, s)?.ok_or_else(conflict)?.writer_epoch;
    let packs: Vec<(u32, Vec<metadata::Operation>)> = {
        let mut st = tx
            .prepare(
                "SELECT tier,payload FROM operation_pack WHERE storage=? AND role=? AND session=?",
            )
            .map_err(failure)?;
        st.query_map(params![d.storage_id.as_str(), role(d), s.as_str()], |r| {
            Ok((r.get::<_, u32>(0)?, r.get::<_, Vec<u8>>(1)?))
        })
        .map_err(failure)?
        .map(|row| {
            let (tier, bytes) = row.map_err(failure)?;
            Ok((tier, codec::decode(&bytes)?))
        })
        .collect::<Result<_, RuntimeError>>()?
    };
    let last = packs
        .iter()
        .flat_map(|(_, ops)| ops)
        .filter(|op| op.3.writer_epoch == epoch)
        .map(|op| op.2)
        .max();
    let mut removed = BTreeSet::new();
    for (tier, mut ops) in packs {
        let previous_len = ops.len();
        ops.retain(|op| {
            let keep = op.2 >= before || (op.3.writer_epoch == epoch && Some(op.2) == last);
            if !keep {
                removed.insert(op.0.clone());
            }
            keep
        });
        // A stable pack's sealed payload and nonce do not change on GC.
        if ops.len() == previous_len {
            continue;
        }
        if ops.is_empty() {
            tx.execute(
                "DELETE FROM operation_pack WHERE storage=? AND role=? AND session=? AND tier=?",
                params![d.storage_id.as_str(), role(d), s.as_str(), tier],
            )
            .map_err(failure)?;
        } else {
            tx.execute("UPDATE operation_pack SET payload=? WHERE storage=? AND role=? AND session=? AND tier=?", params![codec::encode(&ops)?, d.storage_id.as_str(), role(d), s.as_str(), tier]).map_err(failure)?;
        }
    }
    Ok(removed)
}

#[async_trait]
impl JournalStorage for PackedSqliteJournalStorage {
    fn sharing(&self) -> JournalBodySharing {
        JournalBodySharing::PerSession
    }
    async fn retained_bytes(&self, d: &JournalDomain) -> Result<Option<u64>, RuntimeError> {
        let mut state = self.lock()?;
        let sessions: Vec<SessionId> = {
            let mut st = state
                .conn
                .prepare(
                    "SELECT session FROM session WHERE storage=?1 AND role=?2
                 UNION SELECT session FROM membership WHERE storage=?1 AND role=?2",
                )
                .map_err(failure)?;
            st.query_map(params![d.storage_id.as_str(), role(d)], |r| {
                r.get::<_, String>(0)
            })
            .map_err(failure)?
            .map(|r| r.map(SessionId::new).map_err(failure))
            .collect::<Result<_, _>>()?
        };
        let mut bytes = 0;
        for session in sessions {
            state.session(&session)?;
            let mut st = state
                .conn
                .prepare("SELECT pack_id FROM pack WHERE owner=?")
                .map_err(failure)?;
            for pid in st
                .query_map([session.as_str()], |r| r.get::<_, i64>(0))
                .map_err(failure)?
            {
                bytes += pack_bytes(&state.packs[&pid.map_err(failure)?]) as u64;
            }
        }
        state.consistent()?;
        Ok(Some(bytes))
    }
    async fn load(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Option<JournalSessionRecord>, RuntimeError> {
        load(&self.lock()?.conn, d, s)
    }
    async fn objects(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        ids: &[JournalObjectId],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RuntimeError> {
        let mut state = self.lock()?;
        state.session(s)?;
        let members = directory(&state.conn, d, s)?;
        state.consistent()?;
        ids.iter()
            .map(|id| {
                let Some(&(pid, _)) = state.locations.get(&(s.as_str().to_owned(), id.clone()))
                else {
                    return Ok(None);
                };
                let Some(bits) = members.get(&pid) else {
                    return Ok(None);
                };
                let pack = &state.packs[&pid];
                let slot = pack.find(id)?.ok_or_else(conflict)?;
                Ok(bit(bits, slot).then(|| pack.objects[slot].bytes.clone()))
            })
            .collect()
    }
    async fn members(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalObjectId>, RuntimeError> {
        let mut state = self.lock()?;
        state.session(s)?;
        let mut ids = Vec::new();
        let directory = directory(&state.conn, d, s)?;
        state.consistent()?;
        for (pid, bits) in directory {
            ids.extend(
                state.packs[&pid]
                    .objects
                    .iter()
                    .enumerate()
                    .filter(|(slot, _)| bit(&bits, *slot))
                    .map(|(_, o)| o.id.clone()),
            );
        }
        ids.sort();
        Ok(ids)
    }
    async fn operation(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        op: &JournalOperationId,
    ) -> Result<Option<JournalOperationRecord>, RuntimeError> {
        let mut state = self.lock()?;
        state.session(s)?;
        state.consistent()?;
        Ok(state
            .operations
            .get(&(
                d.storage_id.as_str().to_owned(),
                role(d),
                s.as_str().to_owned(),
                op.clone(),
            ))
            .cloned())
    }
    async fn pins(
        &self,
        d: &JournalDomain,
        s: &SessionId,
    ) -> Result<Vec<JournalPinRecord>, RuntimeError> {
        let state = self.lock()?;
        let mut st = state
            .conn
            .prepare(
                "SELECT payload FROM pin WHERE storage=? AND role=? AND session=? ORDER BY pin",
            )
            .map_err(failure)?;
        st.query_map(params![d.storage_id.as_str(), role(d), s.as_str()], |r| {
            r.get::<_, Vec<u8>>(0)
        })
        .map_err(failure)?
        .map(|r| codec::decode::<metadata::Pin>(&r.map_err(failure)?).map(Into::into))
        .collect()
    }
    async fn catalog(
        &self,
        d: &JournalDomain,
        s: &SessionId,
        head: &JournalObjectId,
    ) -> Result<Option<JournalCatalogEntry>, RuntimeError> {
        let state = self.lock()?;
        let raw: Option<Vec<u8>> = state
            .conn
            .query_row(
                "SELECT payload FROM catalog WHERE storage=? AND role=? AND session=? AND head=?",
                params![
                    d.storage_id.as_str(),
                    role(d),
                    s.as_str(),
                    head.to_bytes().as_slice()
                ],
                |r| r.get(0),
            )
            .optional()
            .map_err(failure)?;
        raw.map(|b| {
            codec::decode::<(u32, Vec<u8>)>(&b).map(|(version, bytes)| JournalCatalogEntry {
                head: head.clone(),
                engine: JournalEngineState { version, bytes },
            })
        })
        .transpose()
    }
    async fn dirty_sessions(
        &self,
        d: &JournalDomain,
        after: Option<&SessionId>,
        limit: usize,
    ) -> Result<Vec<SessionId>, RuntimeError> {
        let state = self.lock()?;
        let mut st=state.conn.prepare("SELECT session FROM session WHERE storage=? AND role=? AND dirty=1 AND (?3 IS NULL OR session>?3) ORDER BY session LIMIT ?4").map_err(failure)?;
        st.query_map(
            params![
                d.storage_id.as_str(),
                role(d),
                after.map(SessionId::as_str),
                i64::try_from(limit).unwrap_or(i64::MAX)
            ],
            |r| r.get::<_, String>(0),
        )
        .map_err(failure)?
        .map(|r| r.map(SessionId::new).map_err(failure))
        .collect()
    }
    async fn apply(&self, write: JournalWrite) -> Result<JournalApplyReceipt, RuntimeError> {
        let mut state = self.lock()?;
        state.session(&write.session)?;
        let State {
            conn,
            packs,
            locations,
            version,
            operations,
            ..
        } = &mut *state;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failure)?;
        // Refresh *inside* the transaction if another connection committed since
        // lock(). The next call rebuilds the cache; this one safely refuses.
        let current: i64 = tx
            .query_row("PRAGMA data_version", [], |r| r.get(0))
            .map_err(failure)?;
        if current != *version {
            return Err(conflict());
        }
        let d = &write.domain;
        let s = &write.session;
        let actual = load(&tx, d, s)?.map_or(JournalFence::default(), |r| r.fence());
        if actual != write.expect
            || write.expect.generation.checked_add(1) != Some(write.record.generation)
            || write.objects.objects.windows(2).any(|p| p[0].id >= p[1].id)
        {
            return Err(conflict());
        }
        // Validate identities and immutable duplicates before performing any SQL.
        for o in &write.objects.objects {
            if o.bytes.len() > LOGICAL_PACK_CAP {
                return Err(conflict());
            }
            if (JournalObject {
                id: o.id.clone(),
                bytes: o.bytes.to_vec(),
            })
            .kind()?
                != o.kind
            {
                return Err(conflict());
            }
            if let Some(&(pid, slot)) = locations.get(&(s.as_str().to_owned(), o.id.clone())) {
                if packs[&pid].objects[slot].bytes != o.bytes {
                    return Err(conflict());
                }
            }
        }
        let mut updates = Updates::new();
        let mut directory = directory(&tx, d, s)?;
        let mut next_pid: i64 = tx
            .query_row("SELECT coalesce(max(pack_id),0)+1 FROM pack", [], |r| {
                r.get(0)
            })
            .map_err(failure)?;
        let mut admitted = HashMap::new();
        for (step, cut) in [
            JournalCommitFailurePoint::AfterObjects,
            JournalCommitFailurePoint::AfterCheckpoint,
            JournalCommitFailurePoint::AfterBatch,
            JournalCommitFailurePoint::AfterHead,
        ]
        .into_iter()
        .enumerate()
        {
            let mut new = Vec::new();
            let mut changed = BTreeSet::new();
            for o in write.objects.objects.iter().filter(|o| match o.kind {
                JournalObjectKind::Checkpoint => step == 1,
                JournalObjectKind::Batch => step == 2,
                JournalObjectKind::Head => step == 3,
                _ => step == 0,
            }) {
                if let Some(&(existing, slot)) =
                    locations.get(&(s.as_str().to_owned(), o.id.clone()))
                {
                    let bits = directory
                        .entry(existing)
                        .or_insert_with(|| vec![0; packs[&existing].objects.len().div_ceil(8)]);
                    set(bits, slot, true);
                    changed.insert(existing);
                } else {
                    new.push(o.clone());
                }
            }
            for pid in changed {
                put_bits(&tx, d, s, pid, &directory[&pid])?;
            }
            for pack in split_packs(new)? {
                let pid = next_pid;
                next_pid += 1;
                save_pack(&tx, pid, s.as_str(), &pack)?;
                let mut bits = vec![0; pack.objects.len().div_ceil(8)];
                for (slot, object) in pack.objects.iter().enumerate() {
                    set(&mut bits, slot, true);
                    admitted.insert(object.id.clone(), (pid, slot));
                }
                put_bits(&tx, d, s, pid, &bits)?;
                directory.insert(pid, bits);
                updates.insert(pid, Some(Arc::new(pack)));
            }
            if step == 2 {
                fail(&write, cut)?;
                for pin in &write.pins {
                    match pin {
                        JournalPinChange::Put(p) => {
                            tx.execute(
                                "INSERT OR REPLACE INTO pin VALUES (?,?,?,?,?,?)",
                                params![
                                    d.storage_id.as_str(),
                                    role(d),
                                    s.as_str(),
                                    p.pin.as_str(),
                                    codec::encode(&metadata::Pin::from(p))?,
                                    [0u8; 24].as_slice()
                                ],
                            )
                            .map_err(failure)?;
                        }
                        JournalPinChange::Remove(p) => {
                            tx.execute("DELETE FROM pin WHERE storage=? AND role=? AND session=? AND pin=?",params![d.storage_id.as_str(),role(d),s.as_str(),p.as_str()]).map_err(failure)?;
                        }
                    }
                }
                for entry in &write.catalog {
                    match entry {
                        JournalCatalogChange::Put(e) => {
                            tx.execute(
                                "INSERT OR REPLACE INTO catalog VALUES (?,?,?,?,?,?)",
                                params![
                                    d.storage_id.as_str(),
                                    role(d),
                                    s.as_str(),
                                    e.head.to_bytes().as_slice(),
                                    codec::encode(&(e.engine.version, &e.engine.bytes))?,
                                    [0u8; 24].as_slice()
                                ],
                            )
                            .map_err(failure)?;
                        }
                        JournalCatalogChange::Remove(h) => {
                            tx.execute("DELETE FROM catalog WHERE storage=? AND role=? AND session=? AND head=?",params![d.storage_id.as_str(),role(d),s.as_str(),h.to_bytes().as_slice()]).map_err(failure)?;
                        }
                    }
                }
                fail(&write, JournalCommitFailurePoint::AfterPin)?;
            } else if step == 3 {
                let r = &write.record;
                tx.execute(
                    "INSERT OR REPLACE INTO session VALUES (?,?,?,?,?,?,?,?,?)",
                    params![
                        d.storage_id.as_str(),
                        role(d),
                        s.as_str(),
                        r.revision.0,
                        r.writer_epoch,
                        r.collection_dirty,
                        r.generation.to_le_bytes().as_slice(),
                        codec::encode(&metadata::Record::from(r))?,
                        [0u8; 24].as_slice()
                    ],
                )
                .map_err(failure)?;
                fail(&write, cut)?;
            } else {
                fail(&write, cut)?;
            }
        }
        if let Some(op) = &write.operation {
            let key = (
                d.storage_id.as_str().to_owned(),
                role(d),
                s.as_str().to_owned(),
                op.operation.clone(),
            );
            match operations.get(&key) {
                Some(old) if old != op => return Err(conflict()),
                Some(_) => {}
                None => append_operation(&tx, d, s, op)?,
            }
        }
        let pruned = write
            .prune_operations_before
            .map(|before| prune_operations(&tx, d, s, before))
            .transpose()?
            .unwrap_or_default();
        fail(&write, JournalCommitFailurePoint::AfterIdempotency)?;
        let mut touched = BTreeSet::new();
        for id in &write.release {
            let location = locations
                .get(&(s.as_str().to_owned(), id.clone()))
                .copied()
                .or_else(|| admitted.get(id).copied());
            if let Some((pid, slot)) = location {
                if let Some(bits) = directory.get_mut(&pid) {
                    if bit(bits, slot) {
                        set(bits, slot, false);
                        put_bits(&tx, d, s, pid, bits)?;
                        touched.insert(pid);
                    }
                }
            }
        }
        let retired = write.kind == JournalWriteKind::Collect
            && load(&tx, d, s)?.is_some_and(|r| r.retirement.is_some());
        if retired {
            let mut st = tx
                .prepare("SELECT pack_id FROM pack WHERE owner=?")
                .map_err(failure)?;
            touched.extend(
                st.query_map([s.as_str()], |r| r.get::<_, i64>(0))
                    .map_err(failure)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(failure)?,
            );
        }
        let receipt = purge(&tx, packs, &mut updates, &touched, retired)?;
        if self.config.repack_on_collection && write.kind == JournalWriteKind::Collect {
            repack(&tx, packs, &mut updates, s, true)?;
        }
        tx.commit().map_err(commit_failure)?;
        state.update(s, updates);
        if let Some(op) = &write.operation {
            state.operations.insert(
                (
                    d.storage_id.as_str().to_owned(),
                    role(d),
                    s.as_str().to_owned(),
                    op.operation.clone(),
                ),
                op.clone(),
            );
        }
        for operation in pruned {
            state.operations.remove(&(
                d.storage_id.as_str().to_owned(),
                role(d),
                s.as_str().to_owned(),
                operation,
            ));
        }
        fail(&write, JournalCommitFailurePoint::AfterCommit)?;
        Ok(receipt)
    }
    async fn compact(&self, domain: &JournalDomain) -> Result<(), RuntimeError> {
        let mut state = self.lock()?;
        let sessions: Vec<SessionId> = {
            let mut st = state.conn.prepare("SELECT DISTINCT session FROM membership WHERE storage=? AND role=? ORDER BY session").map_err(failure)?;
            st.query_map(params![domain.storage_id.as_str(), role(domain)], |r| {
                r.get::<_, String>(0)
            })
            .map_err(failure)?
            .map(|r| r.map(SessionId::new).map_err(failure))
            .collect::<Result<_, _>>()?
        };
        for session in &sessions {
            state.session(session)?;
        }
        let State {
            conn,
            packs,
            version,
            ..
        } = &mut *state;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failure)?;
        let current: i64 = tx
            .query_row("PRAGMA data_version", [], |r| r.get(0))
            .map_err(failure)?;
        if current != *version {
            return Err(conflict());
        }
        let mut changes = Vec::new();
        for session in sessions {
            let mut updates = Updates::new();
            repack(&tx, packs, &mut updates, &session, true)?;
            changes.push((session, updates));
        }
        tx.commit().map_err(commit_failure)?;
        for (session, updates) in changes {
            state.update(&session, updates);
        }
        state
            .conn
            .execute_batch(
                "PRAGMA wal_checkpoint(TRUNCATE); VACUUM; PRAGMA wal_checkpoint(TRUNCATE);",
            )
            .map_err(failure)
    }
}
