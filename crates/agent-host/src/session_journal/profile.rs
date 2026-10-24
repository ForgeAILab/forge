//! Opt-in profiling for the ignored session-journal benchmark only.
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
static ENABLED: AtomicBool = AtomicBool::new(false);
static DATA: OnceLock<Mutex<BTreeMap<String, Metric>>> = OnceLock::new();
#[derive(Debug, Default, Clone, Serialize)]
pub struct Metric {
    pub calls: u64,
    pub bytes: u64,
    pub ns: u64,
    pub max_ns: u64,
}
#[derive(Debug)]
pub struct Timer {
    label: Option<String>,
    bytes: u64,
    start: Option<Instant>,
}
pub fn timer(label: impl AsRef<str>, bytes: usize) -> Timer {
    let active = ENABLED.load(Ordering::Relaxed);
    Timer {
        label: active.then(|| label.as_ref().to_owned()),
        bytes: bytes as u64,
        start: active.then(Instant::now),
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        if let (Some(label), Some(start)) = (self.label.take(), self.start) {
            let ns = start.elapsed().as_nanos() as u64;
            let mut data = DATA.get_or_init(Default::default).lock().unwrap();
            let m = data.entry(label).or_default();
            m.calls += 1;
            m.bytes += self.bytes;
            m.ns += ns;
            m.max_ns = m.max_ns.max(ns);
        }
    }
}
pub fn enable(value: bool) {
    ENABLED.store(value, Ordering::Relaxed);
}
pub fn take() -> BTreeMap<String, Metric> {
    std::mem::take(&mut *DATA.get_or_init(Default::default).lock().unwrap())
}
pub fn event(label: &str, bytes: usize) {
    let _timer = timer(label, bytes);
}

impl super::ForgeJournalStorage {
    /// Forensic read/re-encoding only, called outside all measured intervals.
    pub async fn profile_disk_for_test(&self) -> serde_json::Value {
        use super::{
            codec,
            seal::{Binding, Unit},
        };
        use agent_runtime::core::{ids::SessionId, journal::*};
        use sqlx::Row;
        use std::collections::{BTreeMap, BTreeSet};
        // Packs are session-owned and shared; each domain's bitmap is listed.
        let rows = sqlx::query("SELECT p.*,(SELECT group_concat(m.role||':'||hex(m.bitmap)) FROM agent_session_journal_membership m WHERE m.pack_id=p.pack_id) AS holders FROM agent_session_journal_pack p ORDER BY p.pack_id").fetch_all(self.db.pool()).await.unwrap();
        let mut packs = Vec::new();
        let mut objects_by_role: BTreeMap<String, BTreeMap<JournalObjectId, JournalStoredObject>> =
            BTreeMap::new();
        let mut held_by_role: BTreeMap<String, BTreeSet<JournalObjectId>> = BTreeMap::new();
        let mut roles = BTreeMap::<String, serde_json::Value>::new();
        for row in rows {
            let holders: Vec<(String, Vec<u8>)> = row
                .get::<Option<String>, _>("holders")
                .unwrap_or_default()
                .split(',')
                .filter(|h| !h.is_empty())
                .map(|h| {
                    let (role, hex) = h.split_once(':').unwrap();
                    let bits = (0..hex.len())
                        .step_by(2)
                        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                        .collect();
                    (role.to_owned(), bits)
                })
                .collect();
            let session = SessionId::new(row.get::<String, _>("runtime_session_id"));
            let owner: String = row.get("forge_session_id");
            let pid: i64 = row.get("pack_id");
            let generation = super::storage::counter(&row, "generation").unwrap();
            let binding = |unit| Binding {
                unit,
                domain: None,
                session: &session,
                owner: &owner,
                row: &[],
                generation,
            };
            let id = pid.to_be_bytes();
            let mut ib = binding(Unit::PackIndex);
            ib.row = &id;
            let ip: Vec<u8> = row.get("index_payload");
            let bp: Vec<u8> = row.get("body_payload");
            let index = self
                .keys
                .open(
                    &ib,
                    &row.get::<Vec<u8>, _>("index_nonce"),
                    &ip,
                    codec::MAX_INDEX_BYTES,
                )
                .unwrap();
            let mut bb = binding(Unit::PackBody);
            bb.row = &id;
            let compressed = self
                .keys
                .open(
                    &bb,
                    &row.get::<Vec<u8>, _>("body_nonce"),
                    &bp,
                    codec::MAX_COMPRESSED_BYTES,
                )
                .unwrap();
            let directory = codec::decode_index(
                &index,
                row.get::<i64, _>("object_count") as usize,
                row.get::<i64, _>("retained_canonical_bytes") as usize,
            )
            .unwrap();
            let objects = codec::decode_body(&directory, &compressed).unwrap();
            let mut kinds = BTreeMap::<String, Vec<JournalStoredObject>>::new();
            let mut held = 0usize;
            let mut dead = 0usize;
            let set = |bits: &[u8], slot: usize| {
                bits.get(slot / 8)
                    .is_some_and(|b| b & (1 << (slot % 8)) != 0)
            };
            for (slot, o) in objects.iter().enumerate() {
                let mut live = false;
                for (role, bits) in &holders {
                    if set(bits, slot) {
                        live = true;
                        objects_by_role
                            .entry(role.clone())
                            .or_default()
                            .insert(o.id.clone(), o.clone());
                        held_by_role
                            .entry(role.clone())
                            .or_default()
                            .insert(o.id.clone());
                    }
                }
                if live {
                    held += o.bytes.len();
                } else {
                    dead += o.bytes.len();
                }
                kinds
                    .entry(o.kind.as_str().to_owned())
                    .or_default()
                    .push(o.clone());
            }
            let mut combined = index.clone();
            let mut ordered = objects.clone();
            ordered.sort_by(|a, b| a.kind.as_str().cmp(b.kind.as_str()).then(a.id.cmp(&b.id)));
            for o in ordered {
                combined.extend_from_slice(&o.bytes);
            }
            let reference_combined = zstd::stream::encode_all(combined.as_slice(), 3)
                .unwrap()
                .len();
            let ks: BTreeMap<_,_> = kinds.into_iter().map(|(kind,os)| {
                let raw: Vec<u8> = os.iter().flat_map(|o| o.bytes.iter().copied()).collect();
                let z = zstd::stream::encode_all(raw.as_slice(),3).unwrap().len();
                (kind,serde_json::json!({"objects":os.len(),"canonical":raw.len(),"compressed_alone":z}))
            }).collect();
            let role: Vec<_> = holders.iter().map(|(r, _)| r.clone()).collect();
            packs.push(serde_json::json!({"id":pid,"holders":role,"objects":objects.len(),"held_canonical":held,"dead_canonical":dead,"index_raw":index.len(),"body_compressed":compressed.len(),"payload_nonce":ip.len()+bp.len()+48,"nonce_tag":80,"envelope":30,"combined_compressed_diagnostic":reference_combined,"kinds":ks}));
        }
        for role in ["protected", "ordinary"] {
            let mut tables = BTreeMap::new();
            for (table, expr) in [
                ("operation_pack", "length(payload)+length(nonce)"),
                ("session", "length(payload)+length(nonce)"),
                ("catalog", "length(payload)+length(nonce)"),
                ("pin", "length(payload)+length(nonce)"),
                ("membership", "length(bitmap)"),
            ] {
                let row=sqlx::query(&format!("SELECT count(*),coalesce(sum({expr}),0) FROM agent_session_journal_{table} WHERE role=?")).bind(role).fetch_one(self.db.pool()).await.unwrap();
                tables.insert(table,serde_json::json!({"rows":row.get::<i64,_>(0),"payload_nonce_bytes":row.get::<i64,_>(1)}));
            }
            roles.insert(role.to_owned(), serde_json::json!(tables));
        }
        let shared = sqlx::query("SELECT count(*),coalesce(sum(length(index_payload)+length(body_payload)+length(index_nonce)+length(body_nonce)),0) FROM agent_session_journal_pack").fetch_one(self.db.pool()).await.unwrap();
        roles.insert("session_packs".to_owned(), serde_json::json!({"rows":shared.get::<i64,_>(0),"payload_nonce_bytes":shared.get::<i64,_>(1)}));
        let empty = BTreeMap::new();
        let protected = objects_by_role.get("protected").unwrap_or(&empty);
        let ordinary = objects_by_role.get("ordinary").unwrap_or(&empty);
        let common: BTreeSet<_> = ordinary
            .keys()
            .filter(|id| protected.contains_key(*id))
            .cloned()
            .collect();
        let common_raw: usize = common.iter().map(|id| ordinary[id].bytes.len()).sum();
        let mut kind_totals = BTreeMap::new();
        for role in ["protected", "ordinary"] {
            let mut groups = BTreeMap::<String, Vec<&JournalStoredObject>>::new();
            for o in objects_by_role.get(role).unwrap_or(&empty).values() {
                groups
                    .entry(o.kind.as_str().to_owned())
                    .or_default()
                    .push(o);
            }
            let ks:BTreeMap<_,_>=groups.into_iter().map(|(k,os)| {
                let raw:Vec<u8>=os.iter().flat_map(|o|o.bytes.iter().copied()).collect();
                let z=zstd::stream::encode_all(raw.as_slice(),3).unwrap().len();
                let held=os.iter().filter(|o|held_by_role.get(role).is_some_and(|h|h.contains(&o.id))).map(|o|o.bytes.len()).sum::<usize>();
                (k,serde_json::json!({"objects":os.len(),"canonical":raw.len(),"held_canonical":held,"compressed_alone":z}))
            }).collect();
            kind_totals.insert(role, ks);
        }
        // Repack only ordinary's cross-domain-unique objects with the current cap
        // and codec, in memory. This does not write any storage.
        let mut ordinary_unique_payload = 0usize;
        let mut remaining: Vec<JournalStoredObject> = ordinary
            .values()
            .filter(|o| !common.contains(&o.id))
            .cloned()
            .collect();
        remaining.sort_by(|a, b| a.id.cmp(&b.id));
        for p in codec::split(remaining).unwrap() {
            let p = codec::encode(&p).unwrap();
            ordinary_unique_payload += p.index.len() + p.body.len() + 110;
        }
        serde_json::json!({"roles":roles,"packs":packs,"kind_totals":kind_totals,"cross_domain_common_objects":common.len(),"cross_domain_common_canonical":common_raw,"ordinary_unique_repacked_payload_diagnostic":ordinary_unique_payload,"per_kind_compression":"standalone diagnostic; mixed-pack compressed bytes are not additively attributable"})
    }
}

/// Receives test-only runtime-copy timer observations.
pub fn record(label: &str, ns: u64, bytes: usize) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    let mut data = DATA.get_or_init(Default::default).lock().unwrap();
    let m = data.entry(label.to_owned()).or_default();
    m.calls += 1;
    m.bytes += bytes as u64;
    m.ns += ns;
    m.max_ns = m.max_ns.max(ns);
}

pub fn snapshot() -> BTreeMap<String, Metric> {
    DATA.get_or_init(Default::default).lock().unwrap().clone()
}
