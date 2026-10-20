//! JRPACK v1: an independently sealed prefix directory and zstd-3 body unit.
use super::{conflict, corrupt};
use agent_runtime::core::{error::RuntimeError, journal::*};
use std::{io::Read, sync::Arc};

pub(super) const PACK_CAP: usize = 1024 * 1024;
pub(super) const MAX_INDEX_BYTES: usize = 12 + PACK_CAP * 10;
pub(super) const MAX_COMPRESSED_BYTES: usize = PACK_CAP + 64 * 1024;
const MAGIC: &[u8; 8] = b"JRPACK\0\x01";

#[derive(Clone)]
pub(super) struct Slot {
    pub prefix: [u8; 4],
    pub offset: usize,
    pub len: usize,
}
#[derive(Clone)]
pub(super) struct Directory {
    pub slots: Vec<Slot>,
    pub body_len: usize,
}
impl Directory {
    pub fn candidates(&self, id: &JournalObjectId) -> std::ops::Range<usize> {
        let prefix: [u8; 4] = id.to_bytes()[..4].try_into().expect("four bytes");
        self.slots.partition_point(|s| s.prefix < prefix)
            ..self.slots.partition_point(|s| s.prefix <= prefix)
    }
    pub fn charge(&self) -> usize {
        std::mem::size_of::<Self>() + self.slots.capacity() * std::mem::size_of::<Slot>()
    }
}

pub(super) struct EncodedPack {
    pub index: Vec<u8>,
    pub body: Vec<u8>,
    pub count: usize,
    pub canonical_bytes: usize,
}

fn varint(mut n: usize, out: &mut Vec<u8>) {
    while n >= 128 {
        out.push(n as u8 | 128);
        n >>= 7;
    }
    out.push(n as u8);
}
fn number(bytes: &[u8], pos: &mut usize) -> Result<usize, RuntimeError> {
    let mut n = 0u64;
    for shift in (0..64).step_by(7) {
        let b = *bytes.get(*pos).ok_or_else(corrupt)?;
        *pos += 1;
        if shift == 63 && b > 1 {
            return Err(corrupt());
        }
        n |= u64::from(b & 127) << shift;
        if b < 128 {
            if shift != 0 && b == 0 {
                return Err(corrupt());
            }
            return usize::try_from(n).map_err(|_| corrupt());
        }
    }
    Err(corrupt())
}

pub(super) fn validate(object: &JournalStoredObject) -> Result<(), RuntimeError> {
    if object.bytes.is_empty() || object.bytes.len() > PACK_CAP {
        return Err(conflict());
    }
    if (JournalObject {
        id: object.id.clone(),
        bytes: object.bytes.to_vec(),
    })
    .kind()?
        != object.kind
    {
        return Err(conflict());
    }
    Ok(())
}

pub(super) fn encode(objects: &[JournalStoredObject]) -> Result<EncodedPack, RuntimeError> {
    if objects.is_empty() || objects.windows(2).any(|p| p[0].id >= p[1].id) {
        return Err(conflict());
    }
    let total = objects.iter().try_fold(0usize, |n, o| {
        validate(o)?;
        n.checked_add(o.bytes.len())
            .filter(|n| *n <= PACK_CAP)
            .ok_or_else(conflict)
    })?;
    let mut ordered: Vec<_> = objects.iter().enumerate().collect();
    ordered.sort_by(|a, b| {
        a.1.kind
            .as_str()
            .cmp(b.1.kind.as_str())
            .then(a.1.id.cmp(&b.1.id))
    });
    let mut offsets = vec![0; objects.len()];
    let mut raw = Vec::with_capacity(total);
    for (i, o) in ordered {
        offsets[i] = raw.len();
        raw.extend_from_slice(&o.bytes);
    }
    let mut index = MAGIC.to_vec();
    index.extend_from_slice(&(objects.len() as u32).to_le_bytes());
    for (i, o) in objects.iter().enumerate() {
        index.extend_from_slice(&o.id.to_bytes()[..4]);
        varint(offsets[i], &mut index);
        varint(o.bytes.len(), &mut index);
    }
    Ok(EncodedPack {
        index,
        body: zstd::stream::encode_all(raw.as_slice(), 3).map_err(|_| corrupt())?,
        count: objects.len(),
        canonical_bytes: total,
    })
}

pub(super) fn decode_index(
    bytes: &[u8],
    count: usize,
    retained: usize,
) -> Result<Directory, RuntimeError> {
    if bytes.len() > MAX_INDEX_BYTES
        || bytes.get(..8) != Some(MAGIC)
        || retained == 0
        || retained > PACK_CAP
        || count == 0
        || count > retained
    {
        return Err(corrupt());
    }
    let encoded = u32::from_le_bytes(
        bytes
            .get(8..12)
            .ok_or_else(corrupt)?
            .try_into()
            .map_err(|_| corrupt())?,
    ) as usize;
    if encoded != count || count > bytes.len().saturating_sub(12) / 6 {
        return Err(corrupt());
    }
    let mut pos = 12;
    let mut slots = Vec::with_capacity(count);
    for _ in 0..count {
        let prefix = bytes
            .get(pos..pos + 4)
            .ok_or_else(corrupt)?
            .try_into()
            .map_err(|_| corrupt())?;
        pos += 4;
        let offset = number(bytes, &mut pos)?;
        let len = number(bytes, &mut pos)?;
        if len == 0 || offset.checked_add(len).is_none_or(|end| end > retained) {
            return Err(corrupt());
        }
        slots.push(Slot {
            prefix,
            offset,
            len,
        });
    }
    if pos != bytes.len() || slots.windows(2).any(|p| p[0].prefix > p[1].prefix) {
        return Err(corrupt());
    }
    // Exact coverage: reject overlap, gaps and hidden/trailing body bytes.
    let mut ranges: Vec<_> = slots.iter().map(|s| (s.offset, s.len)).collect();
    ranges.sort_unstable();
    let mut end = 0;
    for (offset, len) in ranges {
        if offset != end {
            return Err(corrupt());
        }
        end = end.checked_add(len).ok_or_else(corrupt)?;
    }
    if end != retained {
        return Err(corrupt());
    }
    Ok(Directory {
        slots,
        body_len: retained,
    })
}

pub(super) fn decode_body(
    directory: &Directory,
    bytes: &[u8],
) -> Result<Vec<JournalStoredObject>, RuntimeError> {
    if bytes.len() > MAX_COMPRESSED_BYTES {
        return Err(corrupt());
    }
    // The take bound applies to decompressed bytes, including hostile frames.
    let mut decoder = zstd::stream::read::Decoder::new(bytes).map_err(|_| corrupt())?;
    // zstd level 3 uses a 2 MiB window even for a smaller streaming frame.
    decoder.window_log_max(21).map_err(|_| corrupt())?;
    let mut raw = Vec::with_capacity(directory.body_len);
    decoder
        .take((directory.body_len + 1) as u64)
        .read_to_end(&mut raw)
        .map_err(|_| corrupt())?;
    if raw.len() != directory.body_len {
        return Err(corrupt());
    }
    let mut objects = Vec::with_capacity(directory.slots.len());
    for slot in &directory.slots {
        let body = raw
            .get(slot.offset..slot.offset + slot.len)
            .ok_or_else(corrupt)?;
        #[derive(serde::Deserialize)]
        struct Kind {
            kind: JournalObjectKind,
        }
        let kind = serde_json::from_slice::<Kind>(body)
            .map_err(|_| corrupt())?
            .kind;
        let id = journal_object_id(kind, JOURNAL_SCHEMA_VERSION, body);
        if id.to_bytes()[..4] != slot.prefix {
            return Err(corrupt());
        }
        let object = JournalStoredObject {
            id,
            kind,
            bytes: Arc::from(body),
        };
        validate(&object).map_err(|_| corrupt())?;
        objects.push(object);
    }
    if objects.windows(2).any(|p| p[0].id >= p[1].id) {
        return Err(corrupt());
    }
    let mut physical: Vec<_> = objects.iter().zip(&directory.slots).collect();
    physical.sort_by_key(|(_, s)| s.offset);
    if physical
        .windows(2)
        .any(|p| (p[0].0.kind.as_str(), &p[0].0.id) >= (p[1].0.kind.as_str(), &p[1].0.id))
    {
        return Err(corrupt());
    }
    Ok(objects)
}

pub(super) fn split(
    objects: Vec<JournalStoredObject>,
) -> Result<Vec<Vec<JournalStoredObject>>, RuntimeError> {
    let mut packs = Vec::new();
    let mut pending = Vec::new();
    let mut bytes = 0;
    for object in objects {
        validate(&object)?;
        if bytes + object.bytes.len() > PACK_CAP {
            packs.push(std::mem::take(&mut pending));
            bytes = 0;
        }
        bytes += object.bytes.len();
        pending.push(object);
    }
    if !pending.is_empty() {
        packs.push(pending);
    }
    Ok(packs)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn object(n: u64) -> JournalStoredObject {
        let o = JournalObject::encode(JournalObjectKind::JsonInline, &n).unwrap();
        JournalStoredObject {
            id: o.id,
            kind: JournalObjectKind::JsonInline,
            bytes: o.bytes.into(),
        }
    }
    fn roundtrip(objects: &[JournalStoredObject]) -> Vec<JournalStoredObject> {
        let pack = encode(objects).unwrap();
        let directory = decode_index(&pack.index, pack.count, pack.canonical_bytes).unwrap();
        decode_body(&directory, &pack.body).unwrap()
    }
    #[test]
    fn prefix_collision_and_full_id_kind_verification() {
        let mut objects = vec![object(42269), object(139372)];
        assert_eq!(objects[0].id.to_bytes()[..4], objects[1].id.to_bytes()[..4]);
        objects.sort_by(|a, b| a.id.cmp(&b.id));
        let decoded = roundtrip(&objects);
        assert_eq!(decoded, objects);
        let p = encode(&objects).unwrap();
        let d = decode_index(&p.index, p.count, p.canonical_bytes).unwrap();
        let mut missing = objects[0].id.to_bytes();
        missing[31] ^= 1;
        let missing = JournalObjectId::from_bytes(missing);
        assert_eq!(d.candidates(&missing).len(), 2);
        assert!(!decoded.iter().any(|o| o.id == missing));
        let mut bad = objects.clone();
        bad[0].kind = JournalObjectKind::Head;
        assert!(encode(&bad).is_err());
        bad = objects.clone();
        bad[0].id = missing;
        assert!(encode(&bad).is_err());
        let mut bad_dir = d.clone();
        bad_dir.slots[0].prefix[0] ^= 1;
        assert!(decode_body(&bad_dir, &p.body).is_err());
    }
    #[test]
    fn malformed_lengths_counts_offsets_and_compression_are_bounded() {
        let p = encode(&[object(1)]).unwrap();
        for end in 0..p.index.len() {
            assert!(decode_index(&p.index[..end], 1, p.canonical_bytes).is_err());
        }
        for (count, retained) in [
            (0, 1),
            (usize::MAX, 1),
            (1, usize::MAX),
            (2, p.canonical_bytes),
        ] {
            assert!(decode_index(&p.index, count, retained).is_err());
        }
        let mut malformed = MAGIC.to_vec();
        malformed.extend(1u32.to_le_bytes());
        malformed.extend([0; 4]);
        malformed.extend([0xff; 10]);
        malformed.push(1);
        assert!(decode_index(&malformed, 1, p.canonical_bytes).is_err());
        for offset in [1usize, PACK_CAP, usize::MAX] {
            let mut raw = MAGIC.to_vec();
            raw.extend(1u32.to_le_bytes());
            raw.extend(&p.index[12..16]);
            varint(offset, &mut raw);
            varint(p.canonical_bytes, &mut raw);
            assert!(decode_index(&raw, 1, p.canonical_bytes).is_err());
        }
        let mut objects = vec![object(1), object(2)];
        objects.sort_by(|a, b| a.id.cmp(&b.id));
        let p2 = encode(&objects).unwrap();
        let d2 = decode_index(&p2.index, 2, p2.canonical_bytes).unwrap();
        let mut raw = MAGIC.to_vec();
        raw.extend(2u32.to_le_bytes());
        for s in &d2.slots {
            raw.extend(s.prefix);
            varint(0, &mut raw);
            varint(s.len, &mut raw);
        }
        assert!(decode_index(&raw, 2, p2.canonical_bytes).is_err());
        let d = decode_index(&p.index, 1, p.canonical_bytes).unwrap();
        assert!(decode_body(&d, b"invalid zstd").is_err());
        let bomb = zstd::stream::encode_all(vec![0; PACK_CAP + 1].as_slice(), 3).unwrap();
        assert!(decode_body(&d, &bomb).is_err());
        let mut body = p.body.clone();
        body.push(0);
        assert!(decode_body(&d, &body).is_err());
    }
    #[test]
    fn kind_ordered_bodies_and_capped_split() {
        let mut objects = vec![object(1), object(2)];
        let o = JournalObject::encode(JournalObjectKind::StateValue, &serde_json::json!({"x":1}))
            .unwrap();
        objects.push(JournalStoredObject {
            id: o.id,
            kind: JournalObjectKind::StateValue,
            bytes: o.bytes.into(),
        });
        objects.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(roundtrip(&objects), objects);
        let mut large = Vec::new();
        for n in 0..3 {
            let o = JournalObject::encode(
                JournalObjectKind::JsonInline,
                &format!("{n}{}", "x".repeat(PACK_CAP / 2)),
            )
            .unwrap();
            large.push(JournalStoredObject {
                id: o.id,
                kind: JournalObjectKind::JsonInline,
                bytes: o.bytes.into(),
            });
        }
        large.sort_by(|a, b| a.id.cmp(&b.id));
        let packs = split(large).unwrap();
        assert_eq!(packs.len(), 3);
        for p in packs {
            assert!(encode(&p).unwrap().canonical_bytes <= PACK_CAP);
        }
        let mut bad = object(1);
        bad.bytes = vec![0; PACK_CAP + 1].into();
        assert!(split(vec![bad]).is_err());
    }
}
