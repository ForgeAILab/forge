//! Reference framing, deliberately modeling seal lengths rather than encryption.
use super::{failure, invalid_data};
use agent_runtime::core::{error::RuntimeError, journal::*};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;

const MAGIC: &[u8] = b"FORGE-JOURNAL\0\x01";
pub(crate) fn seal(bytes: &[u8]) -> Result<Vec<u8>, RuntimeError> {
    let mut out = MAGIC.to_vec();
    out.extend(zstd::stream::encode_all(bytes, 3).map_err(failure)?);
    out.extend([0; 16]); // modeled AEAD tag; separate 24-byte nonce in SQLite
    Ok(out)
}
pub(crate) fn unseal(bytes: &[u8]) -> Result<Vec<u8>, RuntimeError> {
    if bytes.len() < MAGIC.len() + 16 || !bytes.starts_with(MAGIC) {
        return Err(invalid_data());
    }
    zstd::stream::decode_all(&bytes[MAGIC.len()..bytes.len() - 16]).map_err(failure)
}
pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, RuntimeError> {
    seal(&serde_json::to_vec(value).map_err(failure)?)
}
pub(crate) fn decode<T: DeserializeOwned>(value: &[u8]) -> Result<T, RuntimeError> {
    serde_json::from_slice(&unseal(value)?).map_err(failure)
}
fn varint(mut n: usize, out: &mut Vec<u8>) {
    while n >= 128 {
        out.push(n as u8 | 128);
        n >>= 7;
    }
    out.push(n as u8);
}
fn number(bytes: &[u8], pos: &mut usize) -> Result<usize, RuntimeError> {
    let mut n = 0usize;
    for shift in (0..usize::BITS).step_by(7) {
        let byte = *bytes.get(*pos).ok_or_else(invalid_data)?;
        *pos += 1;
        n |= usize::from(byte & 127)
            .checked_shl(shift)
            .ok_or_else(invalid_data)?;
        if byte < 128 {
            return Ok(n);
        }
    }
    Err(invalid_data())
}
#[derive(Clone, Default)]
pub(crate) struct Pack {
    // Slot order matches prefix-directory order, even when bodies are kind ordered.
    pub(crate) objects: Vec<JournalStoredObject>,
}
impl Pack {
    pub(crate) fn encoded(&self) -> Result<Vec<u8>, RuntimeError> {
        let mut bodies: Vec<_> = self.objects.iter().enumerate().collect();
        bodies.sort_by(|a, b| {
            a.1.kind
                .as_str()
                .cmp(b.1.kind.as_str())
                .then(a.1.id.cmp(&b.1.id))
        });
        let mut offsets = vec![0; bodies.len()];
        let mut payload = Vec::new();
        for (i, object) in bodies {
            offsets[i] = payload.len();
            payload.extend_from_slice(&object.bytes);
        }
        let mut out = b"JRPACK\0\x01".to_vec();
        out.extend_from_slice(&(self.objects.len() as u32).to_le_bytes());
        for (i, object) in self.objects.iter().enumerate() {
            out.extend_from_slice(&object.id.to_bytes()[..4]);
            varint(offsets[i], &mut out);
            varint(object.bytes.len(), &mut out);
        }
        out.extend(payload);
        seal(&out)
    }
    pub(crate) fn decoded(bytes: &[u8]) -> Result<Self, RuntimeError> {
        let raw = unseal(bytes)?;
        if raw.get(..8) != Some(b"JRPACK\0\x01") {
            return Err(invalid_data());
        }
        let count = u32::from_le_bytes(
            raw.get(8..12)
                .ok_or_else(invalid_data)?
                .try_into()
                .map_err(failure)?,
        ) as usize;
        if count > raw.len() / 6 {
            return Err(invalid_data());
        }
        let mut pos = 12;
        let mut directory = Vec::with_capacity(count);
        for _ in 0..count {
            let prefix: [u8; 4] = raw
                .get(pos..pos + 4)
                .ok_or_else(invalid_data)?
                .try_into()
                .map_err(failure)?;
            pos += 4;
            directory.push((prefix, number(&raw, &mut pos)?, number(&raw, &mut pos)?));
        }
        let mut objects = Vec::with_capacity(count);
        for (prefix, offset, len) in directory {
            let start = pos.checked_add(offset).ok_or_else(invalid_data)?;
            let end = start.checked_add(len).ok_or_else(invalid_data)?;
            let body = raw.get(start..end).ok_or_else(invalid_data)?;
            let value: serde_json::Value = serde_json::from_slice(body).map_err(failure)?;
            let kind: JournalObjectKind =
                serde_json::from_value(value["kind"].clone()).map_err(failure)?;
            let id = journal_object_id(kind, JOURNAL_SCHEMA_VERSION, body);
            if id.to_bytes()[..4] != prefix {
                return Err(invalid_data());
            }
            let object = JournalObject {
                id: id.clone(),
                bytes: body.to_vec(),
            };
            object.kind().map_err(|_| invalid_data())?;
            objects.push(JournalStoredObject {
                id,
                kind,
                bytes: Arc::from(body),
            });
        }
        if objects.windows(2).any(|p| p[0].id >= p[1].id) {
            return Err(invalid_data());
        }
        Ok(Self { objects })
    }
    pub(crate) fn find(&self, id: &JournalObjectId) -> Result<Option<usize>, RuntimeError> {
        let prefix = id.to_bytes();
        let start = self
            .objects
            .partition_point(|o| o.id.to_bytes()[..4] < prefix[..4]);
        for (slot, object) in self.objects.iter().enumerate().skip(start) {
            if object.id.to_bytes()[..4] != prefix[..4] {
                break;
            }
            // A prefix is a candidate only. Verify the complete content address.
            if journal_object_id(object.kind, JOURNAL_SCHEMA_VERSION, &object.bytes) == *id {
                return Ok(Some(slot));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prefix_collision_and_missing_full_id_never_alias_a_body() {
        let mut objects: Vec<_> = [42269, 139372]
            .into_iter()
            .map(|value| {
                let object = JournalObject::encode(JournalObjectKind::JsonInline, &value).unwrap();
                JournalStoredObject {
                    id: object.id,
                    kind: JournalObjectKind::JsonInline,
                    bytes: object.bytes.into(),
                }
            })
            .collect();
        assert_eq!(objects[0].id.to_bytes()[..4], objects[1].id.to_bytes()[..4]);
        objects.sort_by(|a, b| a.id.cmp(&b.id));
        let pack = Pack::decoded(
            &Pack {
                objects: objects.clone(),
            }
            .encoded()
            .unwrap(),
        )
        .unwrap();
        for object in &objects {
            let slot = pack.find(&object.id).unwrap().unwrap();
            assert_eq!(pack.objects[slot].bytes, object.bytes);
        }
        let mut missing = objects[0].id.to_bytes();
        missing[31] ^= 1;
        assert_eq!(
            pack.find(&JournalObjectId::from_bytes(missing)).unwrap(),
            None
        );
    }
}
