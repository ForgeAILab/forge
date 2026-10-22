//! Lossless storage DTOs. Engine state is opaque; no private runtime decoding.
use super::corrupt;
use agent_runtime::core::{error::RuntimeError, journal::*};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub(super) const MAX_METADATA: usize = 64 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(remote = "JournalEngineState")]
struct Engine {
    version: u32,
    bytes: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "JournalSessionRecord")]
struct Record {
    generation: u64,
    revision: JournalRevision,
    writer_epoch: u64,
    head: Option<JournalObject>,
    has_checkpoint: bool,
    retirement: Option<JournalRetirement>,
    collection_dirty: bool,
    #[serde(with = "Engine")]
    engine: JournalEngineState,
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "JournalPinRecord")]
struct Pin {
    pin: JournalPinId,
    blocks_retirement: bool,
    roots: Vec<JournalObjectId>,
    #[serde(with = "Engine")]
    engine: JournalEngineState,
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "JournalCatalogEntry")]
struct Catalog {
    head: JournalObjectId,
    #[serde(with = "Engine")]
    engine: JournalEngineState,
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "JournalOperationRecord")]
struct Operation {
    operation: JournalOperationId,
    digest: JournalDigest,
    revision: JournalRevision,
    result: JournalHead,
}

fn encode<T: Serialize>(value: &T, max: usize) -> Result<Vec<u8>, RuntimeError> {
    #[cfg(feature = "test-support")]
    let _profile = crate::session_journal::profile::timer("wire.encode", 0);
    let bytes = serde_json::to_vec(value).map_err(|_| corrupt())?;
    if bytes.len() > max {
        return Err(corrupt());
    }
    Ok(bytes)
}
fn decode<T: DeserializeOwned>(bytes: &[u8], max: usize) -> Result<T, RuntimeError> {
    #[cfg(feature = "test-support")]
    let _profile = crate::session_journal::profile::timer("wire.decode", bytes.len());
    if bytes.len() > max {
        return Err(corrupt());
    }
    serde_json::from_slice(bytes).map_err(|_| corrupt())
}
macro_rules! adapter {
    ($name:ident, $ty:ty, $remote:literal, $encode:ident, $decode:ident) => {
        #[derive(Serialize, Deserialize)]
        struct $name(#[serde(with = $remote)] $ty);
        pub(super) fn $encode(value: &$ty) -> Result<Vec<u8>, RuntimeError> {
            encode(&$name(value.clone()), MAX_METADATA)
        }
        pub(super) fn $decode(bytes: &[u8]) -> Result<$ty, RuntimeError> {
            Ok(decode::<$name>(bytes, MAX_METADATA)?.0)
        }
    };
}
adapter!(
    StoredRecord,
    JournalSessionRecord,
    "Record",
    record_bytes,
    record
);
adapter!(StoredPin, JournalPinRecord, "Pin", pin_bytes, pin);
adapter!(
    StoredCatalog,
    JournalCatalogEntry,
    "Catalog",
    catalog_bytes,
    catalog
);
#[derive(Serialize, Deserialize)]
struct StoredOperation(#[serde(with = "Operation")] JournalOperationRecord);
pub(super) fn operations_bytes(ops: &[JournalOperationRecord]) -> Result<Vec<u8>, RuntimeError> {
    encode(
        &ops.iter().cloned().map(StoredOperation).collect::<Vec<_>>(),
        super::codec::PACK_CAP,
    )
}
pub(super) fn operations(bytes: &[u8]) -> Result<Vec<JournalOperationRecord>, RuntimeError> {
    Ok(
        decode::<Vec<StoredOperation>>(bytes, super::codec::PACK_CAP)?
            .into_iter()
            .map(|o| o.0)
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_engine_versions_and_bytes_are_lossless() {
        let record_value = JournalSessionRecord {
            generation: u64::MAX,
            revision: JournalRevision(u64::MAX),
            writer_epoch: u64::MAX,
            head: None,
            has_checkpoint: false,
            retirement: None,
            collection_dirty: true,
            engine: JournalEngineState {
                version: u32::MAX,
                bytes: vec![0, 255, 1, 0, 128],
            },
        };
        assert_eq!(
            record(&record_bytes(&record_value).unwrap()).unwrap(),
            record_value
        );
        let p = JournalPinRecord {
            pin: JournalPinId::new("pin"),
            blocks_retirement: true,
            roots: vec![JournalObjectId::from_bytes([1; 32])],
            engine: record_value.engine.clone(),
        };
        assert_eq!(pin(&pin_bytes(&p).unwrap()).unwrap(), p);
        let c = JournalCatalogEntry {
            head: p.roots[0].clone(),
            engine: record_value.engine,
        };
        assert_eq!(catalog(&catalog_bytes(&c).unwrap()).unwrap(), c);
    }
}
