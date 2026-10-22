use agent_runtime::core::journal::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(crate) struct Record {
    revision: JournalRevision,
    epoch: u64,
    head: Option<JournalObject>,
    checkpoint: bool,
    retirement: Option<JournalRetirement>,
    dirty: bool,
    engine: (u32, Vec<u8>),
}
impl From<&JournalSessionRecord> for Record {
    fn from(r: &JournalSessionRecord) -> Self {
        Self {
            revision: r.revision,
            epoch: r.writer_epoch,
            head: r.head.clone(),
            checkpoint: r.has_checkpoint,
            retirement: r.retirement.clone(),
            dirty: r.collection_dirty,
            engine: (r.engine.version, r.engine.bytes.clone()),
        }
    }
}
impl From<Record> for JournalSessionRecord {
    fn from(r: Record) -> Self {
        Self {
            revision: r.revision,
            writer_epoch: r.epoch,
            generation: 0, // loaded from the transaction-queryable session column
            head: r.head,
            has_checkpoint: r.checkpoint,
            retirement: r.retirement,
            collection_dirty: r.dirty,
            engine: JournalEngineState {
                version: r.engine.0,
                bytes: r.engine.1,
            },
        }
    }
}
#[derive(Serialize, Deserialize)]
pub(crate) struct Pin(JournalPinId, bool, Vec<JournalObjectId>, u32, Vec<u8>);
impl From<&JournalPinRecord> for Pin {
    fn from(p: &JournalPinRecord) -> Self {
        Self(
            p.pin.clone(),
            p.blocks_retirement,
            p.roots.clone(),
            p.engine.version,
            p.engine.bytes.clone(),
        )
    }
}
impl From<Pin> for JournalPinRecord {
    fn from(p: Pin) -> Self {
        Self {
            pin: p.0,
            blocks_retirement: p.1,
            roots: p.2,
            engine: JournalEngineState {
                version: p.3,
                bytes: p.4,
            },
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Operation(
    pub(crate) JournalOperationId,
    pub(crate) JournalDigest,
    pub(crate) JournalRevision,
    pub(crate) JournalHead,
);
impl From<&JournalOperationRecord> for Operation {
    fn from(o: &JournalOperationRecord) -> Self {
        Self(
            o.operation.clone(),
            o.digest.clone(),
            o.revision,
            o.result.clone(),
        )
    }
}
impl From<Operation> for JournalOperationRecord {
    fn from(o: Operation) -> Self {
        Self {
            operation: o.0,
            digest: o.1,
            revision: o.2,
            result: o.3,
        }
    }
}
