//! Freshness of the stored channel bytes, independent of bounded presentation.
use super::*;
use sha2::{Digest, Sha256};

impl ConditionInput {
    pub fn channel_digest(&self) -> String {
        of_view(&self.view())
    }
}

impl TaskCondition {
    /// Durable Integration/Check witnesses remain in this cache; the digest
    /// fences only the channel inputs, not those independently owned facts.
    pub fn channels_match(&self, input: &ConditionInput, digest: Option<&str>) -> bool {
        match digest {
            Some(digest) => digest == input.channel_digest(),
            None => {
                self.evidence().stated
                    || self.bounded_channels_equal(&compose_channels(&input.view()))
            }
        }
    }

    pub(crate) fn bounded_channels_equal(&self, other: &Self) -> bool {
        let (a, b) = (self.evidence(), other.evidence());
        a.error_annotation == b.error_annotation
            && a.blocked_json == b.blocked_json
            && a.failed_json == b.failed_json
            && a.entry_barrier_json == b.entry_barrier_json
            && a.metadata == b.metadata
            && a.unparsed_metadata == b.unparsed_metadata
    }

    pub(crate) fn can_stamp(&self, expected: &Self) -> bool {
        self.typed() == expected.typed()
            && (self.evidence().stated || self.bounded_channels_equal(expected))
    }
}

fn bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn member(hash: &mut Sha256, name: &str, raw: Option<&str>) {
    bytes(hash, name.as_bytes());
    match raw {
        None => hash.update([0]),
        Some(raw) => {
            // Keep the original spelling even for null, and distinguish it
            // from both SQL NULL/missing and every other present JSON value.
            hash.update([if raw.trim() == "null" { 1 } else { 2 }]);
            bytes(hash, raw.as_bytes());
        }
    }
}

pub(super) fn of_view(view: &ChannelView<'_>) -> String {
    let mut hash = Sha256::new();
    bytes(&mut hash, b"forge.task-condition.channels.1");
    for (name, field, raw) in [
        (
            "error_annotation",
            ConditionField::ErrorAnnotation,
            view.error_annotation,
        ),
        (
            "blocked_json",
            ConditionField::BlockedJson,
            view.blocked_json,
        ),
        ("failed_json", ConditionField::FailedJson, view.failed_json),
        (
            "entry_barrier_json",
            ConditionField::EntryBarrierJson,
            view.entry_barrier_json,
        ),
        (
            "metadata_json",
            ConditionField::MetadataJson,
            view.metadata_json,
        ),
    ] {
        if view.non_text.contains(&field) {
            bytes(&mut hash, name.as_bytes());
            hash.update([3]);
            let raw = view.non_text_bytes.iter().find(|(f, _)| *f == field);
            bytes(
                &mut hash,
                raw.map(|(_, b)| b.as_slice()).unwrap_or_default(),
            );
        } else if field == ConditionField::MetadataJson {
            match raw
                .map(serde_json::from_str::<Fields<'_>>)
                .unwrap_or_else(|| Ok(Fields::new()))
            {
                Ok(fields) => {
                    bytes(&mut hash, name.as_bytes());
                    hash.update([4]);
                    // The 24 evidence keys plus the queued-command
                    // continuation. No unrelated metadata enters the hash.
                    for key in EVIDENCE_METADATA_KEYS
                        .iter()
                        .copied()
                        .chain(["queued_recovery"])
                    {
                        member(&mut hash, key, fields.get(key).map(|v| v.get()));
                    }
                }
                _ => member(&mut hash, name, raw),
            }
        } else {
            member(&mut hash, name, raw);
        }
    }
    hex::encode(hash.finalize())
}

#[cfg(test)]
mod tests;
