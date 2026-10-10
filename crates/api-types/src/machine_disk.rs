//! Free space of the filesystem holding a machine's workspace root, and the
//! floor under which Forge starts no new disk-consuming work there.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Capacity-wait capability recorded on a Task's dispatch disposition; the
/// `capacity_scope` beside it names what the Task waits for.
pub const CAPACITY_SCOPE_DISK: &str = "disk";
/// `workspace_gc_status.state` (and a daemon's reported `gc_state`) of a root
/// whose garbage collector runs.
pub const WORKSPACE_GC_OWNED: &str = "owned";

/// The free-space floor of a workspace root and the higher mark at which its
/// garbage collector runs at once instead of on its timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct DiskFloor {
    /// Bytes free under which no new worktree or check checkout starts. The
    /// larger of this and `min_free_percent` of the filesystem applies.
    #[ts(type = "number")]
    pub min_free_bytes: u64,
    pub min_free_percent: u8,
    /// Percent of the filesystem's inodes free under which the same holds.
    /// `0` turns the inode floor off.
    pub min_free_inode_percent: u8,
    /// Bytes free under which the collector runs at once. Unset: twice the
    /// floor.
    #[ts(type = "number | null")]
    pub gc_free_bytes: Option<u64>,
    pub gc_free_percent: Option<u8>,
}

impl Default for DiskFloor {
    /// 10 GiB or 5 % of the filesystem, whichever is larger; 5 % of its
    /// inodes; the collector runs at once under twice the byte floor.
    fn default() -> Self {
        Self {
            min_free_bytes: 10 * 1024 * 1024 * 1024,
            min_free_percent: 5,
            min_free_inode_percent: 5,
            gc_free_bytes: None,
            gc_free_percent: None,
        }
    }
}

impl DiskFloor {
    /// A floor on bytes alone: no inode floor.
    pub fn of_bytes(min_free_bytes: u64, min_free_percent: u8) -> Self {
        Self {
            min_free_bytes,
            min_free_percent,
            min_free_inode_percent: 0,
            gc_free_bytes: None,
            gc_free_percent: None,
        }
    }

    /// The byte floor on a filesystem of `total` bytes.
    pub fn bytes(&self, total: u64) -> u64 {
        self.min_free_bytes
            .max(total / 100 * u64::from(self.min_free_percent.min(100)))
    }

    /// The inode floor on a filesystem of `total` inodes.
    pub fn inodes(&self, total: u64) -> u64 {
        total / 100 * u64::from(self.min_free_inode_percent.min(100))
    }

    /// Bytes free under which the collector runs at once: the configured
    /// mark, never below the floor, and twice the floor when unset.
    pub fn gc_bytes(&self, total: u64) -> u64 {
        let floor = self.bytes(total);
        match (self.gc_free_bytes, self.gc_free_percent) {
            (None, None) => floor.saturating_mul(2),
            (bytes, percent) => bytes
                .unwrap_or(0)
                .max(total / 100 * u64::from(percent.unwrap_or(0).min(100)))
                .max(floor),
        }
    }

    /// What `facts` is short of, if anything. A filesystem that reports no
    /// inode counts has no inode floor.
    pub fn pressure(&self, facts: &MachineDiskFacts) -> Option<DiskPressureKind> {
        if facts.free_bytes < self.bytes(facts.total_bytes) {
            return Some(DiskPressureKind::Bytes);
        }
        match (facts.free_inodes, facts.total_inodes) {
            (Some(free), Some(total)) if total > 0 && free < self.inodes(total) => {
                Some(DiskPressureKind::Inodes)
            }
            _ => None,
        }
    }

    /// Whether the collector should run now rather than on its timer.
    pub fn wants_gc(&self, facts: &MachineDiskFacts) -> bool {
        facts.free_bytes < self.gc_bytes(facts.total_bytes) || self.pressure(facts).is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum DiskPressureKind {
    Bytes,
    Inodes,
}

impl DiskPressureKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Bytes => "bytes",
            Self::Inodes => "inodes",
        }
    }
}

/// One reading of the filesystem holding a machine's workspace root. The
/// server reads its own; a daemon sends its reading with every report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct MachineDiskFacts {
    #[ts(type = "number")]
    pub free_bytes: u64,
    #[ts(type = "number")]
    pub total_bytes: u64,
    #[serde(default)]
    #[ts(type = "number | null")]
    pub free_inodes: Option<u64>,
    #[serde(default)]
    #[ts(type = "number | null")]
    pub total_inodes: Option<u64>,
    pub measured_at: String,
    /// Whether the machine's workspace garbage collector runs on this root:
    /// `owned`, or why not (`unclaimed`, `claimed_by_other`, `refused`).
    #[serde(default)]
    pub gc_state: Option<String>,
}

/// A machine's reading with the floor applied to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct MachineDisk {
    #[serde(flatten)]
    pub facts: MachineDiskFacts,
    /// The byte floor on this filesystem.
    #[ts(type = "number")]
    pub floor_bytes: u64,
    /// What the machine is short of. While set, no new worktree or check
    /// checkout starts there; work in an existing worktree carries on.
    pub pressure: Option<DiskPressureKind>,
}

impl MachineDisk {
    pub fn new(facts: MachineDiskFacts, floor: &DiskFloor) -> Self {
        Self {
            floor_bytes: floor.bytes(facts.total_bytes),
            pressure: floor.pressure(&facts),
            facts,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(free: u64, total: u64, inodes: Option<(u64, u64)>) -> MachineDiskFacts {
        MachineDiskFacts {
            free_bytes: free,
            total_bytes: total,
            free_inodes: inodes.map(|(free, _)| free),
            total_inodes: inodes.map(|(_, total)| total),
            measured_at: "2026-10-10T00:00:00Z".to_owned(),
            gc_state: None,
        }
    }

    #[test]
    fn floor_is_the_larger_of_bytes_and_percent_and_inodes_have_their_own() {
        let floor = DiskFloor {
            min_free_bytes: 100,
            min_free_percent: 5,
            min_free_inode_percent: 5,
            gc_free_bytes: None,
            gc_free_percent: None,
        };
        assert_eq!(floor.bytes(1_000), 100);
        assert_eq!(floor.bytes(10_000), 500);
        assert_eq!(floor.pressure(&facts(499, 10_000, None)), Some(DiskPressureKind::Bytes));
        assert_eq!(floor.pressure(&facts(500, 10_000, None)), None);
        // Plenty of bytes, no inodes left.
        assert_eq!(
            floor.pressure(&facts(9_000, 10_000, Some((49, 1_000)))),
            Some(DiskPressureKind::Inodes)
        );
        assert_eq!(floor.pressure(&facts(9_000, 10_000, Some((50, 1_000)))), None);
        // A filesystem that counts no inodes has no inode floor.
        assert_eq!(floor.pressure(&facts(9_000, 10_000, Some((0, 0)))), None);
        let off = DiskFloor { min_free_inode_percent: 0, ..floor };
        assert_eq!(off.pressure(&facts(9_000, 10_000, Some((0, 1_000)))), None);
    }

    #[test]
    fn collector_mark_defaults_to_twice_the_floor_and_is_never_below_it() {
        let mut floor = DiskFloor {
            min_free_bytes: 100,
            min_free_percent: 0,
            min_free_inode_percent: 0,
            gc_free_bytes: None,
            gc_free_percent: None,
        };
        assert_eq!(floor.gc_bytes(10_000), 200);
        assert!(floor.wants_gc(&facts(150, 10_000, None)));
        assert!(floor.pressure(&facts(150, 10_000, None)).is_none());
        assert!(!floor.wants_gc(&facts(200, 10_000, None)));
        floor.gc_free_bytes = Some(50);
        assert_eq!(floor.gc_bytes(10_000), 100);
        floor.gc_free_percent = Some(30);
        assert_eq!(floor.gc_bytes(10_000), 3_000);
    }

    #[test]
    fn a_report_without_inode_counts_or_collector_state_still_parses() {
        let parsed: MachineDiskFacts = serde_json::from_str(
            r#"{"free_bytes":1,"total_bytes":2,"measured_at":"2026-10-10T00:00:00Z"}"#,
        )
        .unwrap();
        assert_eq!(parsed, facts(1, 2, None));
    }
}
