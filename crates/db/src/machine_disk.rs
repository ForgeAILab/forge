//! Disk-pressure admission facts: the server's own reading of its workspace
//! root and each daemon's last reported one, held to one floor.
//!
//! Nothing here refuses anything by itself. Placement and check admission
//! ask [`DiskAdmission`] whether a machine is under the floor before they
//! start work that needs new disk there. A handle nobody configured (every
//! test, every tool) knows no floor and never reports pressure, and a
//! reading that cannot be taken refuses nothing: the disk failing to answer
//! must not stop work that the disk may well have room for.

use crate::Result;
use api_types::{DiskFloor, DiskPressureKind, MachineDisk, MachineDiskFacts};
use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

/// How long one reading of the server's root answers admissions.
const READING_TTL: Duration = Duration::from_secs(5);
/// How long a daemon's reported reading decides anything. A daemon reports
/// every minute; one that has been silent for this long (disconnected,
/// stopped, wedged) has a disk nobody knows: its old reading neither
/// refuses work (the Task waits for the owner instead, or another machine
/// takes it) nor counts as "under the floor" on any surface. The daemon's
/// own check at `workspace.prepare` is what keeps a full disk from being
/// filled on an old over-floor reading.
pub const DAEMON_READING_TTL: Duration = Duration::from_secs(5 * 60);

pub type DiskReader = Arc<dyn Fn() -> Option<MachineDiskFacts> + Send + Sync>;

#[derive(Default)]
struct State {
    floor: Option<DiskFloor>,
    reader: Option<DiskReader>,
    reading: Option<(Instant, Option<MachineDiskFacts>)>,
    unreadable_logged: bool,
}

/// The server's disk-admission state, shared through [`crate::SqliteDb`]
/// like the server run cap.
#[derive(Default)]
pub struct DiskAdmission {
    state: RwLock<State>,
}

impl std::fmt::Debug for DiskAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskAdmission")
            .field("floor", &self.floor())
            .finish()
    }
}

impl DiskAdmission {
    /// Turn admission on: the floor every machine is held to and how the
    /// server reads its own workspace root. Called by the running server.
    pub fn configure(&self, floor: DiskFloor, reader: DiskReader) {
        let mut state = self.state.write().unwrap_or_else(|p| p.into_inner());
        state.floor = Some(floor);
        state.reader = Some(reader);
        state.reading = None;
        state.unreadable_logged = false;
    }

    pub fn floor(&self) -> Option<DiskFloor> {
        self.state.read().unwrap_or_else(|p| p.into_inner()).floor
    }

    /// The server's reading, at most [`READING_TTL`] old.
    pub fn server_facts(&self) -> Option<MachineDiskFacts> {
        {
            let state = self.state.read().unwrap_or_else(|p| p.into_inner());
            match &state.reading {
                Some((at, facts)) if at.elapsed() < READING_TTL => return facts.clone(),
                _ => {}
            }
        }
        self.refresh()
    }

    /// Read the server's root now, whatever the cached reading says.
    pub fn refresh(&self) -> Option<MachineDiskFacts> {
        let reader = self
            .state
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .reader
            .clone()?;
        // Outside the lock: the read is a system call.
        let facts = reader();
        let mut state = self.state.write().unwrap_or_else(|p| p.into_inner());
        if facts.is_none() && !state.unreadable_logged {
            tracing::warn!("free space of the workspace root cannot be read; disk-pressure admission is off for the server until it can (nothing is refused for disk)");
        }
        state.unreadable_logged = facts.is_none();
        state.reading = Some((Instant::now(), facts.clone()));
        facts
    }

    /// What `facts` is short of under the configured floor. No floor, or no
    /// reading: nothing.
    pub fn pressure_of(&self, facts: Option<&MachineDiskFacts>) -> Option<DiskPressureKind> {
        self.floor()?.pressure(facts?)
    }

    /// Whether the server's own root is under the floor.
    pub fn server_pressure(&self) -> Option<DiskPressureKind> {
        self.floor()?;
        self.pressure_of(self.server_facts().as_ref())
    }

    /// Whether a daemon's stored reading is under the floor. A reading
    /// older than [`DAEMON_READING_TTL`] refuses nothing.
    pub fn daemon_pressure(&self, disk_json: Option<&str>) -> Option<DiskPressureKind> {
        self.daemon_pressure_at(disk_json, chrono::Utc::now())
    }

    pub fn daemon_pressure_at(
        &self,
        disk_json: Option<&str>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Option<DiskPressureKind> {
        self.pressure_of(
            parse(disk_json)
                .filter(|facts| is_fresh(facts, now))
                .as_ref(),
        )
    }

    /// A daemon's stored reading with the floor applied, for a read
    /// surface. A stale reading is still shown (with the time it was
    /// taken) but is never "under the floor".
    pub fn applied_daemon(&self, disk_json: Option<&str>) -> Option<MachineDisk> {
        let facts = parse(disk_json)?;
        let fresh = is_fresh(&facts, chrono::Utc::now());
        let mut disk = self.applied(Some(facts))?;
        if !fresh {
            disk.pressure = None;
        }
        Some(disk)
    }

    /// A reading with the floor applied, for a read surface.
    pub fn applied(&self, facts: Option<MachineDiskFacts>) -> Option<MachineDisk> {
        Some(MachineDisk::new(facts?, &self.floor()?))
    }
}

fn parse(disk_json: Option<&str>) -> Option<MachineDiskFacts> {
    serde_json::from_str(disk_json?).ok()
}

/// `measured_at` of a stored daemon reading is the server's clock at the
/// report (see [`record_daemon_disk`]). An unparseable time is not fresh.
fn is_fresh(facts: &MachineDiskFacts, now: chrono::DateTime<chrono::Utc>) -> bool {
    chrono::DateTime::parse_from_rfc3339(&facts.measured_at).is_ok_and(|at| {
        now.signed_duration_since(at.with_timezone(&chrono::Utc))
            .to_std()
            // A reading from the future (the clock was set back) is fresh.
            .map_or(true, |age| age < DAEMON_READING_TTL)
    })
}

/// Keep a daemon's reading from its report, stamped with the server's clock:
/// how old a reading is must not depend on the daemon's clock.
pub async fn record_daemon_disk(
    db: &crate::SqliteDb,
    daemon_id: &str,
    facts: &MachineDiskFacts,
) -> Result<()> {
    let facts = MachineDiskFacts {
        measured_at: crate::now_rfc3339(),
        ..facts.clone()
    };
    let json =
        serde_json::to_string(&facts).map_err(|error| crate::DbError::Check(error.to_string()))?;
    sqlx::query("UPDATE daemon SET disk_json = ? WHERE id = ? AND removed_at IS NULL")
        .bind(json)
        .bind(daemon_id)
        .execute(db.pool())
        .await?;
    Ok(())
}

/// What a daemon is short of by its last reported reading, while that
/// reading is fresh and the daemon is registered.
pub async fn daemon_pressure(
    db: &crate::SqliteDb,
    daemon_id: &str,
) -> Result<Option<DiskPressureKind>> {
    let json: Option<Option<String>> =
        sqlx::query_scalar("SELECT disk_json FROM daemon WHERE id = ? AND removed_at IS NULL")
            .bind(daemon_id)
            .fetch_optional(db.pool())
            .await?;
    Ok(db.disk_admission.daemon_pressure(json.flatten().as_deref()))
}

/// One machine and its reading with the floor applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineDiskRow {
    /// `None` is the server host (direct and embedded execution).
    pub daemon_id: Option<String>,
    pub hostname: String,
    pub disk: MachineDisk,
}

/// Every machine with a reading: the server host first, then each daemon
/// that is not the server's own embedded one. Empty until admission is
/// configured.
pub async fn list_machine_disks(db: &crate::SqliteDb) -> Result<Vec<MachineDiskRow>> {
    let admission = &db.disk_admission;
    if admission.floor().is_none() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    if let Some(disk) = admission.applied(admission.server_facts()) {
        rows.push(MachineDiskRow {
            daemon_id: None,
            hostname: "Server host".to_owned(),
            disk,
        });
    }
    let daemons: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT id, hostname, disk_json FROM daemon
         WHERE removed_at IS NULL AND machine_id <> ? ORDER BY hostname, id",
    )
    .bind(db.server_run_cap.embedded_machine_id())
    .fetch_all(db.pool())
    .await?;
    for (id, hostname, disk_json) in daemons {
        if let Some(disk) = admission.applied_daemon(disk_json.as_deref()) {
            rows.push(MachineDiskRow {
                daemon_id: Some(id),
                hostname,
                disk,
            });
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn facts(free: u64) -> MachineDiskFacts {
        MachineDiskFacts {
            free_bytes: free,
            total_bytes: 1_000,
            free_inodes: None,
            total_inodes: None,
            measured_at: "2026-10-10T00:00:00Z".to_owned(),
            gc_state: None,
        }
    }

    /// A daemon's stored reading, taken just now.
    fn reported(free: u64) -> String {
        serde_json::to_string(&MachineDiskFacts {
            measured_at: chrono::Utc::now().to_rfc3339(),
            ..facts(free)
        })
        .unwrap()
    }

    #[test]
    fn unconfigured_admission_never_reports_pressure() {
        let admission = DiskAdmission::default();
        assert_eq!(admission.server_pressure(), None);
        assert_eq!(admission.pressure_of(Some(&facts(0))), None);
        assert_eq!(admission.daemon_pressure(Some("{}")), None);
    }

    #[test]
    fn server_reading_decides_and_an_unreadable_disk_refuses_nothing() {
        let admission = DiskAdmission::default();
        let reading = Arc::new(Mutex::new(Some(facts(10))));
        let source = Arc::clone(&reading);
        admission.configure(
            DiskFloor::of_bytes(100, 0),
            Arc::new(move || source.lock().unwrap().clone()),
        );
        assert_eq!(admission.server_pressure(), Some(DiskPressureKind::Bytes));
        // Level-triggered: the next reading that recovered clears it.
        *reading.lock().unwrap() = Some(facts(500));
        assert_eq!(
            admission.server_pressure(),
            Some(DiskPressureKind::Bytes),
            "one reading answers admissions for a few seconds"
        );
        assert_eq!(admission.refresh(), Some(facts(500)));
        assert_eq!(admission.server_pressure(), None);
        // Unreadable: fail open.
        *reading.lock().unwrap() = None;
        assert_eq!(admission.refresh(), None);
        assert_eq!(admission.server_pressure(), None);
    }

    #[test]
    fn daemon_reading_is_held_to_the_same_floor_and_a_missing_one_refuses_nothing() {
        let admission = DiskAdmission::default();
        admission.configure(DiskFloor::of_bytes(100, 0), Arc::new(|| None));
        let (low, fine) = (reported(99), reported(100));
        assert_eq!(
            admission.daemon_pressure(Some(&low)),
            Some(DiskPressureKind::Bytes)
        );
        assert_eq!(admission.daemon_pressure(Some(&fine)), None);
        assert_eq!(admission.daemon_pressure(None), None);
        assert_eq!(admission.daemon_pressure(Some("not json")), None);
    }

    /// A daemon that stopped reporting has a disk nobody knows. Its last
    /// reading must not keep a Task waiting (another machine may take it,
    /// or the Task waits for the owner, which is a different, visible
    /// wait), and it must not be shown as under the floor for ever.
    #[test]
    fn a_stale_daemon_reading_refuses_nothing_and_is_not_shown_as_pressure() {
        let admission = DiskAdmission::default();
        admission.configure(DiskFloor::of_bytes(100, 0), Arc::new(|| None));
        let now = chrono::Utc::now();
        let at = |age: i64| {
            serde_json::to_string(&MachineDiskFacts {
                measured_at: (now - chrono::Duration::seconds(age)).to_rfc3339(),
                ..facts(10)
            })
            .unwrap()
        };
        let ttl = DAEMON_READING_TTL.as_secs() as i64;
        assert_eq!(
            admission.daemon_pressure_at(Some(&at(ttl - 5)), now),
            Some(DiskPressureKind::Bytes)
        );
        assert_eq!(admission.daemon_pressure_at(Some(&at(ttl + 5)), now), None);
        // A clock set back does not make every reading stale.
        assert_eq!(
            admission.daemon_pressure_at(Some(&at(-3600)), now),
            Some(DiskPressureKind::Bytes)
        );
        let shown = admission.applied_daemon(Some(&at(ttl + 5))).unwrap();
        assert_eq!((shown.pressure, shown.facts.free_bytes), (None, 10));
        assert_eq!(
            admission.applied_daemon(Some(&at(0))).unwrap().pressure,
            Some(DiskPressureKind::Bytes)
        );
        // A time nobody can read is not fresh.
        assert_eq!(
            admission.daemon_pressure(Some(
                &serde_json::to_string(&facts(10))
                    .unwrap()
                    .replace("2026-10-10T00:00:00Z", "yesterday")
            )),
            None
        );
    }
}
