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

    /// Whether a daemon's stored reading is under the floor.
    pub fn daemon_pressure(&self, disk_json: Option<&str>) -> Option<DiskPressureKind> {
        self.pressure_of(parse(disk_json).as_ref())
    }

    /// A reading with the floor applied, for a read surface.
    pub fn applied(&self, facts: Option<MachineDiskFacts>) -> Option<MachineDisk> {
        Some(MachineDisk::new(facts?, &self.floor()?))
    }
}

fn parse(disk_json: Option<&str>) -> Option<MachineDiskFacts> {
    serde_json::from_str(disk_json?).ok()
}

/// Keep a daemon's reading from its report.
pub async fn record_daemon_disk(
    db: &crate::SqliteDb,
    daemon_id: &str,
    facts: &MachineDiskFacts,
) -> Result<()> {
    let json =
        serde_json::to_string(facts).map_err(|error| crate::DbError::Check(error.to_string()))?;
    sqlx::query("UPDATE daemon SET disk_json = ? WHERE id = ? AND removed_at IS NULL")
        .bind(json)
        .bind(daemon_id)
        .execute(db.pool())
        .await?;
    Ok(())
}

/// A daemon's last reported reading.
pub async fn daemon_disk(db: &crate::SqliteDb, daemon_id: &str) -> Result<Option<MachineDiskFacts>> {
    let json: Option<Option<String>> =
        sqlx::query_scalar("SELECT disk_json FROM daemon WHERE id = ?")
            .bind(daemon_id)
            .fetch_optional(db.pool())
            .await?;
    Ok(parse(json.flatten().as_deref()))
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
        if let Some(disk) = admission.applied(parse(disk_json.as_deref())) {
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
        let low = serde_json::to_string(&facts(99)).unwrap();
        let fine = serde_json::to_string(&facts(100)).unwrap();
        assert_eq!(
            admission.daemon_pressure(Some(&low)),
            Some(DiskPressureKind::Bytes)
        );
        assert_eq!(admission.daemon_pressure(Some(&fine)), None);
        assert_eq!(admission.daemon_pressure(None), None);
        assert_eq!(admission.daemon_pressure(Some("not json")), None);
    }
}
