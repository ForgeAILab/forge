//! Model watchdogs, bounded waits and state diagnostics.

use super::*;

// ---------------------------------------------------------------------------
// Hard deadlines
// ---------------------------------------------------------------------------

/// The longest one wait of the model may take: a drain, a dispatcher pass, a
/// request, a start. Each of them takes well under a second when it works.
pub(super) const WAIT_LIMIT: Duration = Duration::from_secs(120);

/// The longest a pre-queue fixture test may take, start to settlement.
pub(super) const TEST_LIMIT: Duration = Duration::from_secs(300);

/// What a wait that never returns is reported from: where the model is, and
/// enough of the stack to read its state without the `World` (which the
/// stuck future borrows) and without the stack's own pool (which may be what
/// is stuck).
#[derive(Default)]
pub(super) struct Probe {
    pub(super) name: String,
    pub(super) waits: Mutex<Vec<(String, std::time::Instant)>>,
    pub(super) database: Mutex<Option<PathBuf>>,
    pub(super) script: Mutex<Option<Arc<Script>>>,
}

impl Probe {
    pub(super) fn new(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            ..Self::default()
        })
    }

    /// Every table that says who owes a Task its next move, read on a
    /// connection of its own, and the in-process state no table has.
    pub(super) async fn dump(&self) -> String {
        let mut out = format!("==== STATE DUMP {} ====\n", self.name);
        for (what, since) in self.waits.lock().unwrap().iter() {
            out.push_str(&format!("waiting {:?} in: {what}\n", since.elapsed()));
        }
        out.push_str(&format!("run cap {:?}\n", run_cap()));
        if let Some(script) = self.script.lock().unwrap().as_ref() {
            out.push_str(&format!(
                "scripted runs waiting for the model (task -> execution): {:?}\n",
                script
                    .waiting
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(task, (execution, _))| (task.clone(), execution.clone()))
                    .collect::<Vec<_>>()
            ));
        }
        out.push_str(&format!(
            "owner checkout locks of the whole test process (key, held, holders and waiters): {:?}\n",
            db::integration_owner_locks()
        ));
        let Some(database) = self.database.lock().unwrap().clone() else {
            return out + "no database yet\n";
        };
        match tokio::time::timeout(Duration::from_secs(30), Self::tables(&database)).await {
            Ok(tables) => out.push_str(&tables),
            Err(_) => out.push_str("the tables could not be read within 30 s\n"),
        }
        out
    }

    pub(super) async fn tables(database: &Path) -> String {
        use sqlx::{Column, Connection, Row, ValueRef};
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(database)
            .read_only(true)
            .busy_timeout(Duration::from_secs(2));
        let mut connection = match sqlx::SqliteConnection::connect_with(&options).await {
            Ok(connection) => connection,
            Err(error) => return format!("the database could not be opened: {error}\n"),
        };
        let mut out = String::new();
        for (title, query) in [
            (
                "machine slots",
                "SELECT (SELECT COUNT(*) FROM execution WHERE status='running') AS running_executions,
                        (SELECT COUNT(*) FROM execution WHERE status IN ('pending','queued')) AS pending_executions,
                        (SELECT COUNT(*) FROM check_run WHERE admitted_at IS NOT NULL AND state IN ('running','cancelling','cleaning','uncertain')) AS admitted_checks,
                        (SELECT COUNT(*) FROM check_run WHERE state='queued') AS queued_checks,
                        (SELECT COUNT(*) FROM task_schedule_wait WHERE daemon_id='*') AS run_slot_waiters,
                        strftime('%Y-%m-%dT%H:%M:%fZ','now') AS now",
            ),
            ("task", "SELECT * FROM task ORDER BY created_at, rowid"),
            ("task_step (open, failed, or the last 12)", "SELECT * FROM task_step WHERE status NOT IN ('done','completed','cancelled','superseded') OR last_error IS NOT NULL OR rowid IN (SELECT rowid FROM task_step ORDER BY rowid DESC LIMIT 12) ORDER BY rowid"),
            ("integration_queue", "SELECT * FROM integration_queue"),
            ("integration_attempt", "SELECT * FROM integration_attempt ORDER BY created_at, rowid"),
            ("check_run", "SELECT * FROM check_run ORDER BY rowid"),
            ("check_consumer", "SELECT * FROM check_consumer ORDER BY rowid"),
            ("execution", "SELECT * FROM execution ORDER BY created_at, rowid"),
            ("task_schedule_wait", "SELECT * FROM task_schedule_wait"),
            ("task_schedule_park", "SELECT * FROM task_schedule_park"),
            ("task_schedule_dirty", "SELECT * FROM task_schedule_dirty"),
            ("project_schedule_dirty", "SELECT * FROM project_schedule_dirty"),
            ("task_slot_wait_age", "SELECT * FROM task_slot_wait_age"),
            ("task_budget", "SELECT * FROM task_budget"),
            ("workspace_lease", "SELECT * FROM workspace_lease"),
            ("worker_health", "SELECT * FROM worker_health"),
            ("worker_item_failure", "SELECT * FROM worker_item_failure ORDER BY rowid DESC LIMIT 20"),
            ("worker_dead_letter", "SELECT * FROM worker_dead_letter ORDER BY rowid DESC LIMIT 20"),
            ("transition_log (last 30)", "SELECT task_id, from_state, to_state, triggered_by, bridge_kind, substr(trigger_reason,1,160) AS reason, created_at FROM transition_log ORDER BY created_at DESC, rowid DESC LIMIT 30"),
        ] {
            out.push_str(&format!("-- {title}\n"));
            let rows = match sqlx::query(query).fetch_all(&mut connection).await {
                Ok(rows) => rows,
                Err(error) => {
                    out.push_str(&format!("   unreadable: {error}\n"));
                    continue;
                }
            };
            for row in rows {
                let mut line = String::new();
                for (at, column) in row.columns().iter().enumerate() {
                    if row.try_get_raw(at).map_or(true, |value| value.is_null()) {
                        continue;
                    }
                    let value = row
                        .try_get::<String, _>(at)
                        .or_else(|_| row.try_get::<i64, _>(at).map(|n| n.to_string()))
                        .or_else(|_| row.try_get::<f64, _>(at).map(|n| n.to_string()))
                        .unwrap_or_else(|_| "<blob>".to_owned());
                    if value.is_empty() || value == "{}" || value == "[]" {
                        continue;
                    }
                    let short: String = value.chars().take(400).collect();
                    line.push_str(&format!("{}={short} | ", column.name()));
                }
                out.push_str(&format!("   {line}\n"));
            }
        }
        let _ = connection.close().await;
        out
    }

    /// The dump, also straight to the process's stderr: libtest shows a
    /// test's captured output only once the whole run ends.
    pub(super) async fn report(&self, what: &str) -> String {
        use std::io::Write;
        let report = format!("HANG {}: {what}\n{}", self.name, self.dump().await);
        let _ = writeln!(std::io::stderr(), "{report}");
        report
    }

    /// The backstop for a test thread that blocks (a lock, a child process)
    /// where no timer of its own runtime can fire: another thread reports the
    /// state and ends the test process.
    pub(super) fn watchdog(self: &Arc<Self>, limit: Duration) -> std::sync::mpsc::Sender<()> {
        let (done, wait) = std::sync::mpsc::channel::<()>();
        let probe = Arc::clone(self);
        std::thread::spawn(move || {
            if wait.recv_timeout(limit) != Err(std::sync::mpsc::RecvTimeoutError::Timeout) {
                return;
            }
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("the watchdog's runtime builds");
            runtime.block_on(probe.report(&format!(
                "the test did not end within {limit:?} and its own deadline did not fire; ending the test process"
            )));
            std::process::exit(101);
        });
        done
    }
}

/// `wait`, or a panic with the state dump when it has not returned within
/// `WAIT_LIMIT`. Nothing the model awaits may wait longer than that.
pub(super) async fn bounded<T>(probe: &Probe, what: &str, wait: impl Future<Output = T>) -> T {
    probe
        .waits
        .lock()
        .unwrap()
        .push((what.to_owned(), std::time::Instant::now()));
    match tokio::time::timeout(WAIT_LIMIT, wait).await {
        Ok(value) => {
            probe.waits.lock().unwrap().pop();
            value
        }
        Err(_) => panic!(
            "{}",
            probe
                .report(&format!("`{what}` did not return within {WAIT_LIMIT:?}"))
                .await
        ),
    }
}
