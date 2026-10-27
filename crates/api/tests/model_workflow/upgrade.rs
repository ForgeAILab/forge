//! Pre-queue fixture restoration, upgrade observations and settlement helpers.

use super::*;

// ===========================================================================
// Upgrade: databases and repositories the last pre-queue build really wrote
// (`tests/fixtures/pre_queue/README.md`), restored and started on this build.
// ===========================================================================

/// The machine id the fixtures' daemon row carries (scrubbed when packed).
pub(super) const PRE_QUEUE_MACHINE: &str = "embedded:fixture-host:macos:aarch64";

/// Replace `from` by `to` in every text file under `dir`: the database dump,
/// the manifest and Git's worktree link files. Git objects are not text.
pub(super) fn reroot(dir: &Path, from: &str, to: &str) -> Result<(), String> {
    for entry in std::fs::read_dir(dir).map_err(|error| error.to_string())? {
        let path = entry.map_err(|error| error.to_string())?.path();
        let meta = std::fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
        if meta.is_dir() {
            reroot(&path, from, to)?;
        } else if meta.is_file() && meta.len() < 16 * 1024 * 1024 {
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            if text.contains(from) {
                std::fs::write(&path, text.replace(from, to)).map_err(|error| error.to_string())?;
            }
        }
    }
    Ok(())
}

/// What a start must leave alone the second time: every Task's state,
/// condition, offers, attempts and history, and every branch.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct UpgradePrint {
    pub(super) tasks: Vec<String>,
    pub(super) refs: String,
}

impl World {
    /// Restore the fixture into a fresh directory, re-rooted there, and start
    /// this build on it. Returns the fixture's manifest and the first
    /// start's import report.
    pub(super) async fn restore(
        name: &str,
        shadow: bool,
        probe: Arc<Probe>,
    ) -> Result<
        (
            Self,
            Value,
            services::integration_steps::import::ImportReport,
        ),
        String,
    > {
        let io = |error: std::io::Error| error.to_string();
        let dir = tempfile::tempdir().map_err(io)?;
        let unpack = tempfile::tempdir().map_err(io)?;
        let archive =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pre_queue/pre_queue.tar.xz");
        let tar = Command::new("tar")
            .arg("-xJf")
            .arg(&archive)
            .arg("-C")
            .arg(unpack.path())
            .arg(name)
            .output()
            .map_err(io)?;
        if !tar.status.success() {
            return Err(format!(
                "unpacking {name}: {}",
                String::from_utf8_lossy(&tar.stderr)
            ));
        }
        for entry in std::fs::read_dir(unpack.path().join(name)).map_err(io)? {
            let entry = entry.map_err(io)?;
            std::fs::rename(entry.path(), dir.path().join(entry.file_name())).map_err(io)?;
        }
        let manifest: Value = serde_json::from_str(
            &std::fs::read_to_string(dir.path().join("manifest.json")).map_err(io)?,
        )
        .map_err(|error| error.to_string())?;
        let old_root = manifest["root"]
            .as_str()
            .ok_or("the manifest names its root")?;
        reroot(dir.path(), old_root, &dir.path().display().to_string())?;
        reroot(
            dir.path(),
            PRE_QUEUE_MACHINE,
            &services::embedded_daemon::embedded_machine_id(),
        )?;
        let dump = std::fs::read_to_string(dir.path().join("db.sql")).map_err(io)?;
        if dump.contains(old_root) {
            return Err(format!("{name}: the dump still names {old_root}"));
        }
        {
            use sqlx::Connection;
            let options = sqlx::sqlite::SqliteConnectOptions::new()
                .filename(dir.path().join("forge.db"))
                .foreign_keys(false)
                .create_if_missing(true);
            let mut connection = sqlx::SqliteConnection::connect_with(&options)
                .await
                .map_err(|error| error.to_string())?;
            sqlx::raw_sql(&dump)
                .execute(&mut connection)
                .await
                .map_err(|error| format!("loading the dump of {name}: {error}"))?;
            if !shadow {
                // An install that never ran the passive queue stages (the
                // last release) has no attempt and no queue row at all.
                sqlx::raw_sql("DELETE FROM integration_attempt; DELETE FROM integration_queue;")
                    .execute(&mut connection)
                    .await
                    .map_err(|error| format!("removing the shadow rows of {name}: {error}"))?;
            }
            connection
                .close()
                .await
                .map_err(|error| error.to_string())?;
        }
        std::fs::remove_file(dir.path().join("db.sql")).map_err(io)?;
        std::fs::create_dir_all(dir.path().join("web")).map_err(io)?;
        std::fs::write(dir.path().join("web/index.html"), "<html></html>").map_err(io)?;

        let shared = Arc::new(Shared::default());
        let mut tasks = Vec::new();
        for task in manifest["tasks"].as_array().into_iter().flatten() {
            let id = task["id"].as_str().unwrap_or_default().to_owned();
            shared.index.lock().unwrap().insert(id.clone(), tasks.len());
            tasks.push(id);
        }
        // Later than every write of the run that made the fixture.
        shared.writes.store(1000, Ordering::SeqCst);
        let live = Self::boot(dir.path(), &shared, &probe).await;
        let agent = |name: &'static str| {
            let pool = live.pool.clone();
            async move {
                sqlx::query_scalar::<_, String>("SELECT id FROM agent_identity WHERE name = ?")
                    .bind(name)
                    .fetch_one(&pool)
                    .await
                    .map_err(|error| format!("{name}: {error}"))
            }
        };
        let (coder_id, reviewer_id) = (agent("model-coder").await?, agent("model-reviewer").await?);
        let report = bounded(&probe, "the first start on the fixture", Self::start(&live)).await?;
        let world = Self {
            _storm: lock_storm(dir.path()),
            dir,
            probe,
            live: Some(live),
            shared,
            project_id: manifest["project_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            coder_id,
            reviewer_id,
            project_paused: manifest["project_paused"] == true,
            agent_paused: false,
            avoid_open_findings: false,
            tasks,
            settled: BTreeMap::new(),
            trace: Vec::new(),
        };
        Ok((world, manifest, report))
    }

    pub(super) fn main_sha(&self) -> Result<String, String> {
        git(
            &self.dir.path().join("repo"),
            &["rev-parse", "refs/heads/main"],
        )
    }

    /// Whether `commit` is on main.
    pub(super) fn on_main(&self, commit: &str) -> bool {
        git(
            &self.dir.path().join("repo"),
            &["merge-base", "--is-ancestor", commit, "refs/heads/main"],
        )
        .is_ok()
    }

    pub(super) async fn upgrade_print(&self) -> Result<UpgradePrint, String> {
        let sql = |error: sqlx::Error| error.to_string();
        let pool = &self.live().pool;
        let mut tasks = Vec::new();
        for (index, id) in self.tasks.iter().enumerate() {
            let task = self.task(index).await?;
            let (_, offers) = self.offers(index).await?;
            let verbs: Vec<String> = offers
                .iter()
                .map(|offer| format!("{}:{}", offer["action"]["verb"], offer["reason"]))
                .collect();
            let attempts: Vec<(String, String, i64, Option<String>)> = sqlx::query_as(
                "SELECT state, admission_key, current, failure_message FROM integration_attempt
                 WHERE task_ref = ? ORDER BY created_at, rowid",
            )
            .bind(id)
            .fetch_all(pool)
            .await
            .map_err(sql)?;
            let history: Vec<(String, String, Option<String>)> = sqlx::query_as(
                "SELECT from_state, to_state, bridge_kind FROM transition_log
                 WHERE task_id = ? ORDER BY created_at, rowid",
            )
            .bind(id)
            .fetch_all(pool)
            .await
            .map_err(sql)?;
            let reviews: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM review WHERE task_id = ?")
                .bind(id)
                .fetch_one(pool)
                .await
                .map_err(sql)?;
            let budgets: Vec<(String, i64)> = sqlx::query_as(
                "SELECT kind, spent FROM task_budget WHERE task_id = ? ORDER BY kind",
            )
            .bind(id)
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            tasks.push(format!(
                "{index}: {} condition {} / {} offers {verbs:?} attempts {attempts:?} history {history:?} reviews {reviews} budgets {budgets:?}",
                task["status"], task["condition"]["primary"], task["condition"]["additional"],
            ));
        }
        Ok(UpgradePrint {
            tasks,
            refs: git(
                &self.dir.path().join("repo"),
                &[
                    "for-each-ref",
                    "--format=%(refname) %(objectname)",
                    "refs/heads",
                ],
            )?,
        })
    }

    /// `rounds` passes of the step worker and the dispatcher (its sweep and
    /// the integration reconciler in it) over a state with nothing to do
    /// commit nothing: SQLite's `data_version`, read on a connection of its
    /// own, moves with every commit of any other connection.
    pub(super) async fn idle_passes_write_nothing(&self, rounds: usize) -> Result<(), String> {
        use sqlx::Connection;
        let sql = |error: sqlx::Error| error.to_string();
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(self.dir.path().join("forge.db"))
            .read_only(true);
        let mut watch = sqlx::SqliteConnection::connect_with(&options)
            .await
            .map_err(sql)?;
        let counts = |pool: sqlx::SqlitePool| async move {
            let mut out = Vec::new();
            for table in [
                "task_step",
                "domain_event",
                "transition_log",
                "integration_attempt",
            ] {
                let n: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                    .fetch_one(&pool)
                    .await
                    .unwrap_or(-1);
                out.push(format!("{table}={n}"));
            }
            let moved: (i64, i64) = sqlx::query_as(
                "SELECT (SELECT COALESCE(SUM(version),0) FROM task), (SELECT COALESCE(SUM(revision),0) FROM integration_attempt)",
            )
            .fetch_one(&pool)
            .await
            .unwrap_or((-1, -1));
            format!(
                "{out:?} task versions {} attempt revisions {}",
                moved.0, moved.1
            )
        };
        // Let the check worker's own start-up writes pass first.
        let mut before: i64 = -1;
        for _ in 0..40 {
            let now: i64 = sqlx::query_scalar("PRAGMA data_version")
                .fetch_one(&mut watch)
                .await
                .map_err(sql)?;
            if now == before {
                break;
            }
            before = now;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let rows = counts(self.live().pool.clone()).await;
        for _ in 0..rounds {
            for id in &self.tasks {
                self.drain(id)
                    .await
                    .map_err(|error| format!("draining an idle Task failed: {error}"))?;
            }
            self.dispatcher_pass().await?;
        }
        let after: i64 = sqlx::query_scalar("PRAGMA data_version")
            .fetch_one(&mut watch)
            .await
            .map_err(sql)?;
        watch.close().await.map_err(sql)?;
        if after != before {
            return Err(format!(
                "{rounds} idle passes committed {} write(s): rows before {rows}, after {}",
                after - before,
                counts(self.live().pool.clone()).await
            ));
        }
        Ok(())
    }

    /// One Task in one line: where it is and what its owner is told.
    pub(super) async fn upgrade_row(&self, index: usize) -> Result<String, String> {
        let task = self.task(index).await?;
        let (epoch, version): (i64, i64) =
            sqlx::query_as("SELECT status_epoch, version FROM task WHERE id = ?")
                .bind(&self.tasks[index])
                .fetch_one(&self.live().pool)
                .await
                .map_err(|error| error.to_string())?;
        let (_, offers) = self.offers(index).await?;
        let offers: Vec<String> = offers
            .iter()
            .map(|offer| {
                format!(
                    "{}({})",
                    offer["action"]["verb"].as_str().unwrap_or_default(),
                    offer["reason"].as_str().unwrap_or_default()
                )
            })
            .collect();
        let attempts: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT state, failure_message FROM integration_attempt WHERE task_ref = ? AND current = 1",
        )
        .bind(&self.tasks[index])
        .fetch_all(&self.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        let reason = |reason: &Value| match reason["kind"].as_str() {
            Some("integration") => format!(
                "integration:{}{}",
                reason["reason"]["kind"].as_str().unwrap_or_default(),
                reason["reason"]["cause"]
                    .as_str()
                    .map(|cause| format!("/{cause}"))
                    .unwrap_or_default()
            ),
            Some("failure") => format!(
                "failure:{}",
                reason["failure_kind"].as_str().unwrap_or_default()
            ),
            Some(kind) => kind.to_owned(),
            None => "-".to_owned(),
        };
        let condition = &task["condition"];
        let additional: Vec<String> = condition["additional"]
            .as_array()
            .into_iter()
            .flatten()
            .map(reason)
            .collect();
        Ok(format!(
            "{}@{epoch} v{version} {} primary {} additional {additional:?} offers {offers:?} attempt {attempts:?}",
            task["status"].as_str().unwrap_or_default(),
            condition["kind"].as_str().unwrap_or_default(),
            reason(&condition["primary"]),
        ))
    }

    /// The rows behind a failure: each Task's own columns, its attempts with
    /// their journals, its open steps and the trace.
    #[allow(clippy::type_complexity)]
    pub(super) async fn upgrade_rows(&self) -> String {
        let pool = &self.live().pool;
        let mut out = String::new();
        for id in &self.tasks {
            let task: Vec<(String, i64, i64, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
                "SELECT status,version,status_epoch,blocked_json,failed_json,error_annotation,metadata_json,condition_json FROM task WHERE id = ?",
            )
            .bind(id)
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            out.push_str(&format!("ROW task {id}: {task:?}\n"));
            let attempts: Vec<(String, String, String, i64, Option<String>, Option<String>, Option<String>, String, Option<String>)> = sqlx::query_as(
                "SELECT id,state,admission_key,current,failure_kind,failure_message,available_at,operation_receipts_json,import_source_json FROM integration_attempt WHERE task_ref = ? ORDER BY created_at, rowid",
            )
            .bind(id)
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            for attempt in attempts {
                out.push_str(&format!("ROW   attempt {attempt:?}\n"));
            }
            let steps: Vec<(String, String, String, String, Option<String>)> = sqlx::query_as(
                "SELECT kind,status,expected_status,causation_key,last_error FROM task_step WHERE task_id = ? AND (status NOT IN ('done','completed','cancelled','superseded') OR last_error IS NOT NULL) ORDER BY seq",
            )
            .bind(id)
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            for step in steps {
                out.push_str(&format!("ROW   step {step:?}\n"));
            }
        }
        format!("{out}TRACE {:#?}", self.trace)
    }

    /// The typed-statement refusal round 2 found on rows written with plain
    /// SQL, on any step of any Task.
    pub(super) async fn refused_statements(&self) -> Result<Vec<String>, String> {
        sqlx::query_scalar(
            "SELECT task_id || ' ' || kind || ': ' || last_error FROM task_step
             WHERE last_error LIKE '%differs from its legacy and owner witnesses%'",
        )
        .fetch_all(&self.live().pool)
        .await
        .map_err(|error| error.to_string())
    }
}

/// What design section 5 says the upgrade does with the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Upgraded {
    /// The merge had landed: it is reported, once, and never repeated.
    Reported,
    /// Nothing had landed: the queue merges it once, with no owner action.
    Merges,
    /// A coder repair is the Task's own flow; it merges after the repair.
    Repaired,
    /// The Task waits with a typed reason and an offered action.
    Parked,
}

/// One fixture test under its hard deadline. The body is not `Send` as a
/// whole, so it is awaited here: the deadline fires at any await, and where
/// the body blocks its thread (a lock, a child process) the watchdog thread
/// reports and ends the process a minute later.
pub(super) async fn pre_queue_upgrade(
    name: &'static str,
    shadow: bool,
    expected: Upgraded,
    take: Option<&'static str>,
) -> Result<(), String> {
    let variant = if shadow { "shadow" } else { "bare" };
    let probe = Probe::new(format!(
        "pre-queue {name} {variant}{}",
        take.map(|verb| format!(" {verb}")).unwrap_or_default()
    ));
    let _watchdog = probe.watchdog(TEST_LIMIT + Duration::from_secs(60));
    let body = pre_queue_upgrade_unbounded(name, shadow, expected, take, Arc::clone(&probe));
    match tokio::time::timeout(TEST_LIMIT, body).await {
        Ok(outcome) => outcome,
        Err(_) => panic!(
            "{}",
            probe
                .report(&format!("the test did not end within {TEST_LIMIT:?}"))
                .await
        ),
    }
}

pub(super) async fn pre_queue_upgrade_unbounded(
    name: &'static str,
    shadow: bool,
    expected: Upgraded,
    take: Option<&'static str>,
    probe: Arc<Probe>,
) -> Result<(), String> {
    let (mut world, manifest, report) = World::restore(name, shadow, probe).await?;
    let variant = if shadow { "shadow" } else { "bare" };
    match pre_queue_upgraded(
        name, variant, expected, take, &mut world, &manifest, &report,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(error) => Err(format!("{error}\n{}", world.upgrade_rows().await)),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn pre_queue_upgraded(
    name: &str,
    variant: &str,
    expected: Upgraded,
    take: Option<&'static str>,
    world: &mut World,
    manifest: &Value,
    report: &services::integration_steps::import::ImportReport,
) -> Result<(), String> {
    let old_main = manifest["main"].as_str().unwrap_or_default().to_owned();
    eprintln!(
        "UPGRADE {name}: first start imported {} quarantined {} needs_fact {} unreadable {} continuations {}",
        report.imported, report.quarantined, report.needs_fact, report.unreadable, report.continuations
    );
    if report.unreadable > 0 {
        return Err(format!(
            "{name}: {} Task(s) were unreadable to the importer",
            report.unreadable
        ));
    }
    let clock = std::time::Instant::now();
    world.quiesce().await?;
    eprintln!(
        "UPGRADE {name}: first quiescence after {:?}",
        clock.elapsed()
    );
    // A merge the queue worker put off (the old build's rebase or lock was
    // still on disk) is retried at its own time, with no owner action.
    for _ in 0..4 {
        if world.project_paused || world.move_deadlines(true).await? == 0 {
            break;
        }
        world.quiesce().await?;
    }
    eprintln!(
        "UPGRADE {name}: retry deadlines passed after {:?}",
        clock.elapsed()
    );
    let line = world.check_quiescent().await?;
    let first = world.upgrade_print().await?;
    eprintln!(
        "UPGRADE {name}: after the first start: {line}\n  {}",
        first.tasks.join("\n  ")
    );
    let refused = world.refused_statements().await?;
    if !refused.is_empty() {
        return Err(format!(
            "{name}: the typed integration statement was refused: {refused:?}"
        ));
    }
    let unknown: Vec<String> = sqlx::query_scalar(
        "SELECT kind || ' ' || expected_status || ': ' || last_error FROM task_step
         WHERE last_error LIKE '%unknown action%'",
    )
    .fetch_all(&world.live().pool)
    .await
    .map_err(|error| error.to_string())?;
    if !unknown.is_empty() {
        return Err(format!(
            "{name}: a step of the old build named a hook this build does not have: {unknown:?}"
        ));
    }

    // A second start changes nothing. A run in flight loses its process on
    // a restart, so this is asked only of a state with no run.
    let running = (0..world.tasks.len()).any(|index| world.waiting(index));
    if !running {
        world.crash_and_restart().await?;
        world.quiesce().await?;
        world.check_quiescent().await?;
        let second = world.upgrade_print().await?;
        if first != second {
            return Err(format!(
                "{name}: a second start changed the state:\n first {first:#?}\n second {second:#?}"
            ));
        }
    }

    eprintln!(
        "UPGRADE {name}: second start checked after {:?}",
        clock.elapsed()
    );
    let last = world.tasks.len() - 1;
    let subject = world.task(last).await?;
    let before: i64 = manifest["tasks"][last]["transitions"]
        .as_array()
        .map_or(0, |rows| rows.len() as i64);
    let moves = |world: &World| {
        let (pool, id) = (world.live().pool.clone(), world.tasks[last].clone());
        async move {
            let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
                "SELECT from_state, to_state, bridge_kind FROM transition_log
                 WHERE task_id = ? ORDER BY created_at, rowid LIMIT -1 OFFSET ?",
            )
            .bind(id)
            .bind(before)
            .fetch_all(&pool)
            .await
            .map_err(|error| error.to_string())?;
            Ok::<String, String>(
                rows.iter()
                    .map(|(from, to, bridge)| match bridge {
                        Some(bridge) => format!("{from}>{to}[{bridge}]"),
                        None => format!("{from}>{to}"),
                    })
                    .collect::<Vec<_>>()
                    .join(" "),
            )
        }
    };
    let runs = manifest["tasks"][last]["executions"]
        .as_array()
        .map_or(0, Vec::len);
    eprintln!(
        "TABLE {name} {variant} start: was {}@{} | now {} | moves [{}] | main {}",
        manifest["tasks"][last]["status"]
            .as_str()
            .unwrap_or_default(),
        manifest["tasks"][last]["epoch"],
        world.upgrade_row(last).await?,
        moves(world).await?,
        if world.main_sha()? == old_main {
            "unchanged"
        } else {
            "advanced"
        },
    );
    let status = subject["status"].as_str().unwrap_or_default().to_owned();
    let main_after_start = world.main_sha()?;
    match expected {
        Upgraded::Reported => {
            if status != "done" || main_after_start != old_main {
                return Err(format!(
                    "{name}: a landed merge is reported once and not repeated; the Task is {status}, main {old_main} -> {main_after_start}"
                ));
            }
        }
        Upgraded::Merges => {
            if status != "done" || main_after_start == old_main {
                return Err(format!(
                    "{name}: the Task merges with no owner action; it is {status} with condition {}, main {old_main} -> {main_after_start}",
                    subject["condition"]
                ));
            }
            // Done means merged: main moved to a commit that carries the
            // Task's own branch tip.
            let branch = format!("refs/heads/task/{}", &world.tasks[last][..8]);
            let tip = git(&world.dir.path().join("repo"), &["rev-parse", &branch])?;
            if !world.on_main(&tip) {
                return Err(format!(
                    "{name}: the Task is done and its branch {tip} is not on main {main_after_start}"
                ));
            }
        }
        Upgraded::Repaired => {
            if TERMINAL.contains(&status.as_str()) {
                return Err(format!(
                    "{name}: the Task was settled without its repair: {status}"
                ));
            }
        }
        Upgraded::Parked => {
            let condition = &subject["condition"];
            let integration = condition["primary"]["kind"] == "integration"
                || condition["additional"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|reason| reason["kind"] == "integration");
            let (_, offers) = world.offers(last).await?;
            if status == "done" || main_after_start != old_main {
                return Err(format!(
                    "{name}: a stopped Task merged with its stop in place: {status}, main {old_main} -> {main_after_start}"
                ));
            }
            if !integration {
                return Err(format!(
                    "{name}: the parked Task does not state its integration reason: {condition}"
                ));
            }
            if offers.is_empty() && !world.project_paused {
                return Err(format!(
                    "{name}: the parked Task offers no action: {condition}"
                ));
            }
        }
    }

    // A stopped Task is not worked on: the reconciler, the gate reads and
    // the dispatcher's sweep commit nothing while it waits.
    if expected == Upgraded::Parked && take.is_none() {
        world
            .idle_passes_write_nothing(10)
            .await
            .map_err(|error| format!("{name}: {error}"))?;
    }
    // What the fixture left on disk for the owner to clean up.
    if name == "p19_target_dirty" {
        git(&world.dir.path().join("repo"), &["checkout", "--", "."])?;
    }
    // One owner command through the real action endpoint, traced.
    let main_before_command = world.main_sha()?;
    if let Some(verb) = take {
        let was = world.upgrade_row(last).await?;
        let taken = world.apply(&Action::Take(last, verb)).await?;
        if taken != verb {
            return Err(format!(
                "{name}: `{verb}` on the Task: {taken}; it is {was}"
            ));
        }
        world.quiesce().await?;
        eprintln!(
            "COMMAND {name} {verb}: before {was} | after {} | moves [{}]",
            world.upgrade_row(last).await?,
            moves(world).await?,
        );
    }
    // The offered actions, and the runs they need, get every Task merged.
    world.drive_to_settlement().await?;
    let new_runs: Vec<(String, String)> = sqlx::query_as(
        "SELECT role, status FROM execution WHERE task_id = ? ORDER BY created_at, rowid LIMIT -1 OFFSET ?",
    )
    .bind(&world.tasks[last])
    .bind(runs as i64)
    .fetch_all(&world.live().pool)
    .await
    .map_err(|error| error.to_string())?;
    eprintln!(
        "TABLE {name} {variant} settled{}: {} | moves [{}] | new runs {new_runs:?} | owner actions {:?}",
        take.map(|verb| format!(" after {verb}")).unwrap_or_default(),
        world.upgrade_row(last).await?,
        moves(world).await?,
        world
            .trace
            .iter()
            .filter_map(|line| line.split_once(": ").map(|(taken, _)| taken.to_owned()))
            .collect::<Vec<_>>(),
    );
    for index in 0..world.tasks.len() {
        let task = world.task(index).await?;
        if take == Some("cancel") && index == last {
            if task["status"] != "cancelled" {
                return Err(format!(
                    "{name}: the cancelled Task ended {}",
                    task["status"]
                ));
            }
            continue;
        }
        // The owner's override at the merge gate declares the Task done
        // without a merge (as before the queue): nothing may merge it after.
        if take == Some("approve") && index == last {
            let live: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM integration_attempt WHERE task_ref = ? AND current = 1
                 AND state NOT IN ('completed','cancelled','superseded')",
            )
            .bind(&world.tasks[index])
            .fetch_one(&world.live().pool)
            .await
            .map_err(|error| error.to_string())?;
            if task["status"] != "done" || world.main_sha()? != main_before_command || live != 0 {
                return Err(format!(
                    "{name}: an approved Task is done with nothing merged and no live attempt; it is {}, main {main_before_command} -> {}, live attempts {live}",
                    task["status"],
                    world.main_sha()?
                ));
            }
            continue;
        }
        if task["status"] != "done" {
            return Err(format!(
                "{name}: task {index} ended {} and not done: {}; trace {:#?}",
                task["status"], task["condition"], world.trace
            ));
        }
        let branch = format!("refs/heads/task/{}", &world.tasks[index][..8]);
        let merged: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM transition_log WHERE task_id = ? AND to_state = 'done'",
        )
        .bind(&world.tasks[index])
        .fetch_one(&world.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        if merged != 1 {
            return Err(format!(
                "{name}: task {index} was reported done {merged} times"
            ));
        }
        if let Ok(tip) = git(&world.dir.path().join("repo"), &["rev-parse", &branch]) {
            if !world.on_main(&tip) {
                return Err(format!(
                    "{name}: task {index} is done and its branch {tip} is not on main"
                ));
            }
        }
    }
    if expected == Upgraded::Reported && world.main_sha()? != old_main {
        return Err(format!(
            "{name}: main moved after a landed merge was reported"
        ));
    }
    // One commit per Task on main: nothing merged twice.
    let subjects = git(
        &world.dir.path().join("repo"),
        &["log", "--format=%s", "refs/heads/main"],
    )?;
    let mut seen = std::collections::BTreeSet::new();
    for subject in subjects.lines() {
        if !seen.insert(subject.to_owned()) {
            return Err(format!(
                "{name}: main carries `{subject}` twice:\n{subjects}"
            ));
        }
    }
    eprintln!("UPGRADE {name}: settled after {:?}", clock.elapsed());
    // An attempt left parked behind a Task that was cancelled or approved is
    // cancelled by the worker's sweep at its retry time.
    if world.move_deadlines(true).await? > 0 {
        world.quiesce().await?;
    }
    // And a start after the end changes nothing either: also not for a
    // cancelled Task, whose Cancel ended the attempt parked behind it in its
    // own transaction (no import of the old build's merge intent again).
    let settled = world.upgrade_print().await?;
    world.crash_and_restart().await?;
    world.quiesce().await?;
    let again = world.upgrade_print().await?;
    if settled != again {
        return Err(format!(
            "{name}: a start after settlement changed the state:\n before {settled:#?}\n after {again:#?}"
        ));
    }
    eprintln!("UPGRADE {name}: settled: {}", settled.tasks.join("\n  "));
    // One writer per merge across the upgrade: at no instant did more than
    // one of the old merge's hooks step, the import continuation and the
    // queue worker's driver hold a Task's merge.
    use services::integration_worker::holders::{merge_holders, MergeHolder};
    let holders_db = db::SqliteDb::new(world.live.as_ref().expect("the server is up").pool.clone());
    for task in &world.tasks {
        let (most, overlap, seen) = merge_holders(&holders_db, task);
        eprintln!("UPGRADE {name}: merge holders of {task}: {seen:?}, at most {most} at once");
        if most > 1 {
            return Err(format!(
                "{name}: two writers held the merge of {task} at once: {overlap:?} (seen {seen:?})"
            ));
        }
    }
    // The two shapes where the old merge's own step was still live at the
    // upgrade: that step and the worker both acted, one after the other.
    if take.is_none()
        && matches!(
            name,
            "p05_rebase_target_no_outcome" | "p02_merge_intent_landed_no_outcome"
        )
    {
        let (_, _, seen) = merge_holders(&holders_db, &world.tasks[world.tasks.len() - 1]);
        for expected in [MergeHolder::LegacyHooksStep, MergeHolder::WorkerDriver] {
            if !seen.contains(&expected) {
                return Err(format!(
                    "{name} ({variant}): {expected:?} never held the merge; seen {seen:?}"
                ));
            }
        }
    }
    Ok(())
}
