//! Workflow model state, stack lifecycle, actions and quiescence.

use super::*;

// ---------------------------------------------------------------------------
// The real stack
// ---------------------------------------------------------------------------

/// The durable check worker of one boot. Review-entry CI is a check it
/// executes; it dies with its server.
pub(super) struct Checks {
    pub(super) stop: tokio::sync::watch::Sender<bool>,
    pub(super) job: tokio::task::JoinHandle<()>,
}

impl Checks {
    pub(super) fn stop(&self) {
        let _ = self.stop.send(true);
        self.job.abort();
    }
}

impl Drop for Checks {
    fn drop(&mut self) {
        self.stop();
    }
}

pub(super) struct Live {
    pub(super) checks: Checks,
    pub(super) pool: sqlx::SqlitePool,
    pub(super) runtime: Arc<services_agents::ForgeRuntime>,
    pub(super) state: AppState,
    pub(super) app: Router,
    pub(super) script: Arc<Script>,
}

/// `FORGE_MODEL_LOCK_STORM=<ms>`: another writer on the model's database,
/// taking the write lock for that long and leaving it for as long, for the
/// whole run. A transaction that reads before it writes without having begun
/// immediate is refused under it almost every time, where it otherwise is
/// refused only when a second writer happens to be there; everything that
/// takes the lock up front merely waits.
pub(super) struct LockStorm(pub(super) tokio::task::JoinHandle<()>);

impl Drop for LockStorm {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) fn lock_storm(dir: &Path) -> Option<LockStorm> {
    let hold = Duration::from_millis(env_number("FORGE_MODEL_LOCK_STORM")?);
    let path = dir.join("forge.db");
    Some(LockStorm(tokio::spawn(async move {
        use sqlx::Connection;
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .busy_timeout(Duration::from_secs(30));
        let mut connection = sqlx::SqliteConnection::connect_with(&options)
            .await
            .expect("the lock storm connects");
        for statement in [
            "CREATE TABLE IF NOT EXISTS model_lock_storm(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)",
            "INSERT OR IGNORE INTO model_lock_storm VALUES (1, 0)",
        ] {
            sqlx::query(statement)
                .execute(&mut connection)
                .await
                .expect("the lock storm table creates");
        }
        loop {
            // A committed change each round: a reader's snapshot goes stale too.
            if sqlx::query("BEGIN IMMEDIATE")
                .execute(&mut connection)
                .await
                .is_ok()
            {
                let _ = sqlx::query("UPDATE model_lock_storm SET n = n + 1")
                    .execute(&mut connection)
                    .await;
                tokio::time::sleep(hold).await;
                let _ = sqlx::query("COMMIT").execute(&mut connection).await;
            }
            tokio::time::sleep(hold).await;
        }
    })))
}

pub(super) struct World {
    pub(super) dir: tempfile::TempDir,
    pub(super) probe: Arc<Probe>,
    pub(super) _storm: Option<LockStorm>,
    pub(super) live: Option<Live>,
    pub(super) shared: Arc<Shared>,
    pub(super) project_id: String,
    pub(super) coder_id: String,
    pub(super) reviewer_id: String,
    pub(super) project_paused: bool,
    pub(super) agent_paused: bool,
    /// Reserved for random runs to step around open findings. The former
    /// guards for cancelled dependencies and Project pause are fixed and
    /// removed; the flag currently changes no generated actions.
    pub(super) avoid_open_findings: bool,
    pub(super) tasks: Vec<String>,
    /// Model index -> the terminal state the Task was first seen in.
    pub(super) settled: BTreeMap<usize, String>,
    pub(super) trace: Vec<String>,
}

/// Rounds a livelock is given before it is reported, and the least wall time
/// it is given as well: a loaded runner makes every round slow, and work a
/// run leaves to a spawned task is measured in time, not in rounds.
pub(super) const QUIESCENCE_BOUND: usize = 400;

pub(super) const QUIESCENCE_PATIENCE: Duration = Duration::from_secs(90);

/// How long the state must stay unchanged to count as settled. Work that a
/// finished run hands to a spawned task has no row to watch while it is
/// pending; on a loaded 4-core runner (eight tests, four workers each) that
/// task can wait tens of milliseconds for a thread.
pub(super) const QUIESCENCE_FLOOR: Duration = Duration::from_millis(250);

/// The server's run cap in every model stack: what a 4-core machine resolves
/// on its own (`config::automatic_run_cap_for_cores`).
pub(super) const MODEL_RUN_CAP: u32 = 2;

tokio::task_local! {
    /// The cap a test pins for the stacks it boots; `None` leaves the cap to
    /// the host (`max_concurrent_runs` unset).
    pub(super) static PINNED_RUN_CAP: Option<u32>;
}

/// A test's own pin first, then `FORGE_MODEL_RUN_CAP`, then `MODEL_RUN_CAP`.
pub(super) fn run_cap() -> Option<u32> {
    PINNED_RUN_CAP.try_with(|cap| *cap).unwrap_or_else(|_| {
        Some(env_number("FORGE_MODEL_RUN_CAP").map_or(MODEL_RUN_CAP, |cap| cap as u32))
    })
}

pub(super) const BUSY_RETRIES: usize = 8;

pub(super) const SETTLE_ROUNDS: usize = 20;

pub(super) const INITIAL: [&str; 2] = ["backlog", "todo"];

pub(super) const TERMINAL: [&str; 2] = ["done", "cancelled"];

impl World {
    pub(super) async fn boot(dir: &Path, shared: &Arc<Shared>, probe: &Arc<Probe>) -> Live {
        *probe.database.lock().unwrap() = Some(dir.join("forge.db"));
        bounded(
            probe,
            "booting the server",
            Self::boot_unbounded(dir, shared, probe),
        )
        .await
    }

    pub(super) async fn boot_unbounded(
        dir: &Path,
        shared: &Arc<Shared>,
        probe: &Arc<Probe>,
    ) -> Live {
        let url = format!("sqlite:{}?mode=rwc", dir.join("forge.db").display());
        let pool = db::create_sqlite_pool(&url).await.expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations run");
        install_replan_audit(&pool).await;
        // A fresh `SqliteDb` per boot: its in-memory schedule generation and
        // step-executor reference die with the process, as in a real crash.
        let db = Arc::new(db::SqliteDb::new(pool.clone()));
        let script = Arc::new(Script {
            alive: AtomicBool::new(true),
            waiting: Mutex::default(),
            shared: Arc::clone(shared),
            db: Arc::clone(&db),
            service: OnceLock::new(),
        });
        let mut registry = executors::AdapterRegistry::new();
        registry.register(Box::new(ScriptedAdapter(Arc::clone(&script))));
        *probe.script.lock().unwrap() = Some(Arc::clone(&script));
        let registry = Arc::new(registry);
        services_agents::ensure_default_agents(db.as_ref(), &registry)
            .await
            .expect("default agents upsert");
        let event_bus = Arc::new(events::EventBus::new(1024));
        let workspaces = dir.join("workspaces");
        let mut config = config::ForgeConfig::with_data_dir(dir.join("data"));
        config.workspace.root = workspaces.clone();
        config.server.max_concurrent_runs = run_cap();
        let runtime = Arc::new(
            services_agents::ForgeRuntimeBuilder::from_config(
                Arc::clone(&db),
                Arc::clone(&event_bus),
                config,
            )
            .with_adapter_registry(Arc::clone(&registry))
            .with_cleanup_scheduler(Arc::new(services::WorkspaceCleanupScheduler::new(
                Arc::clone(&db),
                Arc::clone(&event_bus),
                workspaces.clone(),
            )))
            .with_merge_service(Arc::new(services::MergeService::new_for_test(
                Arc::clone(&db),
                workspaces,
            )))
            .with_workflows_dir(api::state::test_workflows_dir())
            .with_jwt_secret(api::state::test_jwt_secret())
            .with_bcrypt_cost(api::state::test_bcrypt_cost())
            .build(),
        );
        let _ = script.service.set(Arc::downgrade(&runtime.task_service));
        let state = AppState::from_runtime_arc(Arc::clone(&runtime), true);
        let app = build_router(state.clone(), dir.join("web"));
        let (stop, signal) = tokio::sync::watch::channel(false);
        let checks = Checks {
            stop,
            job: state
                .task_service
                .check_worker()
                .expect("the runtime composes the check worker")
                .start(&state.operator_status_service.periodic_workers(), signal),
        };
        Live {
            checks,
            pool,
            runtime,
            state,
            app,
            script,
        }
    }

    pub(super) async fn new() -> Self {
        let dir = tempfile::tempdir().expect("model directory creates");
        std::fs::create_dir_all(dir.path().join("web")).unwrap();
        std::fs::write(dir.path().join("web/index.html"), "<html></html>").unwrap();
        std::fs::create_dir_all(dir.path().join("workspaces")).unwrap();
        let repo = common::setup_git_repo(dir.path());
        std::fs::write(repo.join("shared.txt"), "base\n").unwrap();
        git(&repo, &["add", "-A"]).unwrap();
        git(&repo, &["commit", "-m", "shared file"]).unwrap();

        let shared = Arc::new(Shared::default());
        let probe = Probe::new("model");
        let live = Self::boot(dir.path(), &shared, &probe).await;
        sqlx::query(
            "INSERT INTO user(id,email,password_hash,display_name,is_admin,created_at,updated_at)
             VALUES ('test-user-id','test@example.com','$2b$04$placeholder',NULL,0,?,?)",
        )
        .bind(db::now_rfc3339())
        .bind(db::now_rfc3339())
        .execute(&live.pool)
        .await
        .expect("seed test user");
        let (project_id, repo_id) =
            common::create_project_and_repo(&live.app, "Model", &repo).await;
        let (coder_id, reviewer_id) =
            register_agents(&live.app, &live.state, &dir.path().join("workspaces")).await;
        common::configure_execution_test_setup(
            &live.state.db,
            &project_id,
            &repo_id,
            &coder_id,
            &reviewer_id,
        )
        .await;
        // No planner: an unassigned planning gate is skipped, so the model
        // exercises the coder, review and merge loop. A planner needs a
        // scripted plan artifact, which this model does not write yet.
        let settings: String = sqlx::query_scalar("SELECT settings FROM project WHERE id = ?")
            .bind(&project_id)
            .fetch_one(&live.pool)
            .await
            .expect("project settings load");
        let mut settings: Value = serde_json::from_str(&settings).expect("settings parse");
        settings["default_role_assignments"]
            .as_array_mut()
            .expect("default roles are configured")
            .retain(|role| role["role_name"] != "planner");
        sqlx::query("UPDATE project SET settings = ? WHERE id = ?")
            .bind(settings.to_string())
            .bind(&project_id)
            .execute(&live.pool)
            .await
            .expect("project settings update");
        Self {
            _storm: lock_storm(dir.path()),
            dir,
            probe,
            live: Some(live),
            shared,
            project_id,
            coder_id,
            reviewer_id,
            project_paused: false,
            agent_paused: false,
            avoid_open_findings: false,
            tasks: Vec::new(),
            settled: BTreeMap::new(),
            trace: Vec::new(),
        }
    }

    /// On a violation: every transition of every Task that is not settled.
    pub(super) async fn record_history(&mut self) {
        let Some(live) = self.live.as_ref() else {
            return;
        };
        let mut lines = Vec::new();
        for (index, id) in self.tasks.iter().enumerate() {
            let rows: Vec<(String, String, String, String)> = sqlx::query_as(
                "SELECT from_state, to_state, triggered_by, trigger_reason FROM transition_log
                 WHERE task_id = ? ORDER BY created_at, rowid",
            )
            .bind(id)
            .fetch_all(&live.pool)
            .await
            .unwrap_or_default();
            if rows
                .last()
                .is_some_and(|row| TERMINAL.contains(&row.1.as_str()))
            {
                continue;
            }
            for (from, to, by, reason) in rows {
                let reason: String = reason.chars().take(160).collect();
                lines.push(format!(
                    "  history {index}: {from} -> {to} by {by}: {reason}"
                ));
            }
        }
        self.trace.extend(lines);
    }

    pub(super) fn live(&self) -> &Live {
        self.live.as_ref().expect("the server is up")
    }

    /// Kill the server without a graceful shutdown and start a new one on
    /// the same database, the way `RuntimeSupervisor::start` does: crash
    /// recovery, then the dispatcher's startup reconciliation.
    pub(super) async fn crash_and_restart(&mut self) -> Result<(), String> {
        let mut old = self.live.take().expect("the server is up");
        old.script.alive.store(false, Ordering::SeqCst);
        old.script.waiting.lock().unwrap().clear();
        old.runtime.task_dispatcher.stop();
        old.checks.stop();
        // Whatever the dead process still had in flight can no longer write:
        // its check worker is gone before the pool closes, and the pool is
        // closed down to its last connection. `Pool::close` alone can leave
        // one open, and a connection of the dead process that still held the
        // write lock would make the new server wait out its busy timeout.
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut old.checks.job).await;
        if tokio::time::timeout(Duration::from_secs(10), db::close_sqlite_pool(&old.pool))
            .await
            .is_err()
        {
            eprintln!(
                "model: the crashed server still holds {} database connection(s)",
                old.pool.size()
            );
        }
        drop(old);

        let live = Self::boot(self.dir.path(), &self.shared, &self.probe).await;
        bounded(&self.probe, "starting after a crash", Self::start(&live)).await?;
        self.live = Some(live);
        Ok(())
    }

    /// What `RuntimeSupervisor::start` does before its workers run, call for
    /// call and in its order: crash recovery, the integration import, the
    /// dispatcher's startup reconciliation.
    pub(super) async fn start(
        live: &Live,
    ) -> Result<services::integration_steps::import::ImportReport, String> {
        live.runtime
            .crash_recovery
            .run()
            .await
            .map_err(|error| format!("crash recovery failed: {error}"))?;
        let report = services::integration_steps::import::import_at_start(&live.state.db)
            .await
            .map_err(|error| format!("integration import failed: {error}"))?;
        live.runtime
            .task_dispatcher
            .startup_reconcile()
            .await
            .map_err(|error| format!("startup reconciliation failed: {error}"))?;
        Ok(report)
    }

    pub(super) async fn request(
        &self,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> Result<(StatusCode, Value), String> {
        let builder = Request::builder().method(method.clone()).uri(uri).header(
            header::AUTHORIZATION,
            format!("Bearer {}", common::test_jwt()),
        );
        let request = match body {
            Some(body) => builder
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string())),
            None => builder.body(Body::empty()),
        }
        .expect("request builds");
        let (status, bytes) = bounded(&self.probe, &format!("{method} {uri}"), async {
            let response = self
                .live()
                .app
                .clone()
                .oneshot(request)
                .await
                .expect("router responds");
            let status = response.status();
            let bytes = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body reads");
            (status, bytes)
        })
        .await;
        let value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
        if status.is_server_error() {
            return Err(format!("{method} {uri} answered {status}: {value}"));
        }
        Ok((status, value))
    }

    pub(super) async fn task(&self, index: usize) -> Result<Value, String> {
        let (status, task) = self
            .request(
                Method::GET,
                &format!("/api/v1/tasks/{}", self.tasks[index]),
                None,
            )
            .await?;
        if status != StatusCode::OK {
            return Err(format!("task {index} is unreadable: {status} {task}"));
        }
        Ok(task)
    }

    pub(super) async fn offers(&self, index: usize) -> Result<(i64, Vec<Value>), String> {
        let (status, offers) = self
            .request(
                Method::GET,
                &format!("/api/v1/tasks/{}/actions", self.tasks[index]),
                None,
            )
            .await?;
        if status != StatusCode::OK {
            return Err(format!(
                "offers of task {index} are unreadable: {status} {offers}"
            ));
        }
        Ok((
            offers["version"].as_i64().unwrap_or_default(),
            offers["available_actions"]
                .as_array()
                .cloned()
                .unwrap_or_default(),
        ))
    }

    /// Invariant (b): an advertised offer is accepted when applied.
    pub(super) async fn apply_offer(
        &self,
        index: usize,
        version: i64,
        offer: &Value,
    ) -> Result<String, String> {
        let mut action = offer["action"].clone();
        let verb = action["verb"].as_str().unwrap_or_default().to_owned();
        for parameter in offer["parameters"].as_array().into_iter().flatten() {
            let name = parameter["name"].as_str().unwrap_or_default();
            if parameter["required"] == true && action.get(name).is_none_or(Value::is_null) {
                action[name] = match parameter["boolean_values"].as_array() {
                    Some(values) => values.first().cloned().unwrap_or(Value::Bool(false)),
                    None => json!("the model says so"),
                };
            }
        }
        if verb == "send_back" && action["guidance"].as_str().is_none_or(str::is_empty) {
            action["guidance"] = json!("the model sends it back");
        }
        let (status, body) = self
            .request(
                Method::POST,
                &format!("/api/v1/tasks/{}/actions", self.tasks[index]),
                Some(json!({ "version": version, "action": action })),
            )
            .await?;
        if !status.is_success() {
            return Err(format!(
                "(b) offered action `{verb}` on task {index} was refused: {status} {body}; offer {offer}"
            ));
        }
        Ok(verb)
    }

    pub(super) async fn create(&mut self, parent: Option<usize>) -> Result<String, String> {
        let index = self.tasks.len();
        let mut body = json!({
            "title": format!("model task {index}"),
            "description": "make the model deliverable",
            "role_assignments": [{
                "role_name": "reviewer", "assignee_type": "agent", "assignee_id": self.reviewer_id
            }],
            "review_config": {
                "ci_steps": [format!("test -f {}", deliverable(index))],
                "review_prompt": "Review the model deliverable."
            }
        });
        if let Some(parent) = parent {
            body["parent_task_id"] = json!(self.tasks[parent]);
            // A subtask's workflow has no reviewer role of its own.
            body.as_object_mut().unwrap().remove("role_assignments");
        }
        let (status, task) = self
            .request(
                Method::POST,
                &format!("/api/v1/projects/{}/tasks", self.project_id),
                Some(body),
            )
            .await?;
        if !status.is_success() {
            // Refusing a child of a Task that already moved on is legitimate.
            // The code, not the message: the message names the parent by
            // its random id, and the trace has to be the same on every run.
            return Ok(format!("refused {status} {}", task["code"]));
        }
        let id = task["id"]
            .as_str()
            .expect("created Task has an ID")
            .to_owned();
        let pool = &self.live().pool;
        let stored: Option<String> =
            sqlx::query_scalar("SELECT task_state_config FROM task WHERE id = ?")
                .bind(&id)
                .fetch_one(pool)
                .await
                .map_err(|error| error.to_string())?;
        let mut state_config = stored
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({}));
        if !state_config["review"].is_object() {
            state_config["review"] = json!({ "review_prompt": "Review the model deliverable." });
        }
        sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
            .bind(state_config.to_string())
            .bind(&id)
            .execute(pool)
            .await
            .map_err(|error| error.to_string())?;
        self.shared.index.lock().unwrap().insert(id.clone(), index);
        if let Some(parent) = parent {
            self.shared.parents.lock().unwrap().insert(index, parent);
        }
        self.tasks.push(id);
        Ok("created".to_owned())
    }

    /// The model owns the clock. Backoff deadlines are wall-clock times in
    /// Task metadata; left alone, whether one has passed by the next step
    /// depends on how fast the machine is. So a deadline more than a second
    /// away is frozen (moved out of reach) as soon as it appears, and only
    /// `AdvanceClock` lets the frozen ones pass. Writes go through the Task
    /// repository, so the stored condition follows.
    pub(super) async fn move_deadlines(&self, advance: bool) -> Result<usize, String> {
        const FROZEN: &str = "2099-01-01T00:00:00+00:00";
        let soon = (chrono::Utc::now() + chrono::Duration::seconds(1)).to_rfc3339();
        let mut moved = 0;
        if advance {
            // The retry time of a merge the queue worker parked.
            for id in &self.tasks {
                let waits: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM integration_attempt WHERE task_ref = ? AND current = 1
                     AND state = 'parked' AND julianday(available_at) > julianday('now'))",
                )
                .bind(id)
                .fetch_one(&self.live().pool)
                .await
                .map_err(|error| error.to_string())?;
                if waits
                    && bounded(
                        &self.probe,
                        "expediting a parked attempt",
                        self.live().state.db.expedite_parked_integration_attempt(
                            id,
                            &db::now_rfc3339(),
                            false,
                        ),
                    )
                    .await
                    .map_err(|error| error.to_string())?
                {
                    moved += 1;
                }
            }
        }
        for id in &self.tasks {
            let metadata: Option<String> =
                sqlx::query_scalar("SELECT metadata_json FROM task WHERE id = ?")
                    .bind(id)
                    .fetch_one(&self.live().pool)
                    .await
                    .map_err(|error| error.to_string())?;
            let Some(mut deferral) = metadata
                .as_deref()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                .map(|metadata| metadata["deferred_dispatch"].clone())
                .filter(Value::is_object)
            else {
                continue;
            };
            let deadline = deferral["not_before"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            let frozen = deadline.as_str() >= "2090";
            deferral["not_before"] = match (advance, frozen) {
                (true, true) => json!("1970-01-01T00:00:00+00:00"),
                (false, false) if deadline > soon => json!(FROZEN),
                _ => continue,
            };
            // A Task with a step still queued (always, right after a crash)
            // refuses a direct write as busy: run its steps and try again.
            // The deadline is re-read on the next pass, so a write the steps
            // made obsolete is harmless.
            let mut attempt = 0;
            loop {
                let now = db::now_rfc3339();
                let written = bounded(
                    &self.probe,
                    "moving a deadline",
                    db::TaskRepo::mutate_metadata(
                        &*self.live().state.db,
                        id,
                        None,
                        vec![db::TaskMetadataMutation::Set {
                            key: "deferred_dispatch".to_owned(),
                            value: deferral.clone(),
                        }],
                        &now,
                    ),
                )
                .await;
                match written {
                    Ok(_) => break,
                    Err(db::DbError::TaskBusy { .. }) if attempt < BUSY_RETRIES => {
                        attempt += 1;
                        self.drain(id)
                            .await
                            .map_err(|error| format!("draining a busy Task failed: {error}"))?;
                    }
                    Err(error) => return Err(format!("moving a deadline failed: {error}")),
                }
            }
            moved += 1;
        }
        Ok(moved)
    }

    /// The first Task at or after `from` (wrapping) for which `wanted` holds.
    pub(super) fn first_from(&self, from: usize, wanted: impl Fn(usize) -> bool) -> Option<usize> {
        let count = self.tasks.len();
        (0..count)
            .map(|offset| (from + offset) % count)
            .find(|index| wanted(*index))
    }

    pub(super) fn waiting(&self, index: usize) -> bool {
        self.live()
            .script
            .waiting
            .lock()
            .unwrap()
            .contains_key(&self.tasks[index])
    }

    pub(super) async fn apply(&mut self, action: &Action) -> Result<String, String> {
        let count = self.tasks.len();
        let pick = |index: usize| (count > 0).then(|| index % count);
        match action {
            Action::CreateLeadRoot => {
                let root = self.tasks.len();
                self.create(None).await?;
                self.create(Some(root)).await?;
                self.create(Some(root)).await?;
                self.assign_lead(root).await
            }
            Action::Create => self.create(None).await,
            Action::CreateChild(parent) => match pick(*parent) {
                Some(parent) => self.create(Some(parent)).await,
                None => Ok("skipped".to_owned()),
            },
            Action::Depend(task, on) => {
                let (Some(task), Some(on)) = (pick(*task), pick(*on)) else {
                    return Ok("skipped".to_owned());
                };
                if task == on {
                    return Ok("skipped".to_owned());
                }
                let (status, _) = self
                    .request(
                        Method::POST,
                        &format!("/api/v1/tasks/{}/dependencies", self.tasks[task]),
                        Some(json!({ "depends_on_id": self.tasks[on] })),
                    )
                    .await?;
                Ok(format!("{status}"))
            }
            Action::Claim(task) => {
                let Some(task) = pick(*task) else {
                    return Ok("skipped".to_owned());
                };
                let (status, body) = self
                    .request(
                        Method::POST,
                        &format!("/api/v1/tasks/{}/claim", self.tasks[task]),
                        Some(json!({ "agent_id": self.coder_id, "overrides": null })),
                    )
                    .await?;
                Ok(if status.is_success() {
                    "claimed".to_owned()
                } else {
                    format!("refused {status} {}", body["error"]["code"])
                })
            }
            Action::Finish(task, outcome) => {
                // Finish the named Task's run, or the next one that has one.
                let Some(task) =
                    pick(*task).and_then(|task| self.first_from(task, |t| self.waiting(t)))
                else {
                    return Ok("skipped".to_owned());
                };
                let gate = self
                    .live()
                    .script
                    .waiting
                    .lock()
                    .unwrap()
                    .remove(&self.tasks[task]);
                match gate {
                    Some((_, gate)) => {
                        let _ = gate.send(*outcome);
                        Ok(format!("finished {task}"))
                    }
                    None => Ok("skipped".to_owned()),
                }
            }
            Action::Verdict(task, pass) => {
                let Some(task) = pick(*task) else {
                    return Ok("skipped".to_owned());
                };
                self.shared
                    .verdicts
                    .lock()
                    .unwrap()
                    .insert(self.tasks[task].clone(), *pass);
                Ok("scripted".to_owned())
            }
            Action::Offer(task, n) => {
                let Some(task) = pick(*task) else {
                    return Ok("skipped".to_owned());
                };
                let (version, offers) = self.offers(task).await?;
                if offers.is_empty() {
                    return Ok("no offers".to_owned());
                }
                self.apply_offer(task, version, &offers[n % offers.len()])
                    .await
            }
            Action::Cancel(task) | Action::Take(task, _) => {
                let verb = match action {
                    Action::Take(_, verb) => verb,
                    _ => "cancel",
                };
                let Some(task) = pick(*task) else {
                    return Ok("skipped".to_owned());
                };
                let (version, offers) = self.offers(task).await?;
                match offers.iter().find(|offer| offer["action"]["verb"] == verb) {
                    Some(offer) => self.apply_offer(task, version, offer).await,
                    None => Ok(format!("{verb} not offered")),
                }
            }
            Action::PauseProject | Action::ResumeProject => {
                let pause = *action == Action::PauseProject;
                let verb = if pause { "pause" } else { "resume" };
                let uri = format!("/api/v1/projects/{}/{verb}", self.project_id);
                let (status, body) = self.request(Method::POST, &uri, None).await?;
                if !status.is_success() {
                    return Err(format!("(b) project {verb} was refused: {status} {body}"));
                }
                self.project_paused = pause;
                Ok(verb.to_owned())
            }
            Action::EditProject => {
                let uri = format!("/api/v1/projects/{}", self.project_id);
                let (_, project) = self.request(Method::GET, &uri, None).await?;
                let version = project["version"].as_i64().unwrap_or_default();
                let body = json!({ "version": version, "name": project["name"] });
                let (status, saved) = self.request(Method::PATCH, &uri, Some(body)).await?;
                if !status.is_success() {
                    return Err(format!(
                        "(b) saving the Project was refused: {status} {saved}"
                    ));
                }
                if saved["version"].as_i64().unwrap_or_default() <= version {
                    return Err(format!(
                        "the model expects a Project edit to move its version: {version} -> {}",
                        saved["version"]
                    ));
                }
                Ok("saved".to_owned())
            }
            Action::PauseAgent | Action::ResumeAgent => {
                let pause = *action == Action::PauseAgent;
                let verb = if pause { "pause" } else { "resume" };
                let uri = format!("/api/v1/agents/{}/{verb}", self.coder_id);
                let (status, body) = self.request(Method::POST, &uri, None).await?;
                if !status.is_success() {
                    return Err(format!("(b) agent {verb} was refused: {status} {body}"));
                }
                self.agent_paused = pause;
                Ok(verb.to_owned())
            }
            Action::AssignLead(task) => match pick(*task) {
                Some(task) => self.assign_lead(task).await,
                None => Ok("skipped".to_owned()),
            },
            Action::RemoveLead(task) => match pick(*task) {
                Some(task) => {
                    let (status, body) = self
                        .request(
                            Method::DELETE,
                            &format!("/api/v1/tasks/{}/roles/planner", self.tasks[task]),
                            None,
                        )
                        .await?;
                    Ok(if status.is_success() {
                        "lead removed".to_owned()
                    } else {
                        format!("refused {status} {}", body["code"])
                    })
                }
                None => Ok("skipped".to_owned()),
            },
            Action::AdvanceClock => Ok(format!(
                "{} deadline(s) passed",
                self.move_deadlines(true).await?
            )),
            Action::Crash => {
                self.crash_and_restart().await?;
                Ok("restarted".to_owned())
            }
        }
    }

    pub(super) async fn executions(
        &self,
        index: usize,
    ) -> Result<Vec<(String, String, String)>, String> {
        let (_, page) = self
            .request(
                Method::GET,
                &format!("/api/v1/tasks/{}/executions?limit=100", self.tasks[index]),
                None,
            )
            .await?;
        Ok(page["items"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|execution| {
                let text = |key: &str| execution[key].as_str().unwrap_or_default().to_owned();
                (text("id"), text("role"), text("status"))
            })
            .collect())
    }

    /// Everything that can change while the system still has work to do, and
    /// whether an execution is running that no model action can finish.
    pub(super) async fn fingerprint(&self) -> Result<(String, Vec<String>), String> {
        let mut print = String::new();
        let mut unowned = Vec::new();
        for (index, id) in self.tasks.iter().enumerate() {
            let (status, version): (String, i64) =
                sqlx::query_as("SELECT status, version FROM task WHERE id = ?")
                    .bind(id)
                    .fetch_one(&self.live().pool)
                    .await
                    .map_err(|error| error.to_string())?;
            let pending = self
                .live()
                .state
                .db
                .pending_steps(id)
                .await
                .map_err(|error| error.to_string())?;
            // A hooks step suspended on review-entry CI is not a pending
            // step, but the check worker still owes this Task a result.
            let awaiting: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM task_step WHERE task_id = ? AND status = 'suspended'",
            )
            .bind(id)
            .fetch_one(&self.live().pool)
            .await
            .map_err(|error| error.to_string())?;
            if awaiting > 0 {
                // A check queued behind a full machine is owed nothing until
                // a slot frees: it waits for a run the model can finish, like
                // a Task parked on capacity. With no such run, the slot it
                // waits for is held by nothing the model can end.
                let slot = self
                    .live()
                    .state
                    .db
                    .awaited_check_waits_for_slot(id)
                    .await
                    .map_err(|error| error.to_string())?;
                let held = (0..self.tasks.len()).any(|other| other != index && self.waiting(other));
                if !slot {
                    unowned.push(format!("task {index} waits for a check result"));
                } else if !held {
                    unowned.push(format!(
                        "task {index} waits for a check slot that no execution holds"
                    ));
                }
            }
            print.push_str(&format!("{index}:{status}@{version}+{pending}~{awaiting}["));
            for (execution, role, status) in self.executions(index).await? {
                print.push_str(&format!("{role}={status},"));
                let gated = self
                    .live()
                    .script
                    .waiting
                    .lock()
                    .unwrap()
                    .get(id)
                    .is_some_and(|(waiting, _)| *waiting == execution);
                if matches!(status.as_str(), "running" | "pending" | "queued") && !gated {
                    unowned.push(format!("task {index} {role} execution is {status}"));
                }
            }
            print.push_str("] ");
        }
        Ok((print, unowned))
    }

    /// The step worker's drain of one Task, under the model's hard deadline.
    pub(super) async fn drain(&self, id: &str) -> Result<(), services::ServiceError> {
        bounded(
            &self.probe,
            &format!("draining the steps of Task {id}"),
            self.live().state.task_service.drain(id),
        )
        .await
        .map(|_| ())
    }

    /// One dispatcher pass, under the model's hard deadline.
    pub(super) async fn dispatcher_pass(&self) -> Result<(), String> {
        bounded(
            &self.probe,
            "a dispatcher pass",
            self.live().runtime.task_dispatcher.check_once(),
        )
        .await
        .map(|_| ())
        .map_err(|error| format!("dispatcher pass failed: {error}"))
    }

    /// Invariant (d): run the step worker and the dispatcher until nothing
    /// changes. Not settling within the bound is a livelock.
    pub(super) async fn quiesce(&mut self) -> Result<(), String> {
        let mut last = String::new();
        let mut stable = 0;
        let mut unowned = Vec::new();
        let mut history = Vec::new();
        let mut changed = std::time::Instant::now();
        let started = changed;
        for round in 0.. {
            if round >= QUIESCENCE_BOUND && started.elapsed() >= QUIESCENCE_PATIENCE {
                break;
            }
            self.adopt_unindexed_children().await?;
            self.sync_lead_tasks();
            self.move_deadlines(false).await?;
            for (index, id) in self.tasks.iter().enumerate() {
                self.drain(id).await.map_err(|error| {
                    format!("draining the steps of task {index} failed: {error}")
                })?;
            }
            self.dispatcher_pass().await?;
            tokio::time::sleep(Duration::from_millis(if round < 20 { 5 } else { 25 })).await;
            let frozen = self.move_deadlines(false).await?;
            let (mut print, running) =
                bounded(&self.probe, "reading the fingerprint", self.fingerprint()).await?;
            if frozen > 0 {
                print.push_str(&format!("froze {frozen} at round {round}"));
            }
            unowned = running;
            if print == last {
                stable += 1;
                // Work a finished run leaves to its spawned task (recording
                // the failure, scheduling the retry) has no row to watch
                // while it is pending, so "unchanged" also has to last: three
                // rounds pass in microseconds of scheduler time on a loaded
                // runner.
                if stable >= 3 && unowned.is_empty() && changed.elapsed() >= QUIESCENCE_FLOOR {
                    return Ok(());
                }
            } else {
                stable = 0;
                changed = std::time::Instant::now();
                history.push(print.clone());
                last = print;
            }
        }
        if stable >= 3 {
            return Err(format!(
                "(a) an execution stays in flight with no live owner and nothing reclaims it: {unowned:?}; state {last}"
            ));
        }
        let tail = history.len().saturating_sub(6);
        Err(format!(
            "(d) livelock: no quiescence after {QUIESCENCE_BOUND} rounds; last states {:#?}",
            &history[tail..]
        ))
    }

    /// A child the lead created is indexed by the scripted adapter after
    /// its `lead.subtask create` returns. A run cancelled in that window
    /// (the root's Cancel stops the planner) drops the future after the
    /// create committed, and the model would never drive the child's Task
    /// steps (the root's cancel cascade enqueues one), then report the
    /// child it stranded itself under (o). The product's step worker runs
    /// every pending step; the model indexes every child of a Task it
    /// knows from the database.
    pub(super) async fn adopt_unindexed_children(&mut self) -> Result<(), String> {
        let known: Vec<String> = self.shared.index.lock().unwrap().keys().cloned().collect();
        let known = serde_json::to_string(&known).map_err(|error| error.to_string())?;
        let found: Vec<(String, String)> = sqlx::query_as(
            "SELECT id, parent_task_id FROM task
             WHERE parent_task_id IN (SELECT value FROM json_each(?1))
               AND id NOT IN (SELECT value FROM json_each(?1))
             ORDER BY created_at, id",
        )
        .bind(&known)
        .fetch_all(&self.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        let mut index = self.shared.index.lock().unwrap();
        let mut parents = self.shared.parents.lock().unwrap();
        for (id, parent) in found {
            let next = index.len();
            let root = index[&parent];
            let child = *index.entry(id).or_insert(next);
            parents.insert(child, root);
        }
        Ok(())
    }

    pub(super) fn sync_lead_tasks(&mut self) {
        let mut indexed: Vec<_> = self
            .shared
            .index
            .lock()
            .unwrap()
            .iter()
            .map(|(id, index)| (*index, id.clone()))
            .collect();
        indexed.sort_by_key(|(index, _)| *index);
        self.tasks = indexed.into_iter().map(|(_, id)| id).collect();
    }
}

pub(super) async fn register_agents(
    app: &Router,
    state: &AppState,
    workspaces: &Path,
) -> (String, String) {
    let registration =
        common::register_embedded_daemon(state, workspaces, "model-host", "codex").await;
    let mut ids = Vec::new();
    for name in ["model-coder", "model-reviewer"] {
        let agent: AgentResponse = common::json_request_with_bearer(
            app,
            Method::POST,
            "/api/v1/agents",
            &common::admin_jwt(),
            json!({ "name": name, "executor_type": "codex", "daemon_id": registration.daemon_id }),
            StatusCode::OK,
        )
        .await;
        ids.push(agent.id);
    }
    (ids.remove(0), ids.remove(0))
}
