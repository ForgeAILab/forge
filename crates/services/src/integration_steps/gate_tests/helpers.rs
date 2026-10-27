//! Shared fixtures and assertions for integration gates.
use super::*;

impl World {
    /// A Task in `merging` that nothing has admitted (no attempt, no entry
    /// step), with its implementation run, then admitted by the reconciler's
    /// step: the attempt is bound and `waiting` is stated.
    pub(super) async fn reconciled(&self, task: &str, file: &str) -> IntegrationAttempt {
        self.legacy_task(task, file, "merging").await;
        self.seed_execution(task, "executor", None).await;
        let row = self.task(task).await;
        assert!(
            self.service
                .reconcile_integration_admission(&row)
                .await
                .unwrap(),
            "the reconciler admits a merging Task nothing owns"
        );
        let attempt = self
            .db
            .current_integration_attempt(task)
            .await
            .unwrap()
            .expect("admitted");
        assert_eq!(attempt.state, S::Queued);
        assert!(attempt.workspace_id.is_some(), "bound to the placement");
        attempt
    }
    /// A fixture wrote the Task's legacy columns with plain SQL: derive its
    /// stored condition again, as every real write does.
    pub(super) async fn touch(&self, task: &str) {
        db::TaskRepo::mutate_metadata(&*self.db, task, None, Vec::new(), &db::now_rfc3339())
            .await
            .unwrap();
    }
    pub(super) async fn move_target(&self, file: &str) {
        std::fs::write(self.repo().join(file), "moved\n").unwrap();
        git::commit_all(&self.repo(), "target moved").await.unwrap();
    }
    pub(super) async fn main_tip(&self) -> String {
        git_out(&self.repo(), &["rev-parse", "refs/heads/main"])
            .await
            .unwrap()
    }
    pub(super) async fn on_main(&self, file: &str) -> bool {
        let tip = self.main_tip().await;
        git_out(&self.repo(), &["cat-file", "-e", &format!("{tip}:{file}")])
            .await
            .is_some()
    }
    pub(super) async fn act(&self, task: &str, action: TaskAction) -> db::Task {
        let version = self.task(task).await.version;
        self.service
            .perform_task_action(task, action.clone(), version)
            .await
            .unwrap_or_else(|error| panic!("{action:?} on {task}: {error}"))
            .task
    }
    pub(super) async fn offers(&self, task: &str) -> Vec<(String, String)> {
        self.service
            .task_action_offers(
                task,
                &api_types::Actor::user(api_types::UserActionSource::Api),
            )
            .await
            .unwrap()
            .available_actions
            .into_iter()
            .map(|offer| (offer.action.verb().to_owned(), offer.reason))
            .collect()
    }
    /// Rebase receipts on every attempt of the Task: Git rebases performed.
    pub(super) async fn rebases(&self, task: &str) -> usize {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT effect_receipts_json FROM integration_attempt WHERE task_ref=?",
        )
        .bind(task)
        .fetch_all(self.db.pool())
        .await
        .unwrap();
        rows.iter()
            .flat_map(|raw| serde_json::from_str::<Vec<Value>>(raw).unwrap_or_default())
            .filter(|receipt| {
                receipt["request"]["kind"] == "rebase" && receipt["operation_state"] == "succeeded"
            })
            .count()
    }
    pub(super) async fn merges(&self, task: &str) -> usize {
        self.comments(task)
            .await
            .iter()
            .filter(|comment| comment.starts_with("Changes merged to main"))
            .count()
    }
}

/// `(admission key, state, failure message, available_at, effect_seq, journal)`.
pub(super) type AttemptRow = (String, String, Option<String>, Option<String>, i64, String);

impl World {
    /// Everything a failed wait needs to be read: the Task's condition, its
    /// attempts and its steps.
    pub(super) async fn dump(&self, task: &str) -> String {
        let row = self.task(task).await;
        let attempts: Vec<AttemptRow> = sqlx::query_as(
            "SELECT admission_key,state,failure_message,available_at,effect_seq,operation_receipts_json FROM integration_attempt WHERE task_ref=? ORDER BY created_at,id",
        )
        .bind(task)
        .fetch_all(self.db.pool())
        .await
        .unwrap();
        let steps: Vec<String> = self
            .db
            .task_steps(task)
            .await
            .unwrap()
            .iter()
            .map(|step| {
                format!(
                    "#{} {} {} {} attempts={} error={:?}",
                    step.seq,
                    step.kind,
                    step.causation_key,
                    step.status,
                    step.attempts,
                    step.last_error
                )
            })
            .collect();
        format!(
            "status={} version={} blocked={:?} annotation={:?} condition={}\nattempts={attempts:#?}\nsteps={steps:#?}",
            row.status,
            row.version,
            row.blocked_json,
            row.error_annotation,
            json!(row.condition)
        )
    }
    /// `eventually` with a short bound and the Task's rows in the failure.
    pub(super) async fn until<F, Fut>(&self, task: &str, what: &str, mut check: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            if check().await {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out waiting for: {what}\n{}", self.dump(task).await);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

pub(super) async fn claim(production: &Production) -> HeadDriver {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(driver) = production.worker.sweep_once().await.unwrap().pop() {
            return driver;
        }
        assert!(tokio::time::Instant::now() < deadline, "no queue to claim");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Step the head until `attempt` is in `stop` (that state's own pass has not
/// run) or it left the slot. Returns the attempt's state.
pub(super) async fn step_to(world: &World, driver: &mut HeadDriver, attempt: &str, stop: S) -> S {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let state = world.attempt(attempt).await.state;
        if state == stop {
            return state;
        }
        match driver.pass().await.unwrap() {
            Pass::Released | Pass::Lost => return world.attempt(attempt).await.state,
            Pass::Wait(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            Pass::Progress => {}
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "head never reached {stop}"
        );
    }
}

pub(super) async fn merged(world: &World, task: &str, production: &Production) {
    let running = production.run();
    world
        .until(task, &format!("{task} is done"), || async {
            world.task(task).await.status == "done"
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}

pub(super) fn gate_code(attempt: &IntegrationAttempt) -> String {
    attempt
        .failure_message
        .clone()
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default()
        .to_owned()
}

/// The dispatcher over this World. Its repository sync pauses a Project
/// that has no primary repository, so the World's Project gets its one.
pub(super) async fn dispatcher(world: &World) -> crate::TaskDispatcher {
    sqlx::query("UPDATE project SET primary_repo_id='r' WHERE id='p' AND primary_repo_id IS NULL")
        .execute(world.db.pool())
        .await
        .unwrap();
    crate::TaskDispatcher::new(
        world.db.clone(),
        world.bus.clone(),
        Arc::new(world.service.clone()),
    )
}

impl World {
    /// Run the dispatcher's reconciliation until the Task has a current
    /// attempt that is `queued` (or give up after a few laps).
    pub(super) async fn reconcile_until_admitted(&self, task: &str) -> Option<IntegrationAttempt> {
        let dispatcher = dispatcher(self).await;
        for _ in 0..40 {
            dispatcher.reconcile_all().await.unwrap();
            if let Some(attempt) = self.db.current_integration_attempt(task).await.unwrap() {
                if attempt.state == S::Queued
                    && attempt.original_candidate_sha.is_some()
                    && attempt.import_source_json.is_none()
                {
                    return Some(attempt);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        None
    }
}

impl World {
    /// The Project gets a workflow of its own: the default one with its
    /// merge, repair and done states renamed, and one more hook in front of
    /// `run_merge` on the merge state's entry.
    pub(super) async fn rename_merge_states(&self) {
        self.rename_states(false).await
    }
    /// The same, and with `every_role` the done, cancellation and review
    /// states renamed too (`shipped`, `dropped`, `inspection`).
    pub(super) async fn rename_states(&self, every_role: bool) {
        let mut workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow("{}");
        let rename = |name: &mut String| match name.as_str() {
            "merging" => *name = "landing".into(),
            "merge_failed" => *name = "fixing".into(),
            "done" if every_role => *name = "shipped".into(),
            "cancelled" if every_role => *name = "dropped".into(),
            "review" if every_role => *name = "inspection".into(),
            _ => {}
        };
        for state in &mut workflow.states {
            rename(&mut state.name);
            for trigger in state.triggers.values_mut() {
                rename(&mut trigger.to);
            }
            if state.name == "landing" {
                let mut extra = state.hooks.on_enter[0].clone();
                extra.action = "notify_role_holder".into();
                state.hooks.on_enter.insert(0, extra);
            }
        }
        let definition = serde_json::to_string(&workflow).unwrap();
        sqlx::query("UPDATE project SET workflow_definition=? WHERE id='p'")
            .bind(&definition)
            .execute(self.db.pool())
            .await
            .unwrap();
        let resolved = crate::workflow::engine::WorkflowEngine::resolve_workflow(&definition);
        assert!(
            super::super::reconcile::merge_state(&resolved, "landing")
                && !resolved.states.iter().any(|state| state.name == "merging"),
            "the Project's own workflow is in force"
        );
    }
}

pub(super) async fn merged_as(world: &World, task: &str, status: &str, production: &Production) {
    let running = production.run();
    world
        .until(task, &format!("{task} is {status}"), || async {
            world.task(task).await.status == status
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}

pub(super) fn retry_action() -> TaskAction {
    TaskAction::Retry {
        reason: None,
        fresh_session: None,
        refresh_workspace: None,
        reset_budget: None,
        guidance: None,
    }
}

impl World {
    /// A `merging` Task with no execution, imported as the non-current
    /// `missing execution or pinned workspace` quarantine and parked by the
    /// start's continuation. Returns the quarantined attempt's id.
    pub(super) async fn parked_quarantine(&self, task: &str) -> String {
        let imported = self.db.import_integration_pass(100).await.unwrap();
        assert_eq!(imported.quarantined, 1);
        import::import_at_start(&self.db).await.unwrap();
        self.until(task, "the quarantine is an owner park", || async {
            integration(&self.task(task).await)["cause"] == "owner_required"
        })
        .await;
        sqlx::query_scalar(
            "SELECT id FROM integration_attempt WHERE task_ref=? AND admission_key LIKE 'import:%'",
        )
        .bind(task)
        .fetch_one(self.db.pool())
        .await
        .unwrap()
    }
    /// Every Task-step row and scheduler generation of the Task.
    pub(super) async fn bookkeeping(
        &self,
        task: &str,
    ) -> (Vec<(String, String)>, Vec<(i64, i64, i64)>, i64) {
        let steps = sqlx::query_as("SELECT id,status FROM task_step WHERE task_id=? ORDER BY id")
            .bind(task)
            .fetch_all(self.db.pool())
            .await
            .unwrap();
        let dirty = sqlx::query_as(
            "SELECT generation,dirty,external FROM task_schedule_dirty WHERE task_id=?",
        )
        .bind(task)
        .fetch_all(self.db.pool())
        .await
        .unwrap();
        let attempts = self
            .scalar(
                "SELECT COUNT(*) FROM integration_attempt WHERE task_ref=?",
                task,
            )
            .await;
        (steps, dirty, attempts)
    }
    pub(super) async fn main_commits(&self) -> usize {
        git_out(&self.repo(), &["rev-list", "--count", "refs/heads/main"])
            .await
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }
}
