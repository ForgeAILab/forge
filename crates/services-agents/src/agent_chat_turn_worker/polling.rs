//! Chat turn polling, claiming, and worker startup.

use super::*;

impl AgentChatTurnWorker {
    pub fn new(
        db: Arc<SqliteDb>,
        embedded_agents: Arc<EmbeddedAgentService>,
        cli_executor: Arc<dyn TaskExecutor>,
        turn_logs: AgentChatTurnLogRoot,
    ) -> Self {
        let runner = Arc::new(FederatedAgentChatTurnRunner::new(
            Arc::clone(&db),
            embedded_agents,
            cli_executor,
            turn_logs,
        ));
        Self::with_runner(db, runner)
    }

    pub fn with_runner(db: Arc<SqliteDb>, runner: Arc<dyn AgentChatTurnRunner>) -> Self {
        Self {
            chat_service: Arc::new(AgentChatService::new(Arc::clone(&db))),
            db,
            runner,
            lease_owner: format!("agent-chat-worker:{}", db::new_uuid_v4()),
            lease_renew_interval: LEASE_RENEW_INTERVAL,
            rotation_pass: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    #[cfg(test)]
    pub(super) fn with_lease_renew_interval(mut self, interval: Duration) -> Self {
        self.lease_renew_interval = interval;
        self
    }

    pub fn start(
        self: Arc<Self>,
        workers: &crate::worker_runtime::PeriodicWorkers,
        shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        self.start_with_claim(
            workers,
            shutdown,
            Duration::from_secs(300),
            |worker, capacity| async move { worker.claim_available(capacity).await },
        )
    }

    pub(super) fn start_with_claim<F, Fut>(
        self: Arc<Self>,
        workers: &crate::worker_runtime::PeriodicWorkers,
        shutdown: watch::Receiver<bool>,
        timeout: Duration,
        claim: F,
    ) -> JoinHandle<()>
    where
        F: Fn(Arc<Self>, usize) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Vec<AgentChatTurnJob>>> + Send + 'static,
    {
        let claim = Arc::new(claim);
        workers.worker("agent-chat-turns").with_stall_budget(timeout)
            .start(shutdown, || false, move |periodic, mut shutdown| {
                let self_worker = Arc::clone(&self);
                let claim = Arc::clone(&claim);
                async move {
                    let cancellation = CancellationToken::new();
                    let _cancel_on_drop = cancellation.clone().drop_guard();
                    let mut active = tokio::task::JoinSet::new();
                    // Borrow the in-flight set from outside the unwind boundary.
                    // Poller failure must cancel and drain before restarting.
                    let exit = {
                        let polling = async {
                            let mut poll = tokio::time::interval(POLL_INTERVAL);
                            poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                            loop {
                                tokio::select! {
                                    changed = shutdown.changed() => {
                                        if changed.is_err() || *shutdown.borrow_and_update() { break; }
                                    }
                                    _ = poll.tick(), if active.len() < MAX_ACTIVE_TURNS => {
                                        match periodic.tick(claim(Arc::clone(&self_worker), MAX_ACTIVE_TURNS - active.len())).await {
                                            Ok(jobs) => for job in jobs {
                                                let worker = Arc::clone(&self_worker);
                                                let token = cancellation.child_token();
                                                active.spawn(async move { worker.process_claimed(job, token).await; });
                                            },
                                            Err(error) => tracing::warn!(worker = periodic.name(), error = %error, "Agent Chat turn polling failed"),
                                        }
                                    }
                                    Some(result) = active.join_next(), if !active.is_empty() => {
                                        if let Err(error) = result {
                                            tracing::warn!(error = %error, "Agent Chat worker task stopped unexpectedly");
                                        }
                                    }
                                }
                            }
                        };
                        tokio::pin!(polling);
                        std::future::poll_fn(|cx| {
                            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| polling.as_mut().poll(cx))) {
                                Ok(std::task::Poll::Ready(())) => std::task::Poll::Ready(Ok(())),
                                Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
                                Err(panic) => std::task::Poll::Ready(Err(panic)),
                            }
                        }).await
                    };
                    cancellation.cancel();
                    let drain = async {
                        while let Some(result) = active.join_next().await {
                            if let Err(error) = result {
                                tracing::warn!(worker = periodic.name(), error = %error, "Agent Chat worker task stopped during shutdown");
                            }
                        }
                    };
                    // Shutdown is bounded by the owning supervisor. Panic cleanup is
                    // bounded by PANIC_DRAIN_TIMEOUT: a non-cooperative turn must not
                    // prevent restart while renewing its lease indefinitely.
                    let drained = if exit.is_err() {
                        tokio::time::timeout(PANIC_DRAIN_TIMEOUT, drain).await.is_ok()
                    } else {
                        drain.await;
                        true
                    };
                    if !drained {
                        tracing::warn!(worker = periodic.name(), timeout_secs = PANIC_DRAIN_TIMEOUT.as_secs(), "Agent Chat turn drain timed out");
                        active.abort_all();
                        while let Some(result) = active.join_next().await {
                            if let Err(error) = result {
                                tracing::warn!(worker = periodic.name(), error = %error, "Agent Chat worker task stopped during shutdown");
                            }
                        }
                    }
                    if let Err(panic) = exit { std::panic::resume_unwind(panic); }
                    Ok(())
                }
            })
    }

    pub async fn run_once(&self) -> Result<usize> {
        self.run_once_at(Utc::now()).await
    }

    /// Select due work at an explicit clock instant; leases retain real time.
    pub async fn run_once_at(&self, now: chrono::DateTime<Utc>) -> Result<usize> {
        self.recover_expired().await?;
        // Deterministic for callers: due rotations complete (or back off)
        // before this pass claims. The polling loop runs them off-path.
        crate::topic_rotation::run_topic_rotation_pass(&self.db, &self.runner, now).await?;
        let jobs: Vec<_> = self.claim_one_at(now).await?.into_iter().collect();
        let count = jobs.len();
        for job in jobs {
            self.process_claimed(job, CancellationToken::new()).await;
        }
        Ok(count)
    }

    pub(super) async fn claim_available(&self, capacity: usize) -> Result<Vec<AgentChatTurnJob>> {
        let mut jobs = Vec::with_capacity(capacity);
        self.recover_expired().await?;
        self.spawn_topic_rotation_pass();
        for _ in 0..capacity {
            let Some(job) = self.claim_one().await? else {
                break;
            };
            jobs.push(job);
        }
        Ok(jobs)
    }

    pub(super) async fn claim_one(&self) -> Result<Option<AgentChatTurnJob>> {
        self.claim_one_at(Utc::now()).await
    }

    /// Starts one topic rotation pass in the background unless one is
    /// already running, so a rotation's provider summary call never delays
    /// claiming any chat's turns.
    fn spawn_topic_rotation_pass(&self) {
        let Ok(guard) = Arc::clone(&self.rotation_pass).try_lock_owned() else {
            return;
        };
        let db = Arc::clone(&self.db);
        let runner = Arc::clone(&self.runner);
        tokio::spawn(async move {
            let _guard = guard;
            if let Err(error) =
                crate::topic_rotation::run_topic_rotation_pass(&db, &runner, Utc::now()).await
            {
                tracing::warn!(%error, "topic rotation pass failed");
            }
        });
    }

    async fn claim_one_at(&self, now: chrono::DateTime<Utc>) -> Result<Option<AgentChatTurnJob>> {
        let now = now.to_rfc3339();
        let leased_until = lease_deadline();
        let mut transaction = db::begin_immediate(self.db.pool()).await?;
        let id = self
            .db
            .claim_fifo_chat_turn_in_tx(
                &mut transaction,
                db::ClaimFifoChatTurnInTxInput {
                    available_at: &now,
                    backoff_at: &now,
                    lease_owner: &self.lease_owner,
                    leased_until: &leased_until,
                    updated_at: &now,
                },
            )
            .await?;
        if let Some(id) = id.as_deref() {
            self.db
                .append_agent_chat_turn_status_in_tx(&mut transaction, id)
                .await?;
        }
        transaction.commit().await?;
        let Some(id) = id else { return Ok(None) };
        AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, &id)
            .await?
            .ok_or_else(|| ServiceError::not_found("agent_chat_turn_job", id))
            .map(Some)
    }
}
