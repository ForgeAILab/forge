//! Query, budget, and synchronization operations for the usage ledger index.

use super::*;

impl UsageLedgerIndex {
    pub fn new(db: Arc<db::SqliteDb>) -> Self {
        Self {
            db,
            configured_budget_mb: AtomicU64::new(u64::MAX),
            state: tokio::sync::Mutex::new(State::default()),
            fallback: tokio::sync::Mutex::new(Fallback::default()),
            operations_fill: tokio::sync::Mutex::new(()),
            agents_fill: tokio::sync::Mutex::new(()),
        }
    }
    /// Update the setting immediately; observation allocations are reconciled on the next read.
    pub fn set_budget_mb(&self, budget: Option<u32>) {
        self.configured_budget_mb
            .store(budget.map_or(u64::MAX, u64::from), Ordering::SeqCst);
    }
    pub fn configured_budget_mb(&self) -> Option<u32> {
        u32::try_from(self.configured_budget_mb.load(Ordering::SeqCst)).ok()
    }
    pub fn budget_mb(&self) -> u32 {
        self.configured_budget_mb()
            .unwrap_or(config::DEFAULT_USAGE_INDEX_BUDGET_MB)
    }
    fn budget_bytes(&self) -> usize {
        usize::try_from(u64::from(self.budget_mb()) * 1024 * 1024).unwrap_or(usize::MAX)
    }
    pub async fn status(&self) -> api_types::UsageIndexStatus {
        let state = self.state.lock().await;
        let budget = self.budget_bytes();
        api_types::UsageIndexStatus {
            current_size_bytes: if state.watermarks.is_some() {
                state.charged_bytes() as u64
            } else {
                0
            },
            budget_bytes: budget as u64,
            fallback: budget == 0 || state.overflow.is_some() || state.bound(budget),
        }
    }
    pub async fn operations(&self) -> Result<UsageAggregate> {
        let mut state = self.state.lock().await;
        let header = self.sync(&mut state).await?;
        if state.overflow.is_none() {
            match state.scopes.entry(None).or_default().public() {
                Ok(value) => return Ok(value),
                Err(error) => self.invariant_failure(&mut state, header, &error),
            }
        }
        drop(state);
        if let Some(value) = {
            let memo = self.fallback.lock().await;
            if memo.header == Some(header) {
                memo.operations.clone()
            } else {
                None
            }
        } {
            return Ok(value);
        }
        // Coalesce concurrent reads of the same oversized ledger. This gate
        // is separate from Agent fills and the observation-index mutex.
        let _fill = self.operations_fill.lock().await;
        let mut tx = self.db.pool().begin().await?;
        let header = Watermarks::read(&mut tx).await?;
        self.prepare_fallback(&mut tx, header).await?;
        if let Some(value) = {
            let memo = self.fallback.lock().await;
            memo.operations.clone()
        } {
            return Ok(value);
        }
        let value = match overflow_usage_snapshot(&mut tx, None).await?.remove(&None) {
            Some(value) => value,
            None => {
                tracing::error!(
                    "usage overflow fold lost Operations scope; using independent fresh reference"
                );
                operations_usage_in_snapshot(&mut tx).await?
            }
        };
        tx.commit().await?;
        let mut memo = self.fallback.lock().await;
        if memo.header == Some(header) {
            memo.store_operations(&value);
        }
        Ok(value)
    }
    pub async fn agent(&self, id: &str) -> Result<UsageAggregate> {
        // agents() constructs an entry for every requested id, including ids
        // with no activity. This local fallback also preserves that contract.
        match self.agents(&[id.to_owned()]).await?.remove(id) {
            Some(value) => Ok(value),
            None => Totals::default().public(Vec::new()),
        }
    }
    pub async fn agents(&self, ids: &[String]) -> Result<HashMap<String, UsageAggregate>> {
        let mut state = self.state.lock().await;
        let header = self.sync(&mut state).await?;
        if state.overflow.is_none() {
            let result = ids
                .iter()
                .map(|id| {
                    let value = match state.scopes.get_mut(&Some(Arc::from(id.as_str()))) {
                        Some(scope) => scope.public()?,
                        None => Totals::default().public(Vec::new())?,
                    };
                    Ok((id.clone(), value))
                })
                .collect::<Result<HashMap<_, _>>>();
            match result {
                Ok(value) => return Ok(value),
                Err(error) => self.invariant_failure(&mut state, header, &error),
            }
        }
        drop(state);
        {
            let memo = self.fallback.lock().await;
            if memo.header == Some(header) && ids.iter().all(|id| memo.agents.contains_key(id)) {
                return Ok(ids
                    .iter()
                    .map(|id| (id.clone(), memo.agents[id].clone()))
                    .collect());
            }
        }
        let _fill = self.agents_fill.lock().await;
        let mut tx = self.db.pool().begin().await?;
        let header = Watermarks::read(&mut tx).await?;
        self.prepare_fallback(&mut tx, header).await?;
        let mut result = HashMap::new();
        let missing = {
            let memo = self.fallback.lock().await;
            ids.iter()
                .filter_map(|id| {
                    if let Some(value) = memo.agents.get(id) {
                        result.insert(id.clone(), value.clone());
                        None
                    } else {
                        Some(id.clone())
                    }
                })
                .collect::<Vec<_>>()
        };
        if !missing.is_empty() {
            let values = overflow_usage_snapshot(&mut tx, Some(&missing))
                .await?
                .into_iter()
                .filter_map(|(scope, value)| scope.map(|id| (id.to_string(), value)))
                .collect::<HashMap<_, _>>();
            let mut memo = self.fallback.lock().await;
            for (id, value) in values {
                if memo.header == Some(header) {
                    memo.store_agent(&id, &value);
                }
                result.insert(id, value);
            }
        }
        tx.commit().await?;
        Ok(result)
    }
    /// Assignment counts stay live; execution deltas supply page statistics.
    pub async fn agent_execution_stats(
        &self,
        ids: &[String],
    ) -> Result<HashMap<String, (db::AgentExecutionStats, i64)>> {
        let mut state = self.state.lock().await;
        self.sync(&mut state).await?;
        let mut result = HashMap::new();
        if state.overflow.is_none() {
            for id in ids {
                let stats = state
                    .execution_stats
                    .entry(Arc::from(id.as_str()))
                    .or_default();
                let average = if let Some(value) = stats.average() {
                    value
                } else {
                    let value = db::ExecutionRepo::stats_by_agent(&*self.db, id)
                        .await?
                        .avg_duration_ms;
                    if stats.unstable == 0 {
                        stats.cached_average = Some(value);
                    }
                    value
                };
                result.insert(
                    id.clone(),
                    (
                        db::AgentExecutionStats {
                            avg_duration_ms: average,
                            success_rate: (stats.count > 0)
                                .then_some(stats.completed as f64 / stats.count as f64),
                        },
                        stats.running,
                    ),
                );
            }
            return Ok(result);
        }
        drop(state);
        let mut tx = self.db.pool().begin().await?;
        let header = Watermarks::read(&mut tx).await?;
        self.prepare_fallback(&mut tx, header).await?;
        let missing = {
            let memo = self.fallback.lock().await;
            ids.iter()
                .filter_map(|id| {
                    if let Some(value) = memo.stats.get(id) {
                        result.insert(id.clone(), value.clone());
                        None
                    } else {
                        Some(id.clone())
                    }
                })
                .collect::<Vec<_>>()
        };
        if !missing.is_empty() {
            let rows = db::usage_index_agent_execution_stats_on_connection(
                &mut tx,
                serde_json::to_string(&missing).map_err(|_| invalid_transition())?,
            )
            .await?;
            let mut memo = self.fallback.lock().await;
            for row in rows {
                let id: String = row.id()?;
                let n: i64 = row.n()?;
                let complete: i64 = row.completed()?;
                let value = (
                    db::AgentExecutionStats {
                        avg_duration_ms: row.duration()?.map(|v| v.round() as i64),
                        success_rate: (n > 0).then_some(complete as f64 / n as f64),
                    },
                    row.running()?,
                );
                if memo.header == Some(header) && row.unstable()? == 0 {
                    memo.room(256);
                    memo.stats.insert(id.clone(), value.clone());
                    memo.bytes += 256;
                }
                result.insert(id, value);
            }
        }
        tx.commit().await?;
        Ok(result)
    }
    async fn prepare_fallback(
        &self,
        connection: &mut sqlx::SqliteConnection,
        header: Watermarks,
    ) -> Result<()> {
        // This small gate covers cache invalidation only. Fresh computations
        // run outside both mutexes, so unrelated requests can execute together.
        let mut memo = self.fallback.lock().await;
        if memo.header == Some(header) {
            return Ok(());
        }
        let affected = if let Some(old) = memo.header {
            if old.deletion_generation != header.deletion_generation
                || old.execution_revision != header.execution_revision
                || old
                    .rows
                    .iter()
                    .zip(header.rows)
                    .zip([
                        (old.invocation_rowid, header.invocation_rowid),
                        (old.event_rowid, header.event_rowid),
                        (old.estimate_rowid, header.estimate_rowid),
                        (old.execution_rowid, header.execution_rowid),
                    ])
                    .any(|((&before, after), (old_rowid, new_rowid))| {
                        after > before && new_rowid <= old_rowid
                    })
            {
                None
            } else {
                let rows = db::usage_index_affected_agents_on_connection(
                    &mut *connection,
                    db::UsageIndexAffectedAgentsOnConnectionInput {
                        old_event_rowid: old.event_rowid,
                        event_rowid: header.event_rowid,
                        old_estimate_rowid: old.estimate_rowid,
                        estimate_rowid: header.estimate_rowid,
                        old_invocation_rowid: old.invocation_rowid,
                        invocation_rowid: header.invocation_rowid,
                        old_invocation_revision: old.invocation_revision,
                        invocation_revision: header.invocation_revision,
                        old_execution_rowid: old.execution_rowid,
                        execution_rowid: header.execution_rowid,
                    },
                )
                .await?;
                let mut affected = HashSet::new();
                for row in rows {
                    for column in [
                        db::UsageIndexAffectedAgentField::Owner,
                        db::UsageIndexAffectedAgentField::Attributed,
                    ] {
                        if let Some(id) = row.agent(column)? {
                            affected.insert(id);
                        }
                    }
                }
                Some(affected)
            }
        } else {
            None
        };
        // No cache mutation above: cancellation leaves the old header and
        // values intact. Header and invalidation are now published together.
        if let Some(affected) = affected {
            memo.agents.retain(|id, _| !affected.contains(id));
            if memo
                .header
                .is_some_and(|old| old.execution_rowid != header.execution_rowid)
            {
                memo.stats.clear();
            }
        } else {
            memo.agents.clear();
            memo.stats.clear();
        }
        memo.operations = None;
        memo.header = Some(header);
        // Conservative charge need not decrease on selective invalidation;
        // insertion's room() clears it before it can exceed the fixed budget.
        Ok(())
    }
    pub(super) fn minimum_charge(header: Watermarks) -> usize {
        // Every execution requires a global run, plus a distinct Agent run
        // when owned. These cannot coalesce because execution ids are unique.
        // Account for the standard HashMap bucket allocation (validated by a
        // schema-independent capacity test), without allocating those tables.
        fn table<K, V>(rows: i64) -> usize {
            if rows <= 0 {
                return 0;
            }
            let buckets = (rows as usize)
                .saturating_mul(8)
                .div_ceil(7)
                .max(4)
                .checked_next_power_of_two()
                .unwrap_or(usize::MAX);
            buckets.saturating_mul(std::mem::size_of::<(K, V)>() + 2)
        }
        let live = table::<Arc<str>, Invocation>(header.rows[0])
            .saturating_add(table::<Arc<str>, Execution>(header.rows[3]))
            .saturating_add(table::<ScopedRun, Run>(
                header.rows[3].saturating_add(header.owned_executions),
            ));
        live.saturating_mul(3) / 2
            + if header.rows[0] > 0 || header.rows[3] > 0 {
                READ_MEMORY_RESERVE
            } else {
                0
            }
    }
    fn invariant_failure(&self, state: &mut State, header: Watermarks, error: &ServiceError) {
        tracing::error!(%error,"usage index invariant failed; using fresh computation");
        *state = State {
            observed_header: Some(header),
            overflow: Some(Overflow {
                budget: self.budget_bytes(),
                header,
                rows: header.rows.iter().sum(),
            }),
            ..State::default()
        };
    }
    fn discard(&self, state: &mut State, header: Watermarks, charge: usize, budget: usize) {
        if budget > 0 {
            tracing::warn!(
                charge,
                bound = budget,
                invocations = header.rows[0],
                events = header.rows[1],
                estimates = header.rows[2],
                executions = header.rows[3],
                "usage index discarded at memory bound"
            );
        }
        *state = State {
            observed_header: Some(header),
            overflow: Some(Overflow {
                budget,
                header,
                rows: header.rows.iter().sum(),
            }),
            ..State::default()
        };
    }
    // A synchronous API is the cancellation boundary: the compiler cannot
    // permit an await between the first mutation and watermark publication.
    #[allow(clippy::too_many_arguments)]
    fn publish(
        &self,
        state: &mut State,
        staged: Staged,
        repairs: Vec<CitationRepair>,
        actual: Watermarks,
        observed: Watermarks,
        cold: bool,
        was_overflow: bool,
        budget: usize,
    ) {
        #[cfg(test)]
        let audit = match &staged {
            Staged::Cold { state, .. } => state.audit,
            Staged::Delta { audit, .. } => *audit,
        };
        match staged {
            Staged::Cold { state: fresh, .. } => *state = *fresh,
            Staged::Delta { changes, bytes, .. } => {
                let mut remaining = bytes - state.charged_bytes();
                for change in changes {
                    remaining -= change.staging_bytes();
                    // Published summaries plus still-private rows share one
                    // budget, including a hash-table capacity growth step.
                    if state.projected_charge(&change).saturating_add(remaining) > budget {
                        self.discard(state, observed, state.charged_bytes(), budget);
                        return;
                    }
                    if let Err(error) = change.apply(state) {
                        self.invariant_failure(state, observed, &error);
                        return;
                    }
                }
            }
        }
        for (scope, key, reference, winner) in repairs {
            if let Some(book) = state
                .scopes
                .get_mut(&scope)
                .and_then(|r| r.sources.get_mut(&key))
            {
                let before = book.heap_bytes();
                if let Err(error) = book.repair(&reference, winner) {
                    self.invariant_failure(state, observed, &error);
                    return;
                }
                let after = book.heap_bytes();
                if let Some(bytes) = state
                    .bytes
                    .checked_add_signed(after as isize - before as isize)
                {
                    state.bytes = bytes;
                } else {
                    self.invariant_failure(state, observed, &invalid_transition());
                    return;
                }
            }
        }
        // No await is allowed from the first published mutation above through
        // these watermark assignments. Dropping a future is safe at every await.
        state.watermarks = Some(actual);
        state.observed_header = Some(observed);
        #[cfg(test)]
        {
            let updates = state.audit.attempt_updates;
            state.audit = audit;
            state.audit.attempt_updates = updates;
        }
        if state.bound(budget) {
            self.discard(state, observed, state.charged_bytes(), budget);
        } else if cold {
            tracing::info!(
                charge = state.charged_bytes(),
                invocations = actual.rows[0],
                events = actual.rows[1],
                executions = actual.rows[3],
                "usage index built"
            );
        }
        if was_overflow && state.overflow.is_none() {
            tracing::info!(charge = state.charged_bytes(), "usage index fits again");
        }
    }

    async fn sync(&self, state: &mut State) -> Result<Watermarks> {
        let budget = self.budget_bytes();
        #[cfg(test)]
        {
            state.audit = Audit {
                statements: 1,
                ..Audit::default()
            };
        }
        let header = {
            let mut connection = self.db.pool().acquire().await?;
            Watermarks::read(&mut connection).await?
        };
        if budget == 0 {
            if state.overflow.is_none_or(|overflow| overflow.budget != 0) {
                self.discard(state, header, state.charged_bytes(), budget);
            } else {
                state.observed_header = Some(header);
            }
            return Ok(header);
        }
        if state.bound(budget) {
            self.discard(state, header, state.charged_bytes(), budget);
            return Ok(header);
        }
        if state.observed_header == Some(header)
            && state
                .overflow
                .is_none_or(|overflow| overflow.budget == budget)
        {
            return Ok(header);
        }
        if let Some(overflow) = state.overflow {
            // Counts are trigger-maintained, so this fit probe is O(1). A tiny
            // deletion does not repeat an expensive, known-oversized build.
            if overflow.budget == budget
                && (header.deletion_generation == overflow.header.deletion_generation
                    || header.rows.iter().sum::<i64>() > overflow.rows * 3 / 4)
            {
                state.observed_header = Some(header);
                return Ok(header);
            }
        }
        let mut tx = self.db.pool().begin().await?;
        let observed = Watermarks::read(&mut tx).await?;
        let stalled_cursor = state.observed_header.is_some_and(|old| {
            old.rows
                .iter()
                .zip(observed.rows)
                .zip([
                    (old.invocation_rowid, observed.invocation_rowid),
                    (old.event_rowid, observed.event_rowid),
                    (old.estimate_rowid, observed.estimate_rowid),
                    (old.execution_rowid, observed.execution_rowid),
                ])
                .any(|((&before, after), (old_rowid, new_rowid))| {
                    after > before && new_rowid <= old_rowid
                })
        });
        let cold = stalled_cursor
            || state
                .watermarks
                .is_none_or(|old| old.deletion_generation != observed.deletion_generation);
        let mut actual = observed;
        if !cold {
            // A cold-build repair may have found rowids above stale migration
            // metadata. Preserve those actual scan positions until a normal
            // insert brings the singleton maxima back into agreement.
            let old = state.watermarks.expect("warm state");
            actual.invocation_rowid = actual.invocation_rowid.max(old.invocation_rowid);
            actual.event_rowid = actual.event_rowid.max(old.event_rowid);
            actual.estimate_rowid = actual.estimate_rowid.max(old.estimate_rowid);
            actual.execution_rowid = actual.execution_rowid.max(old.execution_rowid);
        }

        if cold {
            let row = db::usage_index_actual_rowids_on_connection(&mut tx).await?;
            actual.invocation_rowid = row.i()?;
            actual.event_rowid = row.e()?;
            actual.estimate_rowid = row.r()?;
            actual.execution_rowid = row.x()?;
            if actual != observed {
                tracing::error!(
                    ?observed,
                    ?actual,
                    "usage rowid header mismatch; rebuilding observation index"
                );
            }
            if stalled_cursor {
                tracing::error!("usage row count advanced without its rowid cursor; rebuilding observation index");
            }
            let minimum = Self::minimum_charge(observed);
            if minimum > budget {
                self.discard(state, observed, minimum, budget);
                return Ok(observed);
            }
        }
        let was_overflow = state.overflow.is_some();
        if cold && state.watermarks.is_some() {
            // Deleted ledger rows invalidate the old observation state. Drop
            // its allocations before building the private replacement so a
            // reset cannot keep two full indexes alive under one budget.
            *state = State::default();
        }
        let old = if cold {
            Watermarks::default()
        } else {
            state.watermarks.expect("warm state")
        };
        let mut staged = if cold {
            Staged::Cold {
                state: Box::default(),
                discard_charge: None,
            }
        } else {
            Staged::Delta {
                changes: Vec::new(),
                bytes: state.charged_bytes(),
                #[cfg(test)]
                audit: Audit::default(),
            }
        };
        let result = read_delta(&mut staged, &mut tx, old, actual, cold, budget).await;
        if let Err(error) = result {
            if matches!(error, ServiceError::Db(db::DbError::InvalidTransition)) {
                self.invariant_failure(state, observed, &error);
                return Ok(observed);
            }
            return Err(error);
        }
        let charge = staged.charged_bytes();
        if staged.overflowed(budget) {
            self.discard(state, observed, charge, budget);
            return Ok(observed);
        }
        let repairs = if cold {
            Vec::new()
        } else {
            match citation_repairs(state, &staged, &mut tx, actual, budget).await {
                Ok(CitationRepairs::Ready(repairs, reserved)) => {
                    if let Staged::Delta { bytes, .. } = &mut staged {
                        // Completed repair buffers remain alive while changes
                        // are published, so publication must retain their reserve.
                        *bytes = bytes.saturating_add(reserved);
                    }
                    repairs
                }
                Ok(CitationRepairs::Overflow(charge)) => {
                    self.discard(state, observed, charge, budget);
                    return Ok(observed);
                }
                Err(error) if matches!(error, ServiceError::Db(db::DbError::InvalidTransition)) => {
                    self.invariant_failure(state, observed, &error);
                    return Ok(observed);
                }
                Err(error) => return Err(error),
            }
        };
        // Finish every fallible/awaiting read before touching published state.
        tx.commit().await?;
        self.publish(
            state,
            staged,
            repairs,
            actual,
            observed,
            cold,
            was_overflow,
            budget,
        );
        Ok(observed)
    }
}
