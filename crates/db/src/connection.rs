use crate::{DbError, Result, SqliteStorageStatus};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteTransactionManager},
    Connection, Sqlite, SqliteConnection, SqlitePool, Transaction, TransactionManager,
};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard, OnceLock, Weak,
    },
    time::{Duration, Instant},
};
use tokio::sync::Notify;

/// One open write transaction: where it began and when it got the lock.
struct WriteHold {
    site: &'static std::panic::Location<'static>,
    database: String,
    since: Instant,
}
/// A write transaction held longer than this is reported when it ends.
const WRITE_HOLD_WARN: Duration = Duration::from_secs(5);
/// Open `begin_immediate` transactions of this process by connection handle.
/// SQLite has one writer per database, so more than one entry for a database
/// is a second transaction waiting behind the first.
fn write_holds() -> MutexGuard<'static, HashMap<usize, WriteHold>> {
    static HOLDS: OnceLock<Mutex<HashMap<usize, WriteHold>>> = OnceLock::new();
    match HOLDS.get_or_init(Mutex::default).lock() {
        Ok(guard) => guard,
        Err(poison) => poison.into_inner(),
    }
}
/// Called from the connection's commit and rollback hooks.
fn end_write_hold(key: usize, outcome: &'static str) {
    let Some(hold) = write_holds().remove(&key) else {
        return;
    };
    let held = hold.since.elapsed();
    if held >= WRITE_HOLD_WARN {
        tracing::warn!(
            site = %hold.site,
            database = %hold.database,
            held_ms = held.as_millis() as u64,
            outcome,
            "a write transaction held the database write lock for a long time"
        );
    }
}
/// Callers inside `begin_immediate`: waiting for a connection or the write
/// lock, or holding the lock and not yet recorded as its holder.
fn write_waits() -> MutexGuard<'static, HashMap<u64, WriteHold>> {
    static WAITS: OnceLock<Mutex<HashMap<u64, WriteHold>>> = OnceLock::new();
    match WAITS.get_or_init(Mutex::default).lock() {
        Ok(guard) => guard,
        Err(poison) => poison.into_inner(),
    }
}
/// Removes its entry when `begin_immediate` returns or is dropped mid-wait.
struct WriteWait(u64);
impl WriteWait {
    fn new(site: &'static std::panic::Location<'static>, database: String) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        write_waits().insert(
            id,
            WriteHold {
                site,
                database,
                since: Instant::now(),
            },
        );
        Self(id)
    }
}
impl Drop for WriteWait {
    fn drop(&mut self) {
        write_waits().remove(&self.0);
    }
}
/// The write transactions of this process right now, oldest first: where each
/// began, on which database, and for how long it has held the write lock or
/// has been beginning. What a caller that found the database busy was waiting
/// for.
pub fn open_write_transactions() -> Vec<String> {
    let describe = |state: &str, holds: &mut dyn Iterator<Item = &WriteHold>| {
        let mut open: Vec<_> = holds.collect();
        open.sort_by_key(|hold| hold.since);
        open.iter()
            .map(|hold| {
                format!(
                    "{state} {} on {} for {} ms",
                    hold.site,
                    hold.database,
                    hold.since.elapsed().as_millis()
                )
            })
            .collect::<Vec<_>>()
    };
    let mut open = describe("holds", &mut write_holds().values());
    open.extend(describe("begins", &mut write_waits().values()));
    open
}

#[derive(Debug, Default)]
pub(crate) struct EventHooks {
    committed: Mutex<HashSet<usize>>,
    pending: AtomicUsize,
    generation: AtomicU64,
    notify: Arc<Notify>,
    #[cfg(test)]
    release_inspections: AtomicUsize,
    // Marks and deliveries let a test wait until every committed append has
    // been announced; releases run on a background task.
    #[cfg(test)]
    marks: AtomicUsize,
    #[cfg(test)]
    deliveries: AtomicUsize,
}
impl EventHooks {
    fn guard(&self) -> MutexGuard<'_, HashSet<usize>> {
        match self.committed.lock() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        }
    }
    fn mark(&self, key: usize) {
        let mut committed = self.guard();
        let _inserted = committed.insert(key);
        #[cfg(test)]
        if _inserted {
            self.marks.fetch_add(1, Ordering::Release);
        }
        self.pending.store(committed.len(), Ordering::Release);
    }
    fn take(&self, key: usize) -> bool {
        let mut committed = self.guard();
        let removed = committed.remove(&key);
        self.pending.store(committed.len(), Ordering::Release);
        removed
    }
    pub(crate) fn pending_commit(&self) -> bool {
        self.pending.load(Ordering::Acquire) > 0
    }
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
    pub(crate) fn notify(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }
}
fn notifier_registry() -> &'static Mutex<HashMap<usize, Weak<EventHooks>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<usize, Weak<EventHooks>>>> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}
fn registry_guard() -> MutexGuard<'static, HashMap<usize, Weak<EventHooks>>> {
    match notifier_registry().lock() {
        Ok(guard) => guard,
        Err(poison) => poison.into_inner(),
    }
}
fn pool_key(pool: &SqlitePool) -> usize {
    Arc::as_ptr(&pool.connect_options()) as usize
}
pub(crate) fn domain_event_hooks(pool: &SqlitePool) -> Arc<EventHooks> {
    let mut registry = registry_guard();
    if let Some(hooks) = registry.get(&pool_key(pool)).and_then(Weak::upgrade) {
        return hooks;
    }
    let hooks = Arc::new(EventHooks::default());
    registry.insert(pool_key(pool), Arc::downgrade(&hooks));
    hooks
}

pub async fn create_sqlite_pool(database_url: &str) -> Result<SqlitePool> {
    let max_connections = if database_url.contains(":memory:") {
        1
    } else {
        5
    };
    let options = SqliteConnectOptions::from_str(database_url)?.create_if_missing(true);
    // A private in-memory database IS its one connection: SQLx replaces a
    // connection it closed with a new one, and that one opens another, empty
    // database. Nothing below closes it, and a second connect says so.
    let memory = database_url.contains(":memory:");
    let connected = Arc::new(AtomicBool::new(false));

    // Connection identities are opaque keys only; no pointer is dereferenced.
    // SQLite's commit hook is BEFORE visibility. It only marks a connection;
    // SQLx release delivers the notification AFTER the commit has completed.
    let hooks = Arc::new(EventHooks::default());
    let on_connect = Arc::clone(&hooks);
    let on_release = Arc::clone(&hooks);
    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections)
        // A release must pass through the notification hook rather than the
        // lifetime-expiry close path. Idle connections can still be recycled.
        .max_lifetime(None)
        // SQLx pings an idle connection before it hands it out, and holds it
        // outside the pool while it waits for the answer: an `acquire` future
        // dropped there (an aborted task, a lost `select!` branch, a timeout,
        // a disconnected request) closes the connection with it. The ping
        // proves nothing for SQLite, whose connection is a thread of this
        // process; without it an idle connection is handed out with no await
        // point, and every release is still tested.
        .test_before_acquire(false)
        .acquire_timeout(Duration::from_secs(30));
    // The idle reaper closes a connection too (SQLx default: ten minutes).
    let pool = if memory {
        pool.idle_timeout(None)
    } else {
        pool
    };
    let pool = pool
        .after_connect(move |connection, _metadata| {
            let hooks = Arc::clone(&on_connect);
            let connected = Arc::clone(&connected);
            Box::pin(async move {
                // Loud, because the alternative is `no such table` from an
                // unrelated statement much later.
                assert!(
                    !(memory && connected.load(Ordering::Acquire)),
                    "the connection of an in-memory SQLite pool was closed and is being replaced: \
                     the replacement is a new, empty database and everything written so far is gone"
                );
                // Only empty databases can enable auto-vacuum without a full
                // rebuild. Set this before WAL, which dirties even an empty
                // file. Never change an existing database's mode here.
                let tables: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                ).fetch_one(&mut *connection).await?;
                if tables == 0 {
                    sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
                        .execute(&mut *connection).await?;
                }

                sqlx::query("PRAGMA foreign_keys = ON")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("PRAGMA recursive_triggers = ON")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("PRAGMA journal_mode = WAL")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("PRAGMA synchronous = NORMAL")
                    .execute(&mut *connection)
                    .await?;
                // `FORGE_DB_BUSY_TIMEOUT_MS` is a diagnostic knob: with a
                // short timeout a write lock held too long, or one the
                // waiter itself keeps from being released, fails a test in
                // seconds and the busy log names the holder.
                let busy_timeout = std::env::var("FORGE_DB_BUSY_TIMEOUT_MS")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(30_000);
                sqlx::query(&format!("PRAGMA busy_timeout = {busy_timeout}"))
                    .execute(&mut *connection)
                    .await?;
                let dirty = Arc::new(AtomicBool::new(false));
                let mut handle = connection.lock_handle().await?;
                let key = handle.as_raw_handle().as_ptr() as usize;
                hooks.take(key);
                let updated = Arc::clone(&dirty);
                handle.set_update_hook(move |update| {
                    if (update.table == "agent_session" && update.operation == sqlx::sqlite::SqliteOperation::Delete) || (update.table == "domain_event" && update.operation == sqlx::sqlite::SqliteOperation::Insert) || ((update.table == "task_schedule_dirty" || update.table == "project_schedule_dirty") && update.operation != sqlx::sqlite::SqliteOperation::Delete) {
                        updated.store(true, Ordering::Relaxed);
                    }
                });
                let rollback_dirty = Arc::clone(&dirty);
                handle.set_rollback_hook(move || {
                    end_write_hold(key, "rolled back");
                    rollback_dirty.store(false, Ordering::Relaxed);
                    // Preserve an earlier committed append on a connection
                    // retained across transactions. A failed COMMIT can cause
                    // a harmless extra wake, never a lost committed append.
                });
                handle.set_commit_hook(move || {
                    end_write_hold(key, "committed");
                    if dirty.swap(false, Ordering::Relaxed) {
                        hooks.mark(key);
                    }
                    true
                });
                drop(handle);
                connected.store(true, Ordering::Release);
                Ok(())
            })
        })
        .after_release(move |connection, _metadata| {
            let hooks = Arc::clone(&on_release);
            Box::pin(async move {
                // A custom BEGIN is acknowledged before SQLx awaits its
                // handle check and constructs the Transaction rollback guard.
                // Dropping that future can return an open transaction here.
                // Flush it (and any savepoints) before another owner gets the
                // connection. The ordinary path is only an atomic depth read.
                while SqliteTransactionManager::get_transaction_depth(connection) > 0 {
                    SqliteTransactionManager::rollback(connection).await?;
                }
                // The common read/non-event-write path needs neither a ping nor
                // a trip to the SQLite worker. A marker is published in the
                // pre-commit hook; delivery waits for this committed release.
                if hooks.pending.load(Ordering::Acquire) == 0 { return Ok(true); }
                #[cfg(test)] hooks.release_inspections.fetch_add(1, Ordering::Relaxed);
                // lock_handle queues behind any outstanding rollback/COMMIT;
                // SQLx's own release check subsequently pings the connection.
                let key = connection.lock_handle().await?.as_raw_handle().as_ptr() as usize;
                if hooks.take(key) {
                    hooks.generation.fetch_add(1, Ordering::Release);
                    hooks.notify.notify_waiters();
                    #[cfg(test)] hooks.deliveries.fetch_add(1, Ordering::Release);
                }
                Ok(true)
            })
        })
        .connect_with(options)
        .await?;

    registry_guard().insert(pool_key(&pool), Arc::downgrade(&hooks));
    Ok(pool)
}

/// Begins a write transaction with `BEGIN IMMEDIATE`, acquiring the SQLite
/// write lock up front. A plain (deferred) `BEGIN` only upgrades to a write
/// lock lazily, on the first write statement — and that upgrade does not
/// honor `busy_timeout`, so under contention it fails instantly with
/// SQLITE_BUSY_SNAPSHOT instead of retrying. Use this for any transaction
/// that performs writes.
///
/// Returns `sqlx::Result` (not `crate::Result`) so it is a drop-in
/// replacement for `pool.begin()` at every call site, including outside the
/// `db` crate, without changing error-conversion paths.
///
/// Every transaction begun here is recorded until its connection commits or
/// rolls back: [`open_write_transactions`] names the holders, and one held
/// longer than five seconds is logged with the place that began it.
#[track_caller]
pub fn begin_immediate(
    pool: &SqlitePool,
) -> impl std::future::Future<Output = sqlx::Result<Transaction<'static, Sqlite>>> + Send + '_ {
    // The caller is the label of this transaction in the write-hold record.
    let site = std::panic::Location::caller();
    async move {
        tracing::debug!(target: "forge_db::write_transaction", %site, "begin immediate");
        let database = pool
            .connect_options()
            .get_filename()
            .to_string_lossy()
            .into_owned();
        let wait = WriteWait::new(site, database.clone());
        let started = Instant::now();
        let begun = pool.begin_with("BEGIN IMMEDIATE").await;
        let waited = started.elapsed();
        if waited >= WRITE_HOLD_WARN {
            tracing::warn!(
                %site,
                %database,
                waited_ms = waited.as_millis() as u64,
                acquired = begun.is_ok(),
                writers = ?open_write_transactions(),
                "a write transaction waited a long time for a connection and the database write lock"
            );
        }
        let mut transaction = begun?;
        let key = transaction.lock_handle().await?.as_raw_handle().as_ptr() as usize;
        write_holds().insert(
            key,
            WriteHold {
                site,
                database,
                since: Instant::now(),
            },
        );
        drop(wait);
        Ok(transaction)
    }
}

/// Operator diagnostics for the database's persistent vacuum mode and freelist.
pub async fn sqlite_storage_status(pool: &SqlitePool) -> Result<SqliteStorageStatus> {
    let mut conn = pool.acquire().await?;
    let mode: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
        .fetch_one(&mut *conn)
        .await?;
    let free_pages = sqlx::query_scalar("PRAGMA freelist_count")
        .fetch_one(&mut *conn)
        .await?;
    Ok(SqliteStorageStatus {
        incremental_vacuum: mode == 2,
        free_pages,
    })
}

/// Release at most 100 free pages per maintenance pass. This is deliberately a
/// no-op on existing databases until the operator converts them offline.
pub async fn incremental_vacuum(pool: &SqlitePool) -> Result<()> {
    let mut conn = pool.acquire().await?;
    let mode: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
        .fetch_one(&mut *conn)
        .await?;
    if mode != 2 {
        return Ok(());
    }
    // Skip the write lock entirely when there is nothing to release.
    let free_pages: i64 = sqlx::query_scalar("PRAGMA freelist_count")
        .fetch_one(&mut *conn)
        .await?;
    if free_pages > 0 {
        // SQLite returns one row per vacuum step; consume all rows to finish
        // the bounded statement rather than stopping after its first step.
        sqlx::query("PRAGMA incremental_vacuum(100)")
            .fetch_all(&mut *conn)
            .await?;
    }
    Ok(())
}

/// Close the pool and every connection it still has. `Pool::close` alone can
/// return with a connection left open: a connection dropped just before the
/// call is returned to the pool by a background task, and when that task
/// passed its closed-pool check before the pool was marked closed, it puts
/// the connection back into the idle queue while `close` is in its last wait,
/// after `close` drained that queue for the last time. Such a connection
/// keeps the database file open until the pool itself is dropped, which is
/// enough to refuse an exclusive open that does not wait
/// ([`convert_sqlite_to_incremental`]).
pub async fn close_sqlite_pool(pool: &SqlitePool) {
    loop {
        // A repeated close drains the idle queue again.
        pool.close().await;
        if pool.size() == 0 {
            return;
        }
        tokio::task::yield_now().await;
    }
}

/// One-time offline conversion. The caller must hold the data-root runtime
/// lock. A dedicated exclusive connection prevents other SQLite connections
/// from accessing the file during the full VACUUM, with no busy retry.
pub async fn convert_sqlite_to_incremental(database_url: &str) -> Result<()> {
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(false)
        .busy_timeout(Duration::ZERO);
    let mut conn = SqliteConnection::connect_with(&options).await?;
    sqlx::query("PRAGMA locking_mode = EXCLUSIVE")
        .execute(&mut conn)
        .await?;
    sqlx::query("BEGIN EXCLUSIVE").execute(&mut conn).await?;
    sqlx::query("COMMIT").execute(&mut conn).await?;
    let mode: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
        .fetch_one(&mut conn)
        .await?;
    if mode != 2 {
        sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
            .execute(&mut conn)
            .await?;
        sqlx::query("VACUUM").execute(&mut conn).await?;
    }
    let mode: i64 = sqlx::query_scalar("PRAGMA auto_vacuum")
        .fetch_one(&mut conn)
        .await?;
    if mode != 2 {
        return Err(DbError::Check(
            "database did not enter incremental auto-vacuum mode".to_owned(),
        ));
    }
    conn.close().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::panic::AssertUnwindSafe;

    /// Waits until every marked commit has been announced. A release runs on
    /// a background task, so an earlier wake can otherwise land in a later
    /// assertion's window.
    async fn settled(hooks: &EventHooks) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while hooks.deliveries.load(Ordering::Acquire) != hooks.marks.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("committed appends are announced");
    }
    async fn no_signal(hooks: &EventHooks) {
        settled(hooks).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(1), hooks.notify.notified())
                .await
                .is_err()
        );
    }
    async fn fixture(path: &std::path::Path) -> SqlitePool {
        let pool = create_sqlite_pool(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        sqlx::raw_sql("CREATE TABLE domain_event(id INTEGER PRIMARY KEY, v TEXT UNIQUE); CREATE TABLE other(v TEXT);")
            .execute(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn release_rolls_back_unowned_transaction_and_savepoints() {
        // One connection forces reuse of the abandoned BEGIN, rather than
        // hiding it behind a replacement. Committed data must survive.
        let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
        sqlx::raw_sql("CREATE TABLE other(v TEXT); INSERT INTO other VALUES ('committed');")
            .execute(&pool)
            .await
            .unwrap();
        let mut connection = pool.acquire().await.unwrap();
        // SQLx has acknowledged BEGIN, but no Transaction rollback guard
        // owns it: the same state as cancellation during the handle check.
        SqliteTransactionManager::begin(&mut connection, Some("BEGIN IMMEDIATE".into()))
            .await
            .unwrap();
        sqlx::query("INSERT INTO other VALUES ('uncommitted')")
            .execute(&mut *connection)
            .await
            .unwrap();
        SqliteTransactionManager::begin(&mut connection, None)
            .await
            .unwrap();
        sqlx::query("INSERT INTO other VALUES ('savepoint')")
            .execute(&mut *connection)
            .await
            .unwrap();
        drop(connection);

        let mut reused = pool.acquire().await.unwrap();
        assert_eq!(SqliteTransactionManager::get_transaction_depth(&reused), 0);
        let rows: Vec<String> = sqlx::query_scalar("SELECT v FROM other")
            .fetch_all(&mut *reused)
            .await
            .unwrap();
        assert_eq!(rows, ["committed"]);
        drop(reused);
        let mut tx = begin_immediate(&pool).await.unwrap();
        sqlx::query("INSERT INTO other VALUES ('next owner')")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    /// One poll of `pool.acquire()`: the connection when the pool had it
    /// idle, `None` when the future had to wait and was dropped waiting.
    async fn acquire_or_cancel(pool: &SqlitePool) -> Option<sqlx::pool::PoolConnection<Sqlite>> {
        let mut acquire = std::pin::pin!(pool.acquire());
        match std::future::poll_fn(|cx| std::task::Poll::Ready(acquire.as_mut().poll(cx))).await {
            std::task::Poll::Ready(connection) => Some(connection.unwrap()),
            std::task::Poll::Pending => None,
        }
    }
    async fn raw_handle(connection: &mut SqliteConnection) -> usize {
        connection
            .lock_handle()
            .await
            .unwrap()
            .as_raw_handle()
            .as_ptr() as usize
    }
    async fn idle(pool: &SqlitePool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while pool.num_idle() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the connection is returned to the pool");
    }

    /// A caller that stops waiting for a connection (an aborted task, a lost
    /// `select!` branch, a timeout) must not cost the pool the connection.
    /// With SQLx's ping before acquire the idle connection was out of the
    /// pool while the ping was awaited, and dropping the future there closed
    /// it: a reconnect for a file, a new EMPTY database for `:memory:`.
    #[tokio::test]
    async fn a_cancelled_acquire_does_not_close_the_idle_connection() {
        let dir = tempfile::tempdir().unwrap();
        let file = format!("sqlite://{}", dir.path().join("cancel.sqlite").display());
        for url in ["sqlite::memory:", file.as_str()] {
            let pool = create_sqlite_pool(url).await.unwrap();
            sqlx::raw_sql("CREATE TABLE kept(v TEXT); INSERT INTO kept VALUES ('row');")
                .execute(&pool)
                .await
                .unwrap();
            idle(&pool).await;
            let mut first = pool.acquire().await.unwrap();
            let original = raw_handle(&mut first).await;
            drop(first);
            for round in 0..20 {
                idle(&pool).await;
                let acquired = acquire_or_cancel(&pool).await;
                assert!(
                    acquired.is_some(),
                    "{url} round {round}: an idle connection is handed out without a wait"
                );
            }
            idle(&pool).await;
            assert_eq!(pool.size(), 1, "{url}");
            let mut again = pool.acquire().await.unwrap();
            assert_eq!(
                raw_handle(&mut again).await,
                original,
                "{url}: the same connection"
            );
            let rows: Vec<String> = sqlx::query_scalar("SELECT v FROM kept")
                .fetch_all(&mut *again)
                .await
                .unwrap();
            assert_eq!(rows, ["row"], "{url}");
        }
    }

    /// An in-memory pool whose connection is closed anyway does not carry on
    /// over an empty database: the replacement is refused where it is made.
    #[tokio::test]
    async fn replacing_the_connection_of_an_in_memory_pool_panics_with_the_cause() {
        let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
        sqlx::raw_sql("CREATE TABLE kept(v TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        // Out of the pool for good: the next acquire has to connect.
        let _detached = pool.acquire().await.unwrap().detach();
        let replaced = tokio::spawn(async move { pool.acquire().await.map(drop) }).await;
        let panic = replaced.expect_err("the replacement panics").into_panic();
        let message = panic.downcast_ref::<&str>().copied().unwrap_or_default();
        assert!(message.contains("new, empty database"), "{message}");
    }

    // Ports W1-W5 without wall-clock performance thresholds. The fast-path
    // counter proves ordinary pool users take no additional worker round trip.
    #[tokio::test]
    async fn event_hooks_preserve_commits_isolate_pools_and_skip_unmarked_releases() {
        let dir = tempfile::tempdir().unwrap();
        let pool = fixture(&dir.path().join("events.sqlite")).await;
        let other_pool = fixture(&dir.path().join("other.sqlite")).await;
        let hooks = domain_event_hooks(&pool);
        let other_hooks = domain_event_hooks(&other_pool);
        assert!(!Arc::ptr_eq(&hooks.notify, &other_hooks.notify));
        for _ in 0..100 {
            sqlx::query("SELECT 1").fetch_one(&pool).await.unwrap();
        }
        for _ in 0..20 {
            sqlx::query("INSERT INTO other VALUES ('ordinary write')")
                .execute(&pool)
                .await
                .unwrap();
        }
        assert_eq!(hooks.release_inspections.load(Ordering::Relaxed), 0);

        let mut tx = begin_immediate(&pool).await.unwrap();
        sqlx::query("INSERT INTO domain_event VALUES (1, 'rolled back')")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        no_signal(&hooks).await;
        {
            let mut tx = begin_immediate(&pool).await.unwrap();
            sqlx::query("INSERT INTO domain_event VALUES (1, 'dropped')")
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        no_signal(&hooks).await;
        sqlx::query("INSERT INTO other VALUES ('after rollback')")
            .execute(&pool)
            .await
            .unwrap();
        no_signal(&hooks).await;

        // Enable before commit, and immediately read from another connection
        // on delivery: a durable wake can never precede visibility.
        settled(&hooks).await;
        let notified = hooks.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let mut tx = begin_immediate(&pool).await.unwrap();
        sqlx::query("INSERT INTO domain_event VALUES (1, 'committed')")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut notified)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM domain_event")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        no_signal(&other_hooks).await; // W4: no cross-database wake.

        // W2: a prior commit survives a later rollback on a held connection.
        settled(&hooks).await;
        let notified = hooks.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let mut conn = pool.acquire().await.unwrap();
        sqlx::raw_sql("BEGIN IMMEDIATE; INSERT INTO domain_event VALUES (2, 'held'); COMMIT;")
            .execute(&mut *conn)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM domain_event")
                .fetch_one(&pool)
                .await
                .unwrap(),
            2
        );
        assert!(tokio::time::timeout(Duration::from_secs(1), &mut notified)
            .await
            .is_err());
        sqlx::raw_sql(
            "BEGIN IMMEDIATE; INSERT INTO domain_event VALUES (3, 'rolled back later'); ROLLBACK;",
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        drop(conn);
        tokio::time::timeout(Duration::from_secs(2), &mut notified)
            .await
            .unwrap();

        // W3: savepoint rollback cannot lose an earlier surviving insert.
        settled(&hooks).await;
        let notified = hooks.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let mut tx = begin_immediate(&pool).await.unwrap();
        sqlx::raw_sql(
            "INSERT INTO domain_event VALUES (3, 'survives'); SAVEPOINT s;
            INSERT INTO domain_event VALUES (4, 'nested'); ROLLBACK TO s; RELEASE s;",
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut notified)
            .await
            .unwrap();
        let mut tx = begin_immediate(&pool).await.unwrap();
        {
            let mut child = sqlx::Acquire::begin(&mut *tx).await.unwrap();
            sqlx::query("INSERT INTO domain_event VALUES (4, 'inner commit')")
                .execute(&mut *child)
                .await
                .unwrap();
            child.commit().await.unwrap();
        }
        tx.rollback().await.unwrap();
        no_signal(&hooks).await;

        // A rolled-back savepoint may produce a harmless hint, never an event.
        let mut tx = begin_immediate(&pool).await.unwrap();
        sqlx::raw_sql(
            "SAVEPOINT s; INSERT INTO domain_event VALUES (4, 'gone'); ROLLBACK TO s; RELEASE s;",
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_event")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(count, 3);
        drop(conn);

        // W1/W3 autocommit and failed statement; no stale dirty flag.
        settled(&hooks).await;
        let notified = hooks.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        sqlx::query("INSERT INTO domain_event VALUES (4, 'autocommit')")
            .execute(&pool)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut notified)
            .await
            .unwrap();
        assert!(
            sqlx::query("INSERT INTO domain_event VALUES (4, 'duplicate')")
                .execute(&pool)
                .await
                .is_err()
        );
        no_signal(&hooks).await;

        // N5: poisoning is recovered, not translated by sqlx into ROLLBACK.
        let poisoned = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = hooks.guard();
            panic!("poison test");
        }));
        assert!(poisoned.is_err());
        settled(&hooks).await;
        let notified = hooks.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        sqlx::query("INSERT INTO domain_event VALUES (5, 'after poison')")
            .execute(&pool)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut notified)
            .await
            .unwrap();
        pool.close().await;
        other_pool.close().await;
    }

    /// Why a transaction that writes opens with `begin_immediate`. While
    /// another connection holds the write lock, a transaction that began
    /// with a read is refused the lock at once (`SQLITE_BUSY`, code 5, no
    /// busy wait: waiting could deadlock the two), and an immediate one
    /// waits its turn. `DbError::is_busy` names the refusal for the workers
    /// that wait it out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_after_a_read_is_refused_at_once_and_an_immediate_transaction_waits() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}?mode=rwc", dir.path().join("busy.db").display());
        let pool = create_sqlite_pool(&url).await.unwrap();
        sqlx::raw_sql("CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER NOT NULL); INSERT INTO counter VALUES (1, 0);")
            .execute(&pool)
            .await
            .unwrap();
        // The holder commits when told, or after this at the latest.
        const HELD: Duration = Duration::from_secs(10);
        let hold = |pool: SqlitePool| async move {
            let mut writer = begin_immediate(&pool).await.unwrap();
            sqlx::query("UPDATE counter SET n = n + 1")
                .execute(&mut *writer)
                .await
                .unwrap();
            let (locked, release) = (
                tokio::sync::oneshot::channel::<()>(),
                tokio::sync::oneshot::channel::<()>(),
            );
            let holder = tokio::spawn(async move {
                locked.0.send(()).unwrap();
                let _ = tokio::time::timeout(HELD, release.1).await;
                writer.commit().await.unwrap();
            });
            locked.1.await.unwrap();
            (holder, release.0)
        };

        // Deferred: the read opens the snapshot, the write cannot upgrade it.
        let (holder, release) = hold(pool.clone()).await;
        let mut deferred = pool.begin().await.unwrap();
        let _: i64 = sqlx::query_scalar("SELECT n FROM counter")
            .fetch_one(&mut *deferred)
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let refused = DbError::from(
            sqlx::query("UPDATE counter SET n = n + 1")
                .execute(&mut *deferred)
                .await
                .unwrap_err(),
        );
        assert!(started.elapsed() < HELD, "refused without a busy wait");
        assert!(refused.is_busy() && refused.is_transient(), "{refused}");
        assert!(
            refused.to_string().contains("database is locked"),
            "{refused}"
        );
        deferred.rollback().await.unwrap();
        drop(release);
        holder.await.unwrap();

        // Immediate: the same statements wait for the writer and commit.
        let (holder, release) = hold(pool.clone()).await;
        let waiter = tokio::spawn({
            let pool = pool.clone();
            async move {
                let mut immediate = begin_immediate(&pool).await?;
                let _: i64 = sqlx::query_scalar("SELECT n FROM counter")
                    .fetch_one(&mut *immediate)
                    .await?;
                sqlx::query("UPDATE counter SET n = n + 1")
                    .execute(&mut *immediate)
                    .await?;
                immediate.commit().await
            }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !waiter.is_finished(),
            "waits while the writer holds the lock"
        );
        release.send(()).unwrap();
        holder.await.unwrap();
        waiter.await.unwrap().unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT n FROM counter")
                .fetch_one(&pool)
                .await
                .unwrap(),
            3
        );
        assert!(!DbError::NotFound.is_busy());
    }
}
