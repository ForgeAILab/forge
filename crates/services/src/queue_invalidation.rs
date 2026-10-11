//! The SSE projector of the merge queue: `integration.updated`.
//!
//! The queue worker writes queue and attempt rows and nothing else; it has no
//! event bus. Readers still have to learn that a head changed phase or that
//! the line moved, so this projector, in the API process, compares two reads
//! of the queues and publishes one bus event per queue or member that
//! changed. It is read-only, it publishes cache invalidations only (no ledger
//! row, no Attention input), and an idle queue publishes nothing: a lease
//! renewal changes none of the columns it reads.
use crate::Result;
use db::{IntegrationWatchRow, SqliteDb};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent, INTEGRATION_UPDATED_EVENT};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};

/// The longest a change waits for its event when nothing wakes the projector.
const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// The shortest time between two reads, however often it is woken.
const MIN_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, PartialEq, Eq)]
struct MemberView {
    task_id: String,
    state: String,
    revision: i64,
    /// Members ahead in line; `None` when it is not in line.
    ahead: Option<usize>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueView {
    repo_id: String,
    state: String,
    head_attempt_id: Option<String>,
    members: BTreeMap<String, MemberView>,
}

/// What the projector saw last. `None` until its first read: a process that
/// starts has nothing to invalidate, every client fetches when it connects.
#[derive(Debug, Default)]
pub struct IntegrationWatch {
    queues: Option<HashMap<String, QueueView>>,
}

fn views(rows: Vec<IntegrationWatchRow>) -> HashMap<String, QueueView> {
    let mut queues = HashMap::<String, QueueView>::new();
    let mut waiting = HashMap::<String, usize>::new();
    // Rows arrive in queue order, so the count of waiters seen so far is the
    // number ahead of the next one.
    for row in rows {
        let queue = queues
            .entry(row.queue_id.clone())
            .or_insert_with(|| QueueView {
                repo_id: row.repo_id.clone(),
                state: row.queue_state.clone(),
                head_attempt_id: row.head_attempt_id.clone(),
                members: BTreeMap::new(),
            });
        let (Some(attempt_id), Some(task_id), Some(state), Some(revision)) = (
            row.attempt_id,
            row.task_ref,
            row.attempt_state,
            row.attempt_revision,
        ) else {
            continue;
        };
        let is_head = queue.head_attempt_id.as_deref() == Some(attempt_id.as_str());
        let has_head = usize::from(queue.head_attempt_id.is_some());
        let ahead = if is_head {
            Some(0)
        } else if matches!(state.as_str(), "queued" | "path_wait") {
            let seen = waiting.entry(row.queue_id.clone()).or_default();
            let ahead = *seen + has_head;
            // As in the summary's position: a waiter with a cancel request
            // is never claimed, so it is ahead of nobody.
            if !row.cancel_requested {
                *seen += 1;
            }
            Some(ahead)
        } else {
            None
        };
        queue.members.insert(
            attempt_id,
            MemberView {
                task_id,
                state,
                revision,
                ahead,
            },
        );
    }
    queues
}

/// Read the queues once and publish `integration.updated` for what changed
/// since the last read: `task_id` set for each member that changed state,
/// position or attempt, or left; `task_id` null for a queue whose own state
/// or head changed. Returns the number of events published.
pub async fn project_integration_changes(
    db: &SqliteDb,
    watch: &mut IntegrationWatch,
    bus: &EventBus,
) -> Result<usize> {
    let now = views(db.integration_watch_rows().await?);
    let Some(before) = watch.queues.replace(now.clone()) else {
        return Ok(0);
    };
    let mut published = 0;
    let mut publish = |repo_id: &str, queue_id: &str, task_id: Option<&str>| {
        bus.publish(ForgeEvent {
            event_type: INTEGRATION_UPDATED_EVENT.to_owned(),
            entity_id: queue_id.to_owned(),
            timestamp: event_timestamp(),
            context: EventContext::IntegrationUpdated {
                repo_id: repo_id.to_owned(),
                queue_id: queue_id.to_owned(),
                task_id: task_id.map(str::to_owned),
            },
        });
        published += 1;
    };
    for (queue_id, queue) in &now {
        let old = before.get(queue_id);
        if old.is_none_or(|old| {
            old.state != queue.state || old.head_attempt_id != queue.head_attempt_id
        }) {
            publish(&queue.repo_id, queue_id, None);
        }
        for (attempt_id, member) in &queue.members {
            if old.and_then(|old| old.members.get(attempt_id)) != Some(member) {
                publish(&queue.repo_id, queue_id, Some(&member.task_id));
            }
        }
        // A member that left: its Task no longer has an attempt in progress
        // (or has a successor, published above under its own id).
        for (attempt_id, member) in old.map(|old| &old.members).into_iter().flatten() {
            if !queue.members.contains_key(attempt_id)
                && !queue
                    .members
                    .values()
                    .any(|current| current.task_id == member.task_id)
            {
                publish(&queue.repo_id, queue_id, Some(&member.task_id));
            }
        }
    }
    // A queue that is gone (its repository was deleted).
    for (queue_id, queue) in &before {
        if !now.contains_key(queue_id) {
            publish(&queue.repo_id, queue_id, None);
        }
    }
    Ok(published)
}

pub struct IntegrationUpdateProjector {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
}

impl IntegrationUpdateProjector {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self { db, event_bus }
    }

    /// Reads at most every `MIN_INTERVAL` and at least every `POLL_INTERVAL`.
    /// The integration Task steps already wake the domain-event waiters when
    /// they settle, so a step's change is published at once; a change with no
    /// Task step (the line moved, a head changed phase) is published by the
    /// poll.
    pub fn start(
        self: Arc<Self>,
        workers: &crate::worker_runtime::PeriodicWorkers,
        shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        workers.worker("integration-read-projection").start(
            shutdown,
            || false,
            move |worker, mut shutdown| {
                let projector = Arc::clone(&self);
                async move {
                    let mut watch = IntegrationWatch::default();
                    let woken = projector.db.domain_event_notify();
                    loop {
                        if *shutdown.borrow_and_update() {
                            return Ok(());
                        }
                        // Registered before the read: a step that settles
                        // while this pass reads or rests still wakes the next.
                        let notified = woken.notified();
                        tokio::pin!(notified);
                        notified.as_mut().enable();
                        let _ = worker
                            .tick(project_integration_changes(
                                &projector.db,
                                &mut watch,
                                &projector.event_bus,
                            ))
                            .await;
                        tokio::select! {
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    return Ok(());
                                }
                            }
                            _ = tokio::time::sleep(MIN_INTERVAL) => {}
                        }
                        tokio::select! {
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    return Ok(());
                                }
                            }
                            _ = &mut notified => {}
                            _ = tokio::time::sleep(POLL_INTERVAL - MIN_INTERVAL) => {}
                        }
                    }
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::IntegrationQueueRepo;
    use tokio::sync::broadcast::error::TryRecvError;

    struct Fixture {
        db: Arc<SqliteDb>,
        bus: EventBus,
        queue: String,
        attempts: Vec<String>,
    }
    async fn fixture() -> Fixture {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = Arc::new(SqliteDb::new(pool));
        let now = db::now_rfc3339();
        sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','p',?,?)")
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('r','p','r','main',?,?)")
            .bind(&now)
            .bind(&now)
            .execute(db.pool())
            .await
            .unwrap();
        let queue = db
            .create_or_get_integration_queue("r", "main")
            .await
            .unwrap();
        let mut attempts = Vec::new();
        for task in ["t1", "t2", "t3"] {
            sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES(?,'p',?,'merging',?,?)")
                .bind(task)
                .bind(task)
                .bind(&now)
                .bind(&now)
                .execute(db.pool())
                .await
                .unwrap();
            attempts.push(
                db.admit_integration_attempt(db::IntegrationAttempt::new(
                    Some(queue.id.clone()),
                    task.into(),
                    "p".into(),
                    format!("admission-{task}"),
                    "merging".into(),
                    0,
                    1,
                ))
                .await
                .unwrap()
                .id,
            );
        }
        Fixture {
            db,
            bus: EventBus::new(64),
            queue: queue.id,
            attempts,
        }
    }
    async fn sql(fixture: &Fixture, statement: &str, bind: &str) {
        sqlx::query(statement)
            .bind(bind)
            .execute(fixture.db.pool())
            .await
            .unwrap();
    }
    /// The `(queue_id, task_id)` of every published event, sorted.
    fn drain(
        receiver: &mut tokio::sync::broadcast::Receiver<ForgeEvent>,
    ) -> Vec<(String, Option<String>)> {
        let mut events = Vec::new();
        loop {
            match receiver.try_recv() {
                Ok(event) => {
                    assert_eq!(event.event_type, INTEGRATION_UPDATED_EVENT);
                    let EventContext::IntegrationUpdated {
                        repo_id,
                        queue_id,
                        task_id,
                    } = event.context
                    else {
                        panic!("integration.updated carries its own context");
                    };
                    assert_eq!(repo_id, "r");
                    assert_eq!(event.entity_id, queue_id);
                    events.push((queue_id, task_id));
                }
                Err(TryRecvError::Empty) => break,
                Err(error) => panic!("{error}"),
            }
        }
        events.sort();
        events
    }

    #[tokio::test]
    async fn an_attempt_moving_phases_publishes_its_task_and_an_idle_queue_nothing() {
        let fixture = fixture().await;
        let mut receiver = fixture.bus.subscribe();
        let mut watch = IntegrationWatch::default();
        // The first read has nothing to compare with.
        assert_eq!(
            project_integration_changes(&fixture.db, &mut watch, &fixture.bus)
                .await
                .unwrap(),
            0
        );
        assert!(drain(&mut receiver).is_empty());

        // The worker takes the head: the queue and the head are published.
        // The waiters are not: each still has as many ahead as before.
        sql(
            &fixture,
            "UPDATE integration_queue SET head_attempt_id=?,revision=revision+1",
            &fixture.attempts[0],
        )
        .await;
        sql(
            &fixture,
            "UPDATE integration_attempt SET state='validating',revision=revision+1 WHERE id=?",
            &fixture.attempts[0],
        )
        .await;
        project_integration_changes(&fixture.db, &mut watch, &fixture.bus)
            .await
            .unwrap();
        let queue = fixture.queue.clone();
        assert_eq!(
            drain(&mut receiver),
            [
                (queue.clone(), None),
                (queue.clone(), Some("t1".to_owned())),
            ]
        );

        // Each phase of the head publishes the head's Task and nothing else.
        for state in ["rebasing", "checking", "awaiting_task_step", "ready_ff"] {
            sql(
                &fixture,
                &format!(
                    "UPDATE integration_attempt SET state='{state}',revision=revision+1 WHERE id=?"
                ),
                &fixture.attempts[0],
            )
            .await;
            let published = project_integration_changes(&fixture.db, &mut watch, &fixture.bus)
                .await
                .unwrap();
            assert_eq!(published, 1, "{state}");
            assert_eq!(
                drain(&mut receiver),
                [(queue.clone(), Some("t1".to_owned()))],
                "{state}"
            );
        }

        // Idle: a lease renewal rewrites the queue row and nothing a reader
        // sees. Nothing is published, however often the projector reads.
        for _ in 0..3 {
            sql(
                &fixture,
                "UPDATE integration_queue SET revision=revision+1,updated_at=?",
                &db::now_rfc3339(),
            )
            .await;
            let published = project_integration_changes(&fixture.db, &mut watch, &fixture.bus)
                .await
                .unwrap();
            assert_eq!(published, 0);
        }
        assert!(drain(&mut receiver).is_empty());
    }

    #[test]
    fn the_frame_is_flat_and_names_repo_queue_and_task() {
        let frame = |task_id: Option<&str>| {
            serde_json::to_value(ForgeEvent {
                event_type: INTEGRATION_UPDATED_EVENT.to_owned(),
                entity_id: "q".to_owned(),
                timestamp: "2026-10-01T00:00:00Z".to_owned(),
                context: EventContext::IntegrationUpdated {
                    repo_id: "r".to_owned(),
                    queue_id: "q".to_owned(),
                    task_id: task_id.map(str::to_owned),
                },
            })
            .unwrap()
        };
        assert_eq!(
            frame(Some("t")),
            serde_json::json!({"event_type":"integration.updated","entity_id":"q","timestamp":"2026-10-01T00:00:00Z","repo_id":"r","queue_id":"q","task_id":"t"})
        );
        assert_eq!(frame(None)["task_id"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn the_line_moving_without_a_task_step_publishes_every_waiter() {
        let fixture = fixture().await;
        let mut watch = IntegrationWatch::default();
        sql(
            &fixture,
            "UPDATE integration_queue SET head_attempt_id=?",
            &fixture.attempts[0],
        )
        .await;
        sql(
            &fixture,
            "UPDATE integration_attempt SET state='ff_inflight',revision=revision+1 WHERE id=?",
            &fixture.attempts[0],
        )
        .await;
        project_integration_changes(&fixture.db, &mut watch, &fixture.bus)
            .await
            .unwrap();
        let mut receiver = fixture.bus.subscribe();

        // The head finishes: its attempt leaves the queue and the slot is
        // free. No Task step wrote t2 or t3, and both are one place nearer.
        sqlx::query("UPDATE integration_queue SET head_attempt_id=NULL WHERE id=?")
            .bind(&fixture.queue)
            .execute(fixture.db.pool())
            .await
            .unwrap();
        sql(
            &fixture,
            "UPDATE integration_attempt SET state='completed',current=0,completed_at=updated_at,revision=revision+1 WHERE id=?",
            &fixture.attempts[0],
        )
        .await;
        let published = project_integration_changes(&fixture.db, &mut watch, &fixture.bus)
            .await
            .unwrap();
        let queue = fixture.queue.clone();
        assert_eq!(
            drain(&mut receiver),
            [
                (queue.clone(), None),
                (queue.clone(), Some("t1".to_owned())),
                (queue.clone(), Some("t2".to_owned())),
                (queue.clone(), Some("t3".to_owned())),
            ]
        );
        assert_eq!(published, 4);

        // A waiter that is cancelled moves only the one behind it.
        sql(
            &fixture,
            "UPDATE integration_attempt SET state='cancelled',current=0,completed_at=updated_at,revision=revision+1 WHERE id=?",
            &fixture.attempts[1],
        )
        .await;
        project_integration_changes(&fixture.db, &mut watch, &fixture.bus)
            .await
            .unwrap();
        assert_eq!(
            drain(&mut receiver),
            [
                (queue.clone(), Some("t2".to_owned())),
                (queue.clone(), Some("t3".to_owned())),
            ]
        );
        assert_eq!(
            project_integration_changes(&fixture.db, &mut watch, &fixture.bus)
                .await
                .unwrap(),
            0
        );
        assert!(drain(&mut receiver).is_empty());
    }

    #[tokio::test]
    async fn a_cancel_request_on_a_waiter_moves_the_members_behind_it() {
        let fixture = fixture().await;
        let mut watch = IntegrationWatch::default();
        project_integration_changes(&fixture.db, &mut watch, &fixture.bus)
            .await
            .unwrap();
        let mut receiver = fixture.bus.subscribe();
        // The first waiter is being cancelled: it will never be claimed, so
        // the two behind it each have one fewer ahead, before any of them
        // (or the cancelled attempt's state) is written.
        sql(
            &fixture,
            "UPDATE integration_attempt SET cancel_requested_at=updated_at,revision=revision+1 WHERE id=?",
            &fixture.attempts[0],
        )
        .await;
        project_integration_changes(&fixture.db, &mut watch, &fixture.bus)
            .await
            .unwrap();
        let queue = fixture.queue.clone();
        assert_eq!(
            drain(&mut receiver),
            [
                (queue.clone(), Some("t1".to_owned())),
                (queue.clone(), Some("t2".to_owned())),
                (queue.clone(), Some("t3".to_owned())),
            ]
        );
        // The summary agrees: the second waiter is next.
        let rows = db::integration_members_of_tasks(fixture.db.pool(), &["t2", "t3"])
            .await
            .unwrap();
        let ahead = |task: &str| rows.iter().find(|row| row.task_ref == task).unwrap().ahead;
        assert_eq!((ahead("t2"), ahead("t3")), (Some(0), Some(1)));
    }
}
