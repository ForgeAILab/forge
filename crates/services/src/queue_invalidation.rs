//! The SSE projector of the merge queue: `integration.updated`.
//!
//! The queue worker writes queue and attempt rows and nothing else; it has no
//! event bus. Readers still have to learn that a head changed phase or that
//! the line moved, so this projector, in the API process, compares two reads
//! of the queues and publishes at most ONE bus event per queue per read,
//! whatever happened in between. It is read-only (one SELECT a read, no
//! transaction), it publishes cache invalidations only (no ledger row, no
//! Attention input, ids only), and it compares what a reader of the public
//! summary sees, not row revisions: a lease renewal, a receipt or a fence
//! written on an attempt changes nothing visible and publishes nothing.
use crate::Result;
use db::{IntegrationWatchRow, SqliteDb};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent, INTEGRATION_UPDATED_EVENT};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};

/// The longest a change waits for its event when nothing wakes the projector.
const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// The shortest time between two reads, however often it is woken: a head
/// that runs through its phases inside this window is one event.
const MIN_INTERVAL: Duration = Duration::from_millis(250);

/// What a reader sees of one member: the attempt columns the summary is
/// projected from, and its place in line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MemberView {
    task_id: String,
    project_id: String,
    view: String,
    is_head: bool,
    /// Members ahead in line; `None` when it is not in line.
    ahead: Option<usize>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueView {
    repo_id: String,
    view: String,
    members: BTreeMap<String, MemberView>,
}

/// What the projector saw last. `None` until its first read: a process that
/// starts has nothing to invalidate (every client fetches when it connects),
/// so a restart replays no history.
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
                view: row.queue_view.clone(),
                members: BTreeMap::new(),
            });
        let (Some(attempt_id), Some(task_id), Some(project_id), Some(view)) = (
            row.attempt_id,
            row.task_ref,
            row.project_ref,
            row.member_view,
        ) else {
            continue;
        };
        let ahead = if row.is_head {
            Some(0)
        } else if row.in_line {
            let seen = waiting.entry(row.queue_id.clone()).or_default();
            let ahead = *seen + usize::from(row.has_head);
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
                project_id,
                view,
                is_head: row.is_head,
                ahead,
            },
        );
    }
    queues
}

/// Read the queues once and publish `integration.updated` for each queue in
/// which something a reader sees changed since the last read: one event per
/// queue. `task_id` is the one Task whose summary changed; it is `null` when
/// more than one did, or when the queue's own state or head did (a reader
/// then refreshes what it shows of that queue). `project_ids` are the
/// Projects of the members that changed. Returns the number of events.
pub async fn project_integration_changes(
    db: &SqliteDb,
    watch: &mut IntegrationWatch,
    bus: &EventBus,
) -> Result<usize> {
    let now = views(db.integration_watch_rows().await?);
    let Some(before) = watch.queues.replace(now.clone()) else {
        return Ok(0);
    };
    let none = BTreeMap::new();
    let mut published = 0;
    let queue_ids = now.keys().chain(before.keys()).collect::<BTreeSet<_>>();
    for queue_id in queue_ids {
        let (old, new) = (before.get(queue_id), now.get(queue_id));
        if old == new {
            continue;
        }
        let queue_changed = old.map(|queue| &queue.view) != new.map(|queue| &queue.view);
        let old_members = old.map_or(&none, |queue| &queue.members);
        let new_members = new.map_or(&none, |queue| &queue.members);
        // Every member whose view or place changed, that joined, or that left.
        let changed = new_members
            .iter()
            .filter(|(id, member)| old_members.get(*id) != Some(member))
            .chain(
                old_members
                    .iter()
                    .filter(|(id, _)| !new_members.contains_key(*id)),
            )
            .map(|(_, member)| member)
            .collect::<Vec<_>>();
        let tasks = changed
            .iter()
            .map(|member| member.task_id.as_str())
            .collect::<BTreeSet<_>>();
        // A queue whose own state or head changed concerns every member.
        let projects = if queue_changed {
            old_members.values().chain(new_members.values()).collect()
        } else {
            changed
        }
        .into_iter()
        .map(|member| member.project_id.clone())
        .collect::<BTreeSet<_>>();
        let Some(repo_id) = new.or(old).map(|queue| queue.repo_id.clone()) else {
            continue;
        };
        bus.publish(ForgeEvent {
            event_type: INTEGRATION_UPDATED_EVENT.to_owned(),
            entity_id: queue_id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::IntegrationUpdated {
                repo_id,
                queue_id: queue_id.clone(),
                task_id: match (queue_changed, tasks.len()) {
                    (false, 1) => tasks.first().map(|task| (*task).to_owned()),
                    _ => None,
                },
                project_ids: projects.into_iter().collect(),
            },
        });
        published += 1;
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
    /// The `(queue_id, task_id)` of every published event, sorted. Every
    /// event names the one Project of the fixture and nothing but ids.
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
                        project_ids,
                    } = event.context
                    else {
                        panic!("integration.updated carries its own context");
                    };
                    assert_eq!(repo_id, "r");
                    assert_eq!(project_ids, ["p"]);
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
    async fn read(fixture: &Fixture, watch: &mut IntegrationWatch) -> usize {
        project_integration_changes(&fixture.db, watch, &fixture.bus)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn an_attempt_moving_phases_publishes_its_task_and_an_idle_queue_nothing() {
        let fixture = fixture().await;
        let mut receiver = fixture.bus.subscribe();
        let mut watch = IntegrationWatch::default();
        // The first read has nothing to compare with.
        assert_eq!(read(&fixture, &mut watch).await, 0);
        assert!(drain(&mut receiver).is_empty());

        // The worker takes the head: one event for the queue, whose head
        // changed, not one for the queue and one for each member.
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
        assert_eq!(read(&fixture, &mut watch).await, 1);
        let queue = fixture.queue.clone();
        assert_eq!(drain(&mut receiver), [(queue.clone(), None)]);

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
            assert_eq!(read(&fixture, &mut watch).await, 1, "{state}");
            assert_eq!(
                drain(&mut receiver),
                [(queue.clone(), Some("t1".to_owned()))],
                "{state}"
            );
        }

        // Idle: a lease renewal rewrites the queue row, a receipt or a fence
        // rewrites the attempt row, and neither is anything a reader sees.
        // Nothing is published, however often the projector reads.
        for _ in 0..3 {
            sql(
                &fixture,
                "UPDATE integration_queue SET revision=revision+1,lease_owner='worker',lease_until=?1,updated_at=?1",
                &db::now_rfc3339(),
            )
            .await;
            sql(
                &fixture,
                "UPDATE integration_attempt SET revision=revision+1,updated_at=?1,observations_json='[{\"seen\":true}]'",
                &db::now_rfc3339(),
            )
            .await;
            assert_eq!(read(&fixture, &mut watch).await, 0);
        }
        assert!(drain(&mut receiver).is_empty());

        // A process that starts over a busy queue replays nothing.
        assert_eq!(read(&fixture, &mut IntegrationWatch::default()).await, 0);
        assert!(drain(&mut receiver).is_empty());
    }

    #[tokio::test]
    async fn a_burst_between_two_reads_is_one_event_for_its_queue() {
        let fixture = fixture().await;
        let mut watch = IntegrationWatch::default();
        read(&fixture, &mut watch).await;
        let mut receiver = fixture.bus.subscribe();
        // The head is claimed and runs through six phases, and two members
        // behind it change too, before the projector reads again.
        sql(
            &fixture,
            "UPDATE integration_queue SET head_attempt_id=?",
            &fixture.attempts[0],
        )
        .await;
        for state in [
            "validating",
            "rebasing",
            "checking",
            "awaiting_task_step",
            "ready_ff",
            "ff_inflight",
        ] {
            sql(
                &fixture,
                &format!(
                    "UPDATE integration_attempt SET state='{state}',revision=revision+1 WHERE id=?"
                ),
                &fixture.attempts[0],
            )
            .await;
        }
        assert_eq!(read(&fixture, &mut watch).await, 1);
        assert_eq!(drain(&mut receiver), [(fixture.queue.clone(), None)]);
        assert_eq!(read(&fixture, &mut watch).await, 0);
    }

    #[test]
    fn the_frame_is_flat_and_carries_ids_only() {
        let frame = |task_id: Option<&str>| {
            serde_json::to_value(ForgeEvent {
                event_type: INTEGRATION_UPDATED_EVENT.to_owned(),
                entity_id: "q".to_owned(),
                timestamp: "2026-10-01T00:00:00Z".to_owned(),
                context: EventContext::IntegrationUpdated {
                    repo_id: "r".to_owned(),
                    queue_id: "q".to_owned(),
                    task_id: task_id.map(str::to_owned),
                    project_ids: vec!["p".to_owned()],
                },
            })
            .unwrap()
        };
        assert_eq!(
            frame(Some("t")),
            serde_json::json!({"event_type":"integration.updated","entity_id":"q","timestamp":"2026-10-01T00:00:00Z","repo_id":"r","queue_id":"q","task_id":"t","project_ids":["p"]})
        );
        assert_eq!(frame(None)["task_id"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn the_line_moving_without_a_task_step_publishes_its_queue_once() {
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
        read(&fixture, &mut watch).await;
        let mut receiver = fixture.bus.subscribe();

        // The head finishes: its attempt leaves the queue and the slot is
        // free. No Task step wrote t2 or t3, and both are one place nearer:
        // one event, for the queue.
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
        let queue = fixture.queue.clone();
        assert_eq!(read(&fixture, &mut watch).await, 1);
        assert_eq!(drain(&mut receiver), [(queue.clone(), None)]);

        // A waiter that is cancelled moves the one behind it: two members
        // changed, still one event.
        sql(
            &fixture,
            "UPDATE integration_attempt SET state='cancelled',current=0,completed_at=updated_at,revision=revision+1 WHERE id=?",
            &fixture.attempts[1],
        )
        .await;
        assert_eq!(read(&fixture, &mut watch).await, 1);
        assert_eq!(drain(&mut receiver), [(queue.clone(), None)]);
        // The last member alone changes: the event names its Task.
        sql(
            &fixture,
            "UPDATE integration_attempt SET state='parked',failure_message='target_dirty: local changes',revision=revision+1 WHERE id=?",
            &fixture.attempts[2],
        )
        .await;
        assert_eq!(read(&fixture, &mut watch).await, 1);
        assert_eq!(drain(&mut receiver), [(queue, Some("t3".to_owned()))]);
        assert_eq!(read(&fixture, &mut watch).await, 0);
        assert!(drain(&mut receiver).is_empty());
    }

    #[tokio::test]
    async fn a_cancel_request_on_a_waiter_moves_the_members_behind_it() {
        let fixture = fixture().await;
        let mut watch = IntegrationWatch::default();
        read(&fixture, &mut watch).await;
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
        assert_eq!(read(&fixture, &mut watch).await, 1);
        assert_eq!(drain(&mut receiver), [(fixture.queue.clone(), None)]);
        // The summary agrees: the second waiter is next.
        let rows = db::integration_members_of_tasks(fixture.db.pool(), &["t2", "t3"])
            .await
            .unwrap();
        let ahead = |task: &str| rows.iter().find(|row| row.task_ref == task).unwrap().ahead;
        assert_eq!((ahead("t2"), ahead("t3")), (Some(0), Some(1)));
    }
}
