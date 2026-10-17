use crate::Result;
use chrono::{DateTime, Utc};
use db::{now_rfc3339, Daemon, DaemonRepo, DaemonStatus, PageRequest, SortBy, SortOrder, SqliteDb};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

pub struct DaemonMonitor {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    check_interval: Duration,
    offline_after: Duration,
    stopped: AtomicBool,
    stop_notify: tokio::sync::Notify,
}

impl DaemonMonitor {
    const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(30);
    const DEFAULT_OFFLINE_AFTER: Duration = Duration::from_secs(180);

    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self::with_intervals(
            db,
            event_bus,
            Self::DEFAULT_CHECK_INTERVAL,
            Self::DEFAULT_OFFLINE_AFTER,
        )
    }

    pub fn with_intervals(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        check_interval: Duration,
        offline_after: Duration,
    ) -> Self {
        Self {
            db,
            event_bus,
            check_interval,
            offline_after,
            stopped: AtomicBool::new(false),
            stop_notify: tokio::sync::Notify::new(),
        }
    }

    pub fn start(
        self: Arc<Self>,
        workers: &crate::worker_runtime::PeriodicWorkers,
    ) -> tokio::task::JoinHandle<()> {
        let stop = Arc::clone(&self);
        workers.worker("daemon-monitor").start_stoppable(
            move || stop.is_stopped(),
            move |worker| {
                let monitor = Arc::clone(&self);
                async move {
                    worker
                        .run(
                            || monitor.is_stopped(),
                            "daemon monitor check failed",
                            || monitor.check_once(),
                            || async {
                                tokio::select! {
                                    _ = tokio::time::sleep(monitor.check_interval) => {}
                                    _ = monitor.stop_notify.notified() => {}
                                }
                            },
                        )
                        .await
                }
            },
        )
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.stop_notify.notify_one();
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    pub async fn check_once(&self) -> Result<u64> {
        let mut offline_count = 0;
        for daemon in self.list_daemons().await? {
            if daemon.status == DaemonStatus::Offline {
                crate::recovery::disconnect_daemon_placements(
                    &self.db,
                    &self.event_bus,
                    &daemon.id,
                )
                .await?;
            }
            if daemon.status != DaemonStatus::Online
                || !daemon_is_stale(&daemon, self.offline_after)
            {
                continue;
            }

            let updated_at = now_rfc3339();
            let daemon = DaemonRepo::mark_offline(&*self.db, &daemon.id, &updated_at).await?;
            crate::recovery::disconnect_daemon_placements(&self.db, &self.event_bus, &daemon.id)
                .await?;
            self.event_bus.publish(ForgeEvent {
                event_type: "daemon.offline".to_owned(),
                entity_id: daemon.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::DaemonOffline {},
            });
            self.event_bus.publish(ForgeEvent {
                event_type: "reconciliation.event".to_owned(),
                entity_id: daemon.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::ReconciliationEvent {
                    task_id: None,
                    execution_id: None,
                    reason: "daemon offline".to_owned(),
                },
            });
            offline_count += 1;
        }
        Ok(offline_count)
    }

    async fn list_daemons(&self) -> Result<Vec<Daemon>> {
        let mut daemons = Vec::new();
        let mut cursor = None;
        loop {
            let page = DaemonRepo::list(
                &*self.db,
                PageRequest {
                    cursor,
                    limit: 500,
                    include_total: false,
                    sort_by: SortBy::CreatedAt,
                    sort_order: SortOrder::Asc,
                },
            )
            .await?;
            daemons.extend(page.items);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(daemons)
    }
}

fn daemon_is_stale(daemon: &Daemon, offline_after: Duration) -> bool {
    let Some(last_report_at) = daemon.last_report_at.as_deref() else {
        return false;
    };
    let Ok(last_report_at) = DateTime::parse_from_rfc3339(last_report_at) else {
        return false;
    };
    let elapsed = Utc::now().signed_duration_since(last_report_at.with_timezone(&Utc));
    elapsed
        .to_std()
        .map(|elapsed| elapsed > offline_after)
        .unwrap_or(false)
}
