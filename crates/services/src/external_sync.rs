use crate::{IntegrationService, TaskService};
use chrono::{DateTime, Utc};
use db::{now_rfc3339, IntegrationRepo, ProjectIntegration, SqliteDb};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

pub struct ExternalSyncService {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    task_service: Arc<TaskService>,
    tick_interval: Duration,
    stopped: AtomicBool,
    stop_notify: tokio::sync::Notify,
}

impl ExternalSyncService {
    const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(60);

    pub fn new(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        task_service: Arc<TaskService>,
    ) -> Self {
        Self {
            db,
            event_bus,
            task_service,
            tick_interval: Self::DEFAULT_TICK_INTERVAL,
            stopped: AtomicBool::new(false),
            stop_notify: tokio::sync::Notify::new(),
        }
    }

    pub fn start(
        self: Arc<Self>,
        workers: &crate::worker_runtime::PeriodicWorkers,
    ) -> tokio::task::JoinHandle<()> {
        let stop = Arc::clone(&self);
        workers
            .worker("external-sync")
            .with_stall_budget(Duration::from_secs(3600))
            .start_stoppable(
                move || stop.is_stopped(),
                move |worker| {
                    let monitor = Arc::clone(&self);
                    async move {
                        worker
                            .run(
                                || monitor.is_stopped(),
                                "periodic worker tick failed",
                                || async {
                                    monitor.tick().await;
                                    Ok(())
                                },
                                || async {
                                    tokio::select! {
                                        _ = tokio::time::sleep(monitor.tick_interval) => {}
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

    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    async fn tick(&self) {
        let integrations = match IntegrationRepo::list_enabled(&*self.db).await {
            Ok(integrations) => integrations,
            Err(error) => {
                tracing::warn!(%error, "external sync list enabled integrations failed");
                return;
            }
        };
        let service = IntegrationService::new(
            Arc::clone(&self.db),
            Arc::clone(&self.event_bus),
            Arc::clone(&self.task_service),
        );

        for integration in integrations {
            if !integration_due(&integration) {
                continue;
            }

            match service.sync_integration(&integration).await {
                Ok(result) => {
                    let now = now_rfc3339();
                    match IntegrationRepo::update_last_polled_at(
                        &*self.db,
                        &integration.id,
                        &now,
                        &now,
                    )
                    .await
                    {
                        Ok(()) => {
                            self.event_bus.publish(ForgeEvent {
                                event_type: "external_sync.completed".to_owned(),
                                entity_id: integration.id.clone(),
                                timestamp: event_timestamp(),
                                context: EventContext::ExternalSyncCompleted {
                                    integration_id: integration.id,
                                    imported_count: result.imported,
                                    skipped_count: result.skipped,
                                },
                            });
                        }
                        Err(error) => self.publish_failed(&integration.id, error.to_string()),
                    };
                }
                Err(error) => self.publish_failed(&integration.id, error.to_string()),
            }
        }
    }

    fn publish_failed(&self, integration_id: &str, error: String) {
        self.event_bus.publish(ForgeEvent {
            event_type: "external_sync.failed".to_owned(),
            entity_id: integration_id.to_owned(),
            timestamp: event_timestamp(),
            context: EventContext::ExternalSyncFailed {
                integration_id: integration_id.to_owned(),
                error,
            },
        });
    }
}

fn integration_due(integration: &ProjectIntegration) -> bool {
    let Some(last_polled_at) = integration.last_polled_at.as_deref() else {
        return true;
    };
    let Ok(last_polled_at) = DateTime::parse_from_rfc3339(last_polled_at) else {
        return true;
    };
    let Ok(interval) = u64::try_from(integration.poll_interval_secs).map(Duration::from_secs)
    else {
        return true;
    };
    let elapsed = Utc::now().signed_duration_since(last_polled_at.with_timezone(&Utc));
    elapsed
        .to_std()
        .map(|elapsed| elapsed >= interval)
        .unwrap_or(true)
}
