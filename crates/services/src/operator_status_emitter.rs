use std::sync::Arc;
use std::time::Duration;

use events::{
    event_timestamp, EventBus, EventContext, ForgeEvent, OPERATIONS_STATUS_CHANGED_EVENT,
};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

const ACTIVITY_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

pub struct OperatorStatusEmitter {
    event_bus: Arc<EventBus>,
}

impl OperatorStatusEmitter {
    pub fn new(event_bus: Arc<EventBus>) -> Self {
        Self { event_bus }
    }

    pub fn start(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        let task_event_bus = Arc::clone(&self.event_bus);
        tokio::spawn(async move {
            if *shutdown.borrow_and_update() {
                return;
            }

            let mut rx = task_event_bus.subscribe();
            let mut dirty = false;
            let mut activity_dirty = false;
            let mut last_publish = tokio::time::Instant::now();
            let mut last_event_type: Option<String> = None;
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;

            loop {
                tokio::select! {
                    event = rx.recv() => {
                        match event {
                            Ok(event) => {
                                let event_type = event.event_type;
                                if is_status_affecting_event(&event_type) {
                                    last_event_type = Some(event_type);
                                    dirty = true;
                                } else if event_type == "execution.log" {
                                    activity_dirty = true;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                // Lost notifications require a reconciliation, not silence.
                                dirty = true;
                                last_event_type = Some("events.resync_required".to_owned());
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    _ = interval.tick() => {
                        if refresh_due(dirty, activity_dirty, last_publish.elapsed()) {
                            let trigger = if dirty {
                                last_event_type.take().unwrap_or_else(|| "unknown".to_owned())
                            } else {
                                "execution.log".to_owned()
                            };
                            dirty = false;
                            activity_dirty = false;
                            last_publish = tokio::time::Instant::now();
                            task_event_bus.publish(ForgeEvent {
                                event_type: OPERATIONS_STATUS_CHANGED_EVENT.to_string(),
                                entity_id: "operations".to_string(),
                                timestamp: event_timestamp(),
                                context: EventContext::OperationsStatusChanged { trigger },
                            });
                        }
                    }
                    result = shutdown.changed() => {
                        if result.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
        })
    }
}

// Streaming text is activity, not a lifecycle transition. It still refreshes
// the Operations read model at a bounded cadence; terminal/capacity changes
// keep the existing 500ms coalescing latency.
fn refresh_due(dirty: bool, activity_dirty: bool, elapsed: Duration) -> bool {
    dirty || (activity_dirty && elapsed >= ACTIVITY_REFRESH_INTERVAL)
}

fn is_status_affecting_event(event_type: &str) -> bool {
    if event_type == OPERATIONS_STATUS_CHANGED_EVENT || event_type == "execution.log" {
        return false;
    }

    [
        "task.",
        "execution.",
        "daemon.",
        "workspace.",
        "review.",
        "merge.",
        "cleanup.",
    ]
    .iter()
    .any(|prefix| event_type.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_affecting_event_matches_expected_types() {
        assert!(is_status_affecting_event("task.status_changed"));
        assert!(is_status_affecting_event("task.moved"));
        assert!(is_status_affecting_event("execution.started"));
        assert!(is_status_affecting_event("daemon.registered"));
        assert!(is_status_affecting_event("workspace.created"));
        assert!(is_status_affecting_event("review.decided"));
        assert!(is_status_affecting_event("merge.started"));
        assert!(!is_status_affecting_event(OPERATIONS_STATUS_CHANGED_EVENT));
        assert!(!is_status_affecting_event("unknown.event"));
        assert!(!is_status_affecting_event("execution.log"));
        assert!(is_status_affecting_event("execution.completed"));
    }

    #[test]
    fn log_activity_is_bounded_without_delaying_lifecycle_changes() {
        assert!(!refresh_due(false, false, Duration::from_secs(60)));
        assert!(!refresh_due(false, true, Duration::from_millis(500)));
        assert!(!refresh_due(false, true, Duration::from_millis(4_999)));
        assert!(refresh_due(false, true, Duration::from_secs(5)));
        assert!(refresh_due(true, true, Duration::ZERO));
        assert!(refresh_due(true, false, Duration::ZERO));
    }

    #[tokio::test]
    async fn coalesces_status_changed_events() {
        tokio::time::pause();

        let event_bus = Arc::new(EventBus::new(64));
        let emitter = Arc::new(OperatorStatusEmitter::new(Arc::clone(&event_bus)));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let emitter_handle = Arc::clone(&emitter).start(shutdown_rx);
        let mut rx = event_bus.subscribe();

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        for index in 0..10 {
            event_bus.publish(ForgeEvent {
                event_type: "task.status_changed".to_string(),
                entity_id: format!("task-{index}"),
                timestamp: event_timestamp(),
                context: EventContext::TaskStatusChanged {
                    project_id: "project-1".to_string(),
                    old_status: "todo".to_string(),
                    new_status: "in_progress".to_string(),
                },
            });
        }

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(600)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let mut operations_status_changed_count = 0;
        while let Ok(event) = rx.try_recv() {
            if event.event_type == OPERATIONS_STATUS_CHANGED_EVENT {
                operations_status_changed_count += 1;
                assert!(matches!(
                    event.context,
                    EventContext::OperationsStatusChanged { .. }
                ));
            }
        }

        shutdown_tx.send(true).expect("shutdown sends");
        emitter_handle.await.expect("emitter stops");

        assert_eq!(operations_status_changed_count, 1);
    }
}
