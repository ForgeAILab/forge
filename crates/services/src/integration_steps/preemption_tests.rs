//! Force SQLx's real custom-BEGIN future to stop after its acknowledgment,
//! before its handle check can construct a Transaction with a rollback guard.
use super::{tests::World, IntegrationStepAction};
use sqlx::Connection;
use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex, OnceLock},
    task::{Context, Wake, Waker},
    time::Duration,
};
use tokio::sync::{watch, Notify};

#[derive(Default)]
pub(crate) struct BeginSeam {
    preempt: Mutex<Option<watch::Sender<bool>>>,
    reached: Notify,
}

fn seams() -> &'static Mutex<HashMap<String, Arc<BeginSeam>>> {
    static SEAMS: OnceLock<Mutex<HashMap<String, Arc<BeginSeam>>>> = OnceLock::new();
    SEAMS.get_or_init(Default::default)
}

pub(crate) fn capture_preempt(task: &str, sender: watch::Sender<bool>) {
    if let Some(seam) = seams().lock().unwrap().get(task) {
        *seam.preempt.lock().unwrap() = Some(sender);
    }
}

#[derive(Default)]
struct WorkerGate {
    // BEGIN is initially stopped in SQLite's progress callback so the
    // first poll always installs its response waker before acknowledgment.
    state: Mutex<(bool, bool)>,
    changed: Condvar,
    response: Notify,
}

impl Wake for WorkerGate {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.response.notify_one();
        let mut state = self.state.lock().unwrap();
        // Keep SQLx's worker inside the response wake. The next poll can
        // acknowledge BEGIN, but cannot complete its subsequent UnlockDb.
        while !state.1 {
            state = self.changed.wait(state).unwrap();
        }
    }
}

struct ReleaseWorker(Arc<WorkerGate>);
impl Drop for ReleaseWorker {
    fn drop(&mut self) {
        *self.0.state.lock().unwrap() = (true, true);
        self.0.changed.notify_all();
    }
}

pub(crate) async fn interrupt_begin_if_armed(pool: &sqlx::SqlitePool, task: &str) {
    let seam = seams().lock().unwrap().remove(task);
    let Some(seam) = seam else { return };
    let mut connection = pool.acquire().await.unwrap();
    let gate = Arc::new(WorkerGate::default());
    let _release = ReleaseWorker(gate.clone());
    let progress = gate.clone();
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_progress_handler(1, move || {
            let mut state = progress.state.lock().unwrap();
            while !state.0 {
                state = progress.changed.wait(state).unwrap();
            }
            true
        });
    let waker = Waker::from(gate.clone());
    let mut begin = connection.begin_with("BEGIN IMMEDIATE");
    assert!(begin
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    gate.state.lock().unwrap().0 = true;
    gate.changed.notify_all();
    gate.response.notified().await;
    // This poll consumes SQLx's BEGIN response and then waits on the handle
    // mutex held by its worker. No Transaction exists at this point.
    assert!(begin
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    seam.preempt
        .lock()
        .unwrap()
        .as_ref()
        .expect("step worker captured its preempt sender")
        .send_replace(true);
    seam.reached.notify_one();
    // The real step worker drops this future on that signal. Keep both
    // BEGIN and the pooled connection alive until it does; the release
    // guard unblocks the SQLite thread even if the test panics.
    std::future::pending::<()>().await;
    drop(begin);
}

async fn preempt_begin_and_stop(task: &str) {
    let world = World::new().await;
    let attempt = world.add_task(task, task, "one\n").await;
    let asked = world.place(&attempt.id, "awaiting_task_step", 1).await;
    let seam = Arc::new(BeginSeam::default());
    seams().lock().unwrap().insert(task.into(), seam.clone());
    world.ask(&asked, IntegrationStepAction::Settle).await;
    tokio::time::timeout(Duration::from_secs(5), seam.reached.notified())
        .await
        .expect("preempt delivered after BEGIN acknowledgment");
    let settled = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let steps = world.step(task, IntegrationStepAction::Settle).await;
            if steps.len() == 1
                && steps[0].status == "superseded"
                && steps[0].claimed_by.is_none()
                && steps[0].lease_until.is_none()
            {
                assert_eq!(
                    steps[0].attempts, 1,
                    "settled without waiting for lease expiry"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        settled.is_ok(),
        "preempted BEGIN must relinquish its lock and step:\n{}",
        world.stuck(task).await
    );
    let stopped = tokio::time::timeout(Duration::from_secs(5), async {
        world.service.cancel_task(task).await.unwrap();
        assert_eq!(world.task(task).await.status, "cancelled");
        assert!(world
            .attempt(&attempt.id)
            .await
            .cancel_requested_at
            .is_some());
    })
    .await;
    assert!(
        stopped.is_ok(),
        "owner command must run:\n{}",
        world.stuck(task).await
    );
    assert!(world.attempt(&attempt.id).await.permit_json.is_none());
    assert!(world
        .step(task, IntegrationStepAction::Result)
        .await
        .is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancel_after_preempting_acknowledged_begin_releases_lock_and_lease() {
    preempt_begin_and_stop("cancel-begin-window").await;
}
