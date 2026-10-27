//! The Task gate and the reconciler against the real worker over its
//! production ports, the real step worker and real Git (plan 3.2 stage D2b,
//! round 2). A head is stepped by hand where a test needs the gate to land at
//! one exact point; otherwise the worker runs as the server runs it.
use super::*;
use crate::integration_worker::{HeadDriver, Pass};
use api_types::TaskAction;

#[path = "gate_tests/helpers.rs"]
mod helpers;
use helpers::*;

#[path = "gate_tests/custom_workflows.rs"]
mod custom_workflows;
#[path = "gate_tests/holds.rs"]
mod holds;
#[path = "gate_tests/quarantined_imports.rs"]
mod quarantined_imports;
#[path = "gate_tests/reconciliation.rs"]
mod reconciliation;
#[path = "gate_tests/recovery.rs"]
mod recovery;

/// One Task the import cannot read does not stop the pass: it is
/// quarantined with the error, the others are imported, and the unreadable
/// Task is still admitted by the reconciler from its present rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_unreadable_task_is_quarantined_and_the_import_continues() {
    let world = World::new().await;
    for task in ["bad", "good"] {
        world
            .legacy_task(task, &format!("{task}.txt"), "merging")
            .await;
        world.seed_execution(task, "executor", None).await;
        world.touch(task).await;
    }
    sqlx::query(
        "CREATE TRIGGER unreadable BEFORE INSERT ON integration_attempt \
         WHEN NEW.task_ref='bad' AND NEW.state<>'quarantined' \
         BEGIN SELECT RAISE(ABORT,'legacy rows of this Task cannot be read'); END",
    )
    .execute(world.db.pool())
    .await
    .unwrap();
    let report = super::super::import::import_at_start(&world.db)
        .await
        .unwrap();
    assert_eq!(
        (report.imported, report.unreadable, report.quarantined),
        (2, 1, 1),
        "{report:?}"
    );
    let again = super::super::import::import_at_start(&world.db)
        .await
        .unwrap();
    assert_eq!((again.imported, again.unreadable), (0, 0), "run twice");
    let (state, current, message): (String, bool, String) = sqlx::query_as(
        "SELECT state,current,failure_message FROM integration_attempt WHERE task_ref='bad'",
    )
    .fetch_one(world.db.pool())
    .await
    .unwrap();
    assert_eq!((state.as_str(), current), ("quarantined", false));
    assert!(message.contains("cannot be read"), "{message}");
    assert!(world
        .db
        .current_integration_attempt("good")
        .await
        .unwrap()
        .is_some());
    sqlx::query("DROP TRIGGER unreadable")
        .execute(world.db.pool())
        .await
        .unwrap();
    let production = Production::new(&world);
    let running = production.run();
    let dispatcher = dispatcher(&world).await;
    world
        .until("bad", "both Tasks merge", || async {
            dispatcher.reconcile_all().await.unwrap();
            world.task("bad").await.status == "done" && world.task("good").await.status == "done"
        })
        .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}
