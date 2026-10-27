//! Real SQLite, the real server owner and real Git in temp dirs; fake Task
//! steps, fake check verdicts, fake object transfer, a clock the test moves.
use super::*;
use crate::integration_effects::{EffectOwner, EffectWorkspace};
use crate::integration_owner::ServerIntegrationOwner;
use db::{
    IntegrationAttemptState as S, IntegrationCheckTiming, IntegrationCiSkipReason,
    IntegrationEffectAdmission, IntegrationEffectRequest, IntegrationFailureKind,
    IntegrationLostRaceKind, IntegrationOperationKind,
};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[macro_use]
mod helpers;
use helpers::*;

mod cancellation;
mod merging;
mod recovery;
mod step_delivery;
mod supervision_and_import;

#[test]
fn head_table_is_total_and_inside_the_storage_graph() {
    for state in S::ALL {
        let row = head_row(*state);
        for to in row.success.iter().chain(row.failure).chain(&row.on_timeout) {
            assert!(
                state.exits().contains(to),
                "{state} -> {to} is not a storage transition"
            );
        }
        assert_eq!(
            row.timeout == HeadTimeout::None,
            row.on_timeout.is_none(),
            "{state}: a timeout names its edge"
        );
        // Every non-terminal state has a way out that the worker performs.
        assert!(
            state.terminal() || !row.success.is_empty() || !row.failure.is_empty(),
            "{state} has no exit"
        );
        // The cancel column agrees with what storage lets a Task step ask.
        assert_eq!(
            row.allows(S::Cancelled),
            matches!(row.cancel, CancelRule::Now | CancelRule::AfterReceipt),
            "{state}"
        );
        assert!(
            !row.allows(S::Cancelled) || state.cancel_requestable(),
            "{state}"
        );
    }
    assert_eq!(HEAD_TABLE.len(), S::ALL.len());
}
#[test]
fn worker_sources_stay_inside_the_boundary() {
    for (name, source) in [
        ("mod", include_str!("mod.rs")),
        ("driver", include_str!("driver.rs")),
        ("driver/states", include_str!("driver/states.rs")),
        ("ports", include_str!("ports.rs")),
        ("table", include_str!("table.rs")),
    ] {
        let code = source
            .lines()
            .map(|line| line.split("//").next().unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n");
        for forbidden in [
            "git::",
            "sqlx::",
            ".pool()",
            "Command::",
            "events::",
            "EventBus",
            "TaskRepo",
            "TaskStepRepo",
            "TaskService",
            "ReviewRepo",
            "MergeService",
            "ExecutionRepo",
            "task_service::",
            "task_writer",
            "workflow::",
            "check_runner::",
            "state_integration_condition",
            "record_integration_observation",
            "observe_integration_",
            "admit_integration_attempt",
            "supersede_integration_attempt",
            "request_integration_cancel",
            "UPDATE ",
            "INSERT ",
            "DELETE ",
        ] {
            assert!(
                !code.contains(forbidden),
                "integration_worker/{name}.rs acquired forbidden capability {forbidden}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_matrix_every_state_and_await_point_recovers_after_takeover() {
    for (moved, state, point, expected) in CRASH_MATRIX {
        let label = format!("moved={moved} {state}/{point:?}");
        let world = World::new().await;
        let mut pump = Pump::new(world.worker());
        let a = world.add_task("r", "a", "a.txt", "a\n").await;
        if *moved {
            world.external_commit("r", "outside").await;
        }
        let fired = arm(&pump.worker, *state, *point);
        let crashed = world.pump(&mut pump).await;
        assert!(fired.load(Ordering::SeqCst), "{label}: never reached");
        assert!(crashed.is_some(), "{label}: no crash");
        drop(pump);
        // The process is gone; its lease runs out; another one takes over.
        world.clock.advance(120);
        let mut pump = Pump::new(world.worker());
        world.settle(&mut pump).await;
        let end = world.attempt(&a.id).await;
        assert_eq!(end.state, *expected, "{label}");
        assert_eq!(Some(world.tip("r").await), end.candidate_sha, "{label}");
        assert!(end.effect_intent_json.is_none(), "{label}");
        assert!(ff_successes(&world, &a.id).await <= 1, "{label}");
        let commits = git_out(&world.repo("r"), &["rev-list", "--count", "main"])
            .await
            .unwrap();
        assert_eq!(commits, if *moved { "3" } else { "2" }, "{label}");
        let status: String = sqlx::query_scalar("SELECT status FROM task WHERE id='a'")
            .fetch_one(world.db.pool())
            .await
            .unwrap();
        assert_eq!(status, "done", "{label}");
        assert!(
            world.queue_of(&a).await.head_attempt_id.is_none(),
            "{label}"
        );
    }
}
