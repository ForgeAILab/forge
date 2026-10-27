//! Model-based workflow liveness test (refactor plan, Phase 4).
//!
//! A seeded generator drives random sequences of owner, executor, reviewer,
//! merge and crash actions through the real in-process stack: a file-backed
//! SQLite database, the real `TaskService`, Task-step worker, dispatcher,
//! review runner, merge service and crash recovery, a real temporary Git
//! repository, and a scripted executor in place of a provider CLI.
//!
//! It lives in the api test crate because that is where the fixtures it needs
//! already are: the router and the `/actions` offers endpoint (offers are
//! taken from what the API advertises, never guessed), the public
//! `TaskCondition` read model, daemon and Agent registration, and the
//! execution-setup helper.
//!
//! The run is turn based so that one seed always produces one history: no
//! background step worker or dispatcher loop runs. The model drains queued
//! Task steps and asks the dispatcher to reconcile until nothing changes
//! ("quiescence"), and every coder execution waits on a gate that only a model
//! action opens.
//!
//! The default run is deterministic: fixed seeds from `model_workflow/generator.rs`
//! and regression cases in `model_workflow/liveness_cases.rs`, with nothing drawn
//! from the clock, so it cannot turn an unrelated change red.
//! Random seeds are drawn only when `FORGE_MODEL_CASES=<n>` (n > 0) asks for
//! them; every seed is printed. Reproduce one case with
//! `FORGE_MODEL_SEED=<n>` (a comma list runs several). `FORGE_MODEL_LONG=1`
//! runs longer sequences (`FORGE_MODEL_STEPS` sets the length).
//! `FORGE_MODEL_NO_SHRINK=1` reports a failure as found, without replaying
//! shorter sequences (each replay of a stall costs its full patience).
//! `FORGE_MODEL_CASE=<text>` runs only the cases whose name contains it,
//! `FORGE_MODEL_LOG=<level>` prints the stack's log with timestamps, and
//! `FORGE_MODEL_LOCK_STORM=<ms>` adds a second writer to every model database.
//!
//! The server's run cap is pinned (`MODEL_RUN_CAP`, or `FORGE_MODEL_RUN_CAP=<n>`)
//! instead of derived from the host's cores, so a 4-core CI runner and a
//! workstation walk the same histories. It is the automatic cap of a small
//! machine: runs, and review checks, do wait for a slot in these sequences.
//! `sequences_stay_live_at_run_cap_1` and `sequences_stay_live_without_a_pinned_run_cap`
//! walk a few of the same sequences with one slot and with the host's own cap:
//! a machine limited to one run is a supported setting, and with it a Task's
//! whole life (run, review check, reviewer, merge) happens in that one slot.
#![allow(dead_code)]
mod common;

use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock, Weak,
    },
    time::Duration,
};

use api::{build_router, AppState};
use api_types::AgentResponse;
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use db::TaskStepRepo;
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind, LogKind, LogStream, LogWriter,
};
use forge_agent_host::{CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess};
use serde_json::{json, Value};
use tower::ServiceExt;

use checker::*;
use deadlines::*;
use executor::*;
use generator::*;
use lead::*;
use model::*;
use recovery::*;
use review_checks::*;
use runner::*;
use upgrade::*;

#[path = "model_workflow/checker.rs"]
mod checker;
#[path = "model_workflow/deadlines.rs"]
mod deadlines;
#[path = "model_workflow/executor.rs"]
mod executor;
#[path = "model_workflow/generator.rs"]
mod generator;
#[path = "model_workflow/lead.rs"]
mod lead;
#[path = "model_workflow/liveness_cases.rs"]
mod liveness_cases;
#[path = "model_workflow/model.rs"]
mod model;
#[path = "model_workflow/recovery.rs"]
mod recovery;
#[path = "model_workflow/review_cases.rs"]
mod review_cases;
#[path = "model_workflow/review_checks.rs"]
mod review_checks;
#[path = "model_workflow/runner.rs"]
mod runner;
#[path = "model_workflow/upgrade.rs"]
mod upgrade;
#[path = "model_workflow/upgrade_cases.rs"]
mod upgrade_cases;
