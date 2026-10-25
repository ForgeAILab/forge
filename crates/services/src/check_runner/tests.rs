use super::*;
use api_types::*;
use db::{
    CheckCleanup, CheckConsumerOrigin, CheckDeliveryRepo, CheckResultEvidence, CheckResultOutcome,
    CheckRunFence, CheckRunIdentity, CheckRunState, SqliteDb, TaskStepRepo,
};

use super::worker::{CheckOwnerPort, CheckRunWorker};
use db::{
    CheckAdmission, CheckDispatchIntent, CheckDispatchTarget, CheckWorkerRecord, CheckWorkerRepo,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_util::sync::CancellationToken;

#[path = "tests/canonical.rs"]
mod canonical;
#[path = "tests/capacity.rs"]
mod capacity;
#[path = "tests/consumers.rs"]
mod consumers;
#[path = "tests/dispatch.rs"]
mod dispatch;
#[path = "tests/helpers.rs"]
mod helpers;
#[path = "tests/requests.rs"]
mod requests;
#[path = "tests/waits.rs"]
mod waits;

use helpers::*;
