//! Integration activation test modules.

use super::activation::{
    CANCEL_REQUESTED_SQL, CLAIMABLE_QUEUES_SQL, DUE_PARKED_SQL, EXPIRED_HEADS_SQL, PRUNE_SQL,
    RETAINED_EVIDENCE_SQL, TIMING_SAMPLES_SQL,
};
use super::tests::{admission, fixture, legacy_digest, seed, FIXED_TIME};
use super::*;
use crate::{create_sqlite_pool, run_migrations};
use std::sync::Arc;

use super::shadow;

use helpers::*;

#[path = "activation_tests/cancellation.rs"]
mod cancellation;
#[path = "activation_tests/helpers.rs"]
mod helpers;
#[path = "activation_tests/imported_attempts.rs"]
mod imported_attempts;
#[path = "activation_tests/recovery.rs"]
mod recovery;
#[path = "activation_tests/schema.rs"]
mod schema;
#[path = "activation_tests/sweeps_and_timings.rs"]
mod sweeps_and_timings;
