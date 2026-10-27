//! Real-database scheduler equivalence against `9d9b228f`.
//!
//! This file compiles unchanged on the base commit. There it records what the
//! scanning dispatcher did with each shape (`SCHEDULER_EQUIVALENCE_RECORD=<dir>`);
//! here it replays the same rows through the reconciling dispatcher and compares
//! the outcome with that checked-in recording: dispatched or held, the target,
//! and every legacy field and event written. The only differences accepted are
//! the `@<Exception>` lines of the fixture, each naming one allowed change.
use super::*;
use std::collections::HashMap;

macro_rules! unchanged_cases {
    ($($name:ident => $case:expr,)+) => {
        mod unchanged_condition_guard {
            $(#[test]
            fn $name() {
                super::on_runtime(|| super::unchanged_shapes($case));
            })+
        }
    };
}

unchanged_cases! {
    shapes_0_mod_8 => 0,
    shapes_1_mod_8 => 1,
    shapes_2_mod_8 => 2,
    shapes_3_mod_8 => 3,
    shapes_4_mod_8 => 4,
    shapes_5_mod_8 => 5,
    shapes_6_mod_8 => 6,
    shapes_7_mod_8 => 7,
}

/// The old test names still select every case as a libtest filter.
macro_rules! shape_cases {
    ($group:ident, $mode:expr, $prime:expr; $($name:ident => $case:expr,)+) => {
        mod $group {
            // The cases are exactly 0..SHAPE_CASES, or this does not compile.
            const _: () = {
                let cases = [$($case),+];
                assert!(cases.len() == super::SHAPE_CASES);
                let mut index = 0;
                while index < cases.len() {
                    assert!(cases[index] == index);
                    index += 1;
                }
            };
            $(
                #[test]
                fn $name() {
                    super::on_runtime(|| super::shapes($mode, $prime, $case));
                }
            )+
        }
    };
}

shape_cases! {
    scheduler_equivalence_shapes_steady, "steady", true;
    shapes_0_mod_8 => 0,
    shapes_1_mod_8 => 1,
    shapes_2_mod_8 => 2,
    shapes_3_mod_8 => 3,
    shapes_4_mod_8 => 4,
    shapes_5_mod_8 => 5,
    shapes_6_mod_8 => 6,
    shapes_7_mod_8 => 7,
}

shape_cases! {
    scheduler_equivalence_shapes_startup, "startup", false;
    shapes_0_mod_8 => 0,
    shapes_1_mod_8 => 1,
    shapes_2_mod_8 => 2,
    shapes_3_mod_8 => 3,
    shapes_4_mod_8 => 4,
    shapes_5_mod_8 => 5,
    shapes_6_mod_8 => 6,
    shapes_7_mod_8 => 7,
}

#[path = "equivalence/helpers.rs"]
mod helpers;
use helpers::*;
#[path = "equivalence/condition_guards.rs"]
mod condition_guards;
#[path = "equivalence/recorded_outcomes.rs"]
mod recorded_outcomes;
