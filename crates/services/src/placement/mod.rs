//! Placement admission decisions and capacity reads for reserve/start transactions.

pub mod admission;
pub mod capacity;
pub mod selection;

pub use selection::{
    load_selection_context, needed_run_purposes, select_placement, CandidateRejection,
    ConnectionHandshake, ExecutorFacts, PlacementCandidate, PlacementFilterCode,
    PlacementSelection, PlacementUnavailable, SelectionContext, SelectionLoadInput,
    SelectionOutcome, SelectionReason, SelectionRule, ServerFacts, WorktreeAgent,
};
