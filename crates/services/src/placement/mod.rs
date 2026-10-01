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

/// Recovery queues a refusal only when an otherwise compatible owner is full.
pub(crate) fn is_capacity_refusal(error: &crate::ServiceError) -> bool {
    match error {
        crate::ServiceError::Db(db::DbError::AgentAtCapacity) => true,
        crate::ServiceError::PlacementUnavailable(refusal) => {
            refusal.rejected_candidates.iter().any(|candidate| {
                !candidate.filter_codes.is_empty()
                    && candidate.filter_codes.iter().all(|code| {
                        matches!(
                            code,
                            PlacementFilterCode::AgentCapacity
                                | PlacementFilterCode::DaemonCapacity
                        )
                    })
            })
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn placement_recovery_queues_only_capacity_refusals() {
        let error = |codes| {
            crate::ServiceError::PlacementUnavailable(PlacementUnavailable {
                task_id: "task".into(),
                repo_id: "repo".into(),
                rejected_candidates: vec![CandidateRejection {
                    repo_location_id: "location".into(),
                    owner_kind: "server".into(),
                    daemon_id: None,
                    runtime_id: None,
                    filter_codes: codes,
                }],
            })
        };
        assert!(is_capacity_refusal(&error(vec![
            PlacementFilterCode::AgentCapacity
        ])));
        assert!(is_capacity_refusal(&error(vec![
            PlacementFilterCode::DaemonCapacity
        ])));
        assert!(!is_capacity_refusal(&error(vec![
            PlacementFilterCode::AgentCapacity,
            PlacementFilterCode::ExecutorUnavailable
        ])));
        assert!(!is_capacity_refusal(&error(vec![])));
    }
}
