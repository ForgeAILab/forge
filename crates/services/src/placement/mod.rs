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

/// Classify selection filters; transport errors also require placement state.
pub(crate) fn is_retryable_admission_refusal(error: &crate::ServiceError) -> bool {
    match error {
        crate::ServiceError::Db(db::DbError::AgentAtCapacity | db::DbError::MachineAtCapacity) => {
            true
        }
        crate::ServiceError::PlacementUnavailable(refusal) => {
            refusal.rejected_candidates.iter().any(|candidate| {
                !candidate.filter_codes.is_empty()
                    && candidate.filter_codes.iter().all(|code| {
                        matches!(
                            code,
                            PlacementFilterCode::AgentCapacity
                                | PlacementFilterCode::MachineCapacity
                                | PlacementFilterCode::OwnerUnreachable
                        ) || (candidate
                            .filter_codes
                            .contains(&PlacementFilterCode::OwnerUnreachable)
                            && matches!(
                                code,
                                PlacementFilterCode::WorkspaceProtocolMissing
                                    | PlacementFilterCode::ExecutorUnavailable
                            ))
                    })
            })
        }
        _ => false,
    }
}

pub(crate) async fn admission_refusal_is_retryable(
    db: &db::SqliteDb,
    task_id: &str,
    error: &crate::ServiceError,
) -> crate::Result<bool> {
    let placement = db::WorkspacePlacementRepo::get_for_task(db, task_id).await?;
    if matches!(
        error,
        crate::ServiceError::DaemonUnavailable { .. }
            | crate::ServiceError::DaemonTimeout { .. }
            | crate::ServiceError::Db(db::DbError::VersionConflict)
    ) && placement.as_ref().is_some_and(|placement| {
        matches!(
            placement.state,
            db::PlacementState::Cleaning | db::PlacementState::Cleaned | db::PlacementState::Failed
        )
    }) {
        return Ok(false);
    }
    Ok(match error {
        crate::ServiceError::DaemonUnavailable { daemon_id } => {
            placement.as_ref().is_none_or(|placement| {
                placement
                    .daemon_id
                    .as_deref()
                    .or(placement.execution_daemon_id.as_deref())
                    == Some(daemon_id.as_str())
                    && matches!(
                        placement.state,
                        db::PlacementState::Ready | db::PlacementState::Disconnected
                    )
            })
        }
        crate::ServiceError::DaemonTimeout { daemon_id, method } => {
            method != api_types::METHOD_WORKSPACE_RUN
                && placement.as_ref().is_some_and(|placement| {
                    placement
                        .daemon_id
                        .as_deref()
                        .or(placement.execution_daemon_id.as_deref())
                        == Some(daemon_id.as_str())
                        && matches!(
                            placement.state,
                            db::PlacementState::Ready | db::PlacementState::Disconnected
                        )
                })
        }
        _ => is_retryable_admission_refusal(error),
    })
}

/// At least one otherwise eligible candidate was rejected solely for machine capacity.
pub(crate) fn is_machine_capacity_refusal(error: &crate::ServiceError) -> bool {
    matches!(
        error,
        crate::ServiceError::Db(db::DbError::MachineAtCapacity)
    ) || matches!(error, crate::ServiceError::PlacementUnavailable(refusal) if refusal.rejected_candidates.iter().any(|candidate| candidate.filter_codes == [PlacementFilterCode::MachineCapacity]))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn placement_upgrade_refusal_requires_an_otherwise_eligible_owner_and_no_retry_candidate() {
        use PlacementFilterCode::*;
        let refusal = |codes: Vec<Vec<PlacementFilterCode>>| PlacementUnavailable {
            task_id: "task".into(),
            repo_id: "repo".into(),
            rejected_candidates: codes
                .into_iter()
                .map(|filter_codes| CandidateRejection {
                    repo_location_id: "location".into(),
                    owner_kind: "daemon".into(),
                    daemon_id: Some("daemon".into()),
                    runtime_id: Some("runtime".into()),
                    filter_codes,
                })
                .collect(),
        };
        assert!(refusal(vec![vec![
            DaemonUpgradeRequired,
            CapabilityMissing,
            RunPurposeDenied
        ]])
        .needs_daemon_upgrade());
        for independent in [
            PinMismatch,
            NotVisible,
            OwnerUnreachable,
            ExecutorUnavailable,
            AgentCapacity,
            MachineCapacity,
        ] {
            assert!(
                !refusal(vec![vec![DaemonUpgradeRequired, independent]]).needs_daemon_upgrade(),
                "{independent:?}"
            );
        }
        for retry in [LocationNotReady, AgentCapacity, MachineCapacity] {
            let refusal = refusal(vec![vec![DaemonUpgradeRequired], vec![retry]]);
            assert!(!refusal.needs_daemon_upgrade(), "{retry:?}");
        }
        assert!(
            refusal(vec![vec![DaemonUpgradeRequired], vec![PinMismatch]]).needs_daemon_upgrade()
        );
    }
    #[test]
    fn placement_recovery_queues_transient_owner_and_capacity_refusals() {
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
        assert!(is_retryable_admission_refusal(&error(vec![
            PlacementFilterCode::AgentCapacity
        ])));
        assert!(is_retryable_admission_refusal(&error(vec![
            PlacementFilterCode::MachineCapacity
        ])));
        assert!(!is_retryable_admission_refusal(&error(vec![
            PlacementFilterCode::AgentCapacity,
            PlacementFilterCode::ExecutorUnavailable
        ])));
        assert!(!is_retryable_admission_refusal(&error(vec![])));
        assert!(is_retryable_admission_refusal(&error(vec![
            PlacementFilterCode::OwnerUnreachable
        ])));
        assert!(is_retryable_admission_refusal(&error(vec![
            PlacementFilterCode::OwnerUnreachable,
            PlacementFilterCode::WorkspaceProtocolMissing,
            PlacementFilterCode::ExecutorUnavailable
        ])));
        assert!(!is_retryable_admission_refusal(&error(vec![
            PlacementFilterCode::LocationNotReady
        ])));
        assert!(!is_retryable_admission_refusal(&error(vec![
            PlacementFilterCode::OwnerUnreachable,
            PlacementFilterCode::PinMismatch
        ])));
    }
    #[tokio::test]
    async fn placement_terminal_owner_states_are_permanent_admission_refusals() {
        let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = db::SqliteDb::new(pool);
        let (task, mut placement, _) = crate::recovery::tests::daemon_owned_fixture(&db).await;
        let unavailable = crate::ServiceError::DaemonUnavailable {
            daemon_id: placement.daemon_id.clone().unwrap(),
        };
        assert!(admission_refusal_is_retryable(&db, &task.id, &unavailable)
            .await
            .unwrap());
        for state in [
            db::PlacementState::Failed,
            db::PlacementState::Cleaning,
            db::PlacementState::Cleaned,
        ] {
            let mut update = admission::placement_update(&placement);
            update.state = Some(state);
            placement = db::WorkspacePlacementRepo::update(&db, update)
                .await
                .unwrap();
            assert!(!admission_refusal_is_retryable(&db, &task.id, &unavailable)
                .await
                .unwrap());
            assert!(!admission_refusal_is_retryable(
                &db,
                &task.id,
                &crate::ServiceError::DaemonTimeout {
                    daemon_id: placement.daemon_id.clone().unwrap(),
                    method: api_types::METHOD_WORKSPACE_DESCRIBE.into(),
                }
            )
            .await
            .unwrap());
            assert!(admission_refusal_is_retryable(
                &db,
                &task.id,
                &crate::ServiceError::Db(db::DbError::AgentAtCapacity)
            )
            .await
            .unwrap());
            assert!(!admission_refusal_is_retryable(
                &db,
                &task.id,
                &crate::ServiceError::Db(db::DbError::VersionConflict)
            )
            .await
            .unwrap());
        }
    }
}
