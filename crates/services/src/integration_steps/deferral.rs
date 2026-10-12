//! The one rule for the cause of an integration deferral.
//!
//! Four writers state `deferred` for an attempt that left the line or whose
//! result is unknown: the `park` step, the import continuation, the
//! reconciler's restatement (from the scheduler's compact snapshot) and the
//! `result` step (always `unresolved_result`, for an `applied` attempt). The
//! first three can write the same row. They all decide the cause here, from
//! facts of the attempt row alone, so whichever of them writes first or last
//! the Task reads one cause.
use api_types::IntegrationDeferralCause as C;
use db::{IntegrationAttempt, IntegrationAttemptState as S, IntegrationFailureKind as K};

/// What the importer left on a row that only the importer has parked.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ImportFacts {
    /// The importer's classification row (P1..P20).
    pub priority: Option<u64>,
    /// The row names the old operation (its result may be unknown).
    pub operation: bool,
    /// A stored deadline: the worker's sweep queues the row again at it.
    pub deadline: bool,
    /// The machine a pre-queue owner wait named (row P15).
    pub waited_owner: Option<String>,
}

/// The facts of a `parked` or `quarantined` attempt (or of the suspended
/// queue of a `queued` one) that decide its deferral.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeferralFacts<'a> {
    pub quarantined: bool,
    pub kind: Option<K>,
    pub message: &'a str,
    /// The machine the attempt is bound to.
    pub daemon_id: Option<&'a str>,
    /// `Some` for an imported attempt the queue worker has not parked
    /// itself: its failure fields are the importer's classification, not a
    /// worker park reason. Once the worker parked it, the row reads as any
    /// other attempt.
    pub import: Option<ImportFacts>,
}

/// The cause and the machine waited for.
pub(crate) fn deferral(facts: &DeferralFacts<'_>) -> (C, Option<String>) {
    if facts.quarantined {
        // An imported row quarantined with no operation to ask about has no
        // result that could still arrive: its owner decides.
        let unanswerable = facts
            .import
            .as_ref()
            .is_some_and(|import| !import.operation);
        return (
            if unanswerable {
                C::OwnerRequired
            } else {
                C::UnresolvedResult
            },
            None,
        );
    }
    if let Some(import) = &facts.import {
        return if import.priority == Some(15) {
            // Ends when the machine is back, whatever retry time the old
            // build had stored for its own polling.
            (C::OwnerOffline, import.waited_owner.clone())
        } else if import.deadline {
            // A stored deadline: the Task is admitted again at it.
            (C::Infrastructure, None)
        } else {
            (C::OwnerRequired, None)
        };
    }
    let offline = facts.message.starts_with("owner_offline");
    let cause = if facts.message.starts_with("target_dirty") {
        C::TargetDirty
    } else if offline {
        C::OwnerOffline
    } else {
        match facts.kind {
            Some(K::Infrastructure | K::Timeout) => C::Infrastructure,
            Some(K::TargetUnavailable) => C::OwnerOffline,
            Some(K::NeedsFact) => C::UnresolvedResult,
            _ => C::OwnerRequired,
        }
    };
    // A wait for the machine that holds the Task's checkout names it.
    (
        cause,
        offline
            .then(|| facts.daemon_id.map(str::to_owned))
            .flatten(),
    )
}

/// The queue worker's own park of this attempt is in its journal.
fn worker_parked(journal: &serde_json::Value) -> bool {
    journal
        .as_array()
        .is_some_and(|entries| entries.iter().any(|entry| entry["kind"] == "worker_park"))
}

/// The import facts of an attempt row: `None` unless the importer made the
/// row and the worker has not parked it since.
pub(crate) fn import_facts(
    attempt: &IntegrationAttempt,
    waited_owner: Option<String>,
) -> Option<ImportFacts> {
    let source = attempt.import_source_json.as_ref()?;
    if attempt.state != S::Quarantined && worker_parked(&attempt.operation_receipts_json) {
        return None;
    }
    Some(ImportFacts {
        priority: source["priority"].as_u64(),
        operation: attempt.current_operation_state.is_some() || attempt.operation_id.is_some(),
        deadline: attempt.available_at.is_some(),
        waited_owner,
    })
}

/// The same facts from the scheduler's compact statement snapshot
/// (`db::integration_statement_in_tx`).
pub(crate) fn snapshot_import_facts(snapshot: &serde_json::Value) -> Option<ImportFacts> {
    if snapshot["imported"] != true
        || (snapshot["state"] != "quarantined" && snapshot["worker_parked"] == true)
    {
        return None;
    }
    Some(ImportFacts {
        priority: snapshot["import_priority"].as_u64(),
        operation: snapshot["operation"] == true,
        deadline: snapshot["deadline"] == true,
        waited_owner: snapshot["waited_owner"].as_str().map(str::to_owned),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(quarantined: bool, kind: Option<K>, message: &str) -> DeferralFacts<'_> {
        DeferralFacts {
            quarantined,
            kind,
            message,
            daemon_id: Some("m"),
            import: None,
        }
    }

    #[test]
    fn worker_parks_read_their_kind_and_reason_code() {
        for (kind, message, cause, owner) in [
            (
                Some(K::Infrastructure),
                "infrastructure: x",
                C::Infrastructure,
                None,
            ),
            (Some(K::Timeout), "timeout: x", C::Infrastructure, None),
            (
                Some(K::NeedsFact),
                "target_missing_commit: x",
                C::UnresolvedResult,
                None,
            ),
            (
                Some(K::OwnerRequired),
                "target_dirty: a",
                C::TargetDirty,
                None,
            ),
            (
                Some(K::TargetUnavailable),
                "owner_offline: away",
                C::OwnerOffline,
                Some("m"),
            ),
            (
                Some(K::TargetUnavailable),
                "target_not_ready: x",
                C::OwnerOffline,
                None,
            ),
            (
                Some(K::OwnerRequired),
                "retries_exhausted: infrastructure: x",
                C::OwnerRequired,
                None,
            ),
            (None, "integration is waiting", C::OwnerRequired, None),
        ] {
            assert_eq!(
                deferral(&facts(false, kind, message)),
                (cause, owner.map(str::to_owned)),
                "{message}"
            );
        }
        assert_eq!(
            deferral(&facts(true, Some(K::Infrastructure), "x")).0,
            C::UnresolvedResult
        );
    }

    #[test]
    fn an_imported_row_reads_the_importers_classification_until_the_worker_parks_it() {
        let import = |priority, operation, deadline| ImportFacts {
            priority: Some(priority),
            operation,
            deadline,
            waited_owner: (priority == 15).then(|| "old".to_owned()),
        };
        let with = |quarantined, import| DeferralFacts {
            import: Some(import),
            ..facts(quarantined, Some(K::NeedsFact), "evidence")
        };
        assert_eq!(
            deferral(&with(true, import(20, false, false))).0,
            C::OwnerRequired
        );
        assert_eq!(
            deferral(&with(true, import(2, true, false))).0,
            C::UnresolvedResult
        );
        assert_eq!(
            deferral(&with(false, import(15, false, true))),
            (C::OwnerOffline, Some("old".to_owned()))
        );
        assert_eq!(
            deferral(&with(false, import(14, false, true))).0,
            C::Infrastructure
        );
        assert_eq!(
            deferral(&with(false, import(13, false, false))).0,
            C::OwnerRequired
        );
    }
}
