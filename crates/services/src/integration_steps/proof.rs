//! The settle-time proof: before a Task is reported merged, the owner of the
//! target's default checkout says whether the target branch contains the
//! commit.
//!
//! The attempt's `integrated_sha` is a claim (the worker wrote it from a
//! receipt or from a witness it read earlier). It is not what the Task is
//! reported on: the target can have been rewound since, and a row can carry a
//! commit nothing ever merged. The answer is read once, now, where the target
//! lives: one `git merge-base --is-ancestor` in a server-owned checkout, the
//! read-only `integration.target_facts` of the daemon that owns a
//! daemon-owned one. No receipt replaces it; a receipt that disagrees with
//! the attempt refuses the report before any Git is read.
//!
//! It writes nothing, and the `result` step asks it outside of any database
//! transaction.
use crate::daemon_transport::{
    workspace_client::{DaemonObjectEndpoint, DaemonWorkspaceClient},
    DaemonConnectionRegistry,
};
use crate::integration_owner::OwnerMergeReceipt;
use async_trait::async_trait;
use db::{
    IntegrationAttempt, IntegrationEffectReceipt, IntegrationOperationKind,
    IntegrationOperationState, IntegrationQueue, SqliteDb,
};
use std::sync::Arc;

/// What the target's owner said about one commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetProof {
    /// The target branch contains the commit.
    Contains,
    /// The owner answered: the target branch does not contain it.
    Absent(String),
    /// No answer (the owner is not connected, the checkout is unreadable,
    /// the queue has no target).
    Unknown(String),
}

/// One read on the owner of a queue's default checkout.
#[async_trait]
pub trait TargetProofSource: Send + Sync {
    async fn target_contains(&self, queue: &IntegrationQueue, sha: &str) -> TargetProof;
}

/// The production [`TargetProofSource`].
pub struct TargetOwnerProof {
    db: Arc<SqliteDb>,
    daemons: Option<Arc<DaemonConnectionRegistry>>,
}

fn target_query_error(error: db::DbError) -> sqlx::Error {
    match error {
        db::DbError::Sqlx(error) => error,
        error => sqlx::Error::Protocol(error.to_string()),
    }
}

impl TargetOwnerProof {
    pub fn new(db: Arc<SqliteDb>, daemons: Option<Arc<DaemonConnectionRegistry>>) -> Self {
        Self { db, daemons }
    }
}

fn object_id(sha: &str) -> bool {
    matches!(sha.len(), 40 | 64) && sha.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[async_trait]
impl TargetProofSource for TargetOwnerProof {
    async fn target_contains(&self, queue: &IntegrationQueue, sha: &str) -> TargetProof {
        #[cfg(test)]
        test_reads::add(sha);
        if !object_id(sha) {
            return TargetProof::Absent(format!("`{sha}` is not a commit id"));
        }
        let Some(location_id) = queue.target_location_id.as_deref() else {
            return TargetProof::Unknown("the queue has no default checkout".into());
        };
        let row: Option<(String, String, Option<String>, Option<String>)> = match self
            .db
            .integration_target_location(location_id)
            .await
            .map(|row| row.map(|row| (row.path, row.owner_kind, row.daemon_id, row.runtime_id)))
            .map_err(target_query_error)
        {
            Ok(row) => row,
            Err(error) => return TargetProof::Unknown(error.to_string()),
        };
        let Some((path, owner_kind, daemon_id, runtime_id)) = row else {
            return TargetProof::Unknown("the default checkout is gone".into());
        };
        let branch = &queue.target_branch;
        if owner_kind == "server" {
            let target = format!("refs/heads/{branch}");
            return match git::command_output(
                std::path::Path::new(&path),
                &["merge-base", "--is-ancestor", sha, &target],
            )
            .await
            {
                Ok(output) if output.status.success() => TargetProof::Contains,
                // 1: not an ancestor. 128: the checkout does not hold the
                // commit (or the branch): it cannot contain it either.
                Ok(output) if matches!(output.status.code(), Some(1 | 128)) => {
                    TargetProof::Absent(format!("`{branch}` does not contain {sha}"))
                }
                Ok(output) => TargetProof::Unknown(format!(
                    "git ended with {}: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                )),
                Err(error) => TargetProof::Unknown(error.to_string()),
            };
        }
        let (Some(daemon_id), Some(runtime_id), Some(daemons)) =
            (daemon_id, runtime_id, self.daemons.as_ref())
        else {
            return TargetProof::Unknown(
                "the machine that holds the default checkout is not known to this server".into(),
            );
        };
        match DaemonWorkspaceClient::new(Arc::clone(daemons))
            .target_facts(
                DaemonObjectEndpoint {
                    daemon_id: &daemon_id,
                    runtime_id: &runtime_id,
                    repo_location_id: location_id,
                },
                branch,
                sha,
            )
            .await
        {
            Ok(facts) if facts.candidate_in_target => TargetProof::Contains,
            Ok(_) => TargetProof::Absent(format!("`{branch}` does not contain {sha}")),
            Err(error) => TargetProof::Unknown(format!(
                "machine {daemon_id} did not answer for the default checkout: {error}"
            )),
        }
    }
}

/// The machine that holds `queue`'s default checkout, when it is a daemon
/// this server still knows (not removed) and that is not connected now: the
/// one case of "no answer" that ends by itself, when the machine comes
/// back. `None` for a server-owned checkout, a connected or removed machine,
/// and a queue without a default checkout (nobody will ever answer those).
pub(crate) async fn target_owner_away(
    db: &SqliteDb,
    daemons: Option<&Arc<DaemonConnectionRegistry>>,
    queue: &IntegrationQueue,
) -> crate::Result<Option<String>> {
    let Some(location_id) = queue.target_location_id.as_deref() else {
        return Ok(None);
    };
    let daemon_id: Option<String> = db
        .integration_target_disconnected_daemon_id(location_id)
        .await?
        .flatten();
    Ok(daemon_id.filter(|id| !daemons.is_some_and(|registry| registry.get(id).is_some())))
}

/// The attempt's own records agree on what merged, before any Git is read.
/// `Ok`: the commit to prove. `Err`: why the row cannot be reported.
///
/// A fast-forward receipt on the attempt must be this attempt's, of a claim
/// generation it has had, performed by the queue's target owner, and must
/// name the commit the row says was integrated. An attempt reported from a
/// landed witness (no fast-forward of its own) has no such receipt; the
/// ancestry read alone decides it.
pub(crate) fn recorded_commit(
    attempt: &IntegrationAttempt,
    queue: &IntegrationQueue,
) -> std::result::Result<String, String> {
    let Some(integrated) = attempt.integrated_sha.clone().filter(|sha| !sha.is_empty()) else {
        return Err("the attempt records no merged commit".into());
    };
    let receipts: Vec<IntegrationEffectReceipt> =
        serde_json::from_value(attempt.effect_receipts_json.clone()).unwrap_or_default();
    let merged = receipts.iter().rev().find_map(|receipt| {
        (receipt.operation_state == IntegrationOperationState::Succeeded
            && matches!(
                receipt.request.kind,
                IntegrationOperationKind::FastForward | IntegrationOperationKind::Merge
            ))
        .then(
            || match serde_json::from_value::<OwnerMergeReceipt>(receipt.result.clone()) {
                Ok(OwnerMergeReceipt::Completed {
                    outcome: crate::MergeOutcome::Done { after_sha, .. },
                }) => Some((&receipt.request.fence, after_sha)),
                _ => None,
            },
        )
        .flatten()
    });
    if let Some((fence, after)) = merged {
        if fence.attempt_id != attempt.id
            || Some(fence.queue_id.as_str()) != attempt.queue_id.as_deref()
        {
            return Err("the merge receipt belongs to another attempt".into());
        }
        if fence.generation > attempt.slot_generation {
            return Err(format!(
                "the merge receipt is of claim generation {}, the attempt has reached {}",
                fence.generation, attempt.slot_generation
            ));
        }
        // The owner's identity: its checkout, its kind, its machine. The
        // location's version and the machine's runtime id change without the
        // owner changing (a re-verification, a daemon restart).
        let same_owner = queue.target_owner_json.as_ref().is_none_or(|owner| {
            ["location_id", "owner_kind", "daemon_id"]
                .iter()
                .all(|field| owner[field] == fence.target_owner[field])
        });
        if !same_owner {
            return Err("the merge receipt is not from the owner of the target".into());
        }
        if after != integrated {
            return Err(format!(
                "the merge receipt names {after} and the attempt {integrated}"
            ));
        }
    }
    Ok(integrated)
}

/// Test-only: how often the production proof was asked for a commit.
#[cfg(test)]
pub(crate) mod test_reads {
    use std::{collections::HashMap, sync::Mutex};
    static READS: Mutex<Option<HashMap<String, usize>>> = Mutex::new(None);
    pub(super) fn add(sha: &str) {
        *READS
            .lock()
            .unwrap()
            .get_or_insert_with(HashMap::new)
            .entry(sha.to_owned())
            .or_default() += 1;
    }
    pub(crate) fn of(sha: &str) -> usize {
        READS
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|reads| reads.get(sha).copied())
            .unwrap_or(0)
    }
}
