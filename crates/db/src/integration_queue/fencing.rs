//! Durable attempt ownership and effect receipts; no Git or Task mutation.
use super::*;
use sha2::{Digest, Sha256};

pub use api_types::IntegrationOwnerFence;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrationEffectRefusal {
    StaleFence,
    ForeignOwner,
    ReconciliationRequired,
    RequestConflict,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntegrationEffectRequest {
    pub fence: IntegrationOwnerFence,
    pub kind: IntegrationOperationKind,
    /// Frozen workspace/placement, HEAD, target and effect input. No secrets.
    pub witness: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntegrationEffectReceipt {
    pub request: IntegrationEffectRequest,
    pub result: Value,
    pub operation_state: IntegrationOperationState,
    pub recorded_at: String,
}

pub enum IntegrationEffectAdmission {
    Replay(IntegrationEffectReceipt),
    Started(IntegrationEffectGuard),
    Refused(IntegrationEffectRefusal),
}

/// Serializes one owner's checkout without retaining SQLite's single writer.
/// Intent and receipt transactions are short; a dropped guard leaves a durable
/// checkpoint that distinguishes an admitted request from a started effect.
pub struct IntegrationEffectGuard {
    pool: sqlx::SqlitePool,
    request: IntegrationEffectRequest,
    _owner_locks: Vec<tokio::sync::OwnedMutexGuard<()>>,
}

async fn lock_owner(request: &IntegrationEffectRequest) -> Vec<tokio::sync::OwnedMutexGuard<()>> {
    use std::sync::{Arc, Mutex, OnceLock, Weak};
    type OwnerLocks = Mutex<std::collections::HashMap<String, Weak<tokio::sync::Mutex<()>>>>;
    static LOCKS: OnceLock<OwnerLocks> = OnceLock::new();
    let owner = &request.fence.target_owner;
    let prefix = format!(
        "{}:{}:{}",
        owner["owner_kind"], owner["daemon_id"], owner["runtime_id"]
    );
    let source = request.witness["workspace"]["handle"].as_str();
    let mut keys = Vec::new();
    if !matches!(
        request.kind,
        IntegrationOperationKind::Rebase | IntegrationOperationKind::Check
    ) || source.is_none()
    {
        keys.push(format!("{prefix}:location:{}", owner["location_id"]));
    }
    if let Some(source) = source {
        keys.push(format!("{prefix}:workspace:{source}"));
    }
    keys.sort();
    keys.dedup();
    let locks = {
        let mut registry = LOCKS
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        registry.retain(|_, lock| lock.strong_count() > 0);
        keys.into_iter()
            .map(|key| {
                if let Some(lock) = registry.get(&key).and_then(Weak::upgrade) {
                    return lock;
                }
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                registry.insert(key, Arc::downgrade(&lock));
                lock
            })
            .collect::<Vec<_>>()
    };
    let mut guards = Vec::new();
    for lock in locks {
        guards.push(lock.lock_owned().await);
    }
    guards
}

impl IntegrationEffectGuard {
    pub fn request(&self) -> &IntegrationEffectRequest {
        &self.request
    }

    /// The last durable fence check, immediately before owner-side Git probes.
    /// A refused request is settled as not performed in the same transaction.
    pub async fn start(&mut self) -> Result<Option<IntegrationEffectRefusal>> {
        let mut tx = begin_immediate(&self.pool).await?;
        if let Some(refusal) = verify(&mut tx, &self.request.fence).await? {
            let receipt = make_receipt(
                self.request.clone(),
                serde_json::json!({"kind":"not_performed","reason":format!("{refusal:?}")}),
                IntegrationOperationState::Failed,
            );
            record_in_tx(&mut tx, &receipt).await?;
            tx.commit().await?;
            return Ok(Some(refusal));
        }
        sqlx::query("UPDATE integration_attempt SET effect_intent_json=json_set(effect_intent_json,'$.started',json('true')),revision=revision+1,updated_at=? WHERE id=?")
            .bind(now_rfc3339()).bind(&self.request.fence.attempt_id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(None)
    }

    pub async fn record_owner_receipt(self, receipt: IntegrationEffectReceipt) -> Result<()> {
        if receipt.request != self.request {
            return Err(DbError::IdempotencyConflict);
        }
        let mut tx = begin_immediate(&self.pool).await?;
        record_in_tx(&mut tx, &receipt).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn record(
        self,
        result: Value,
        operation_state: IntegrationOperationState,
    ) -> Result<IntegrationEffectReceipt> {
        let receipt = make_receipt(self.request, result, operation_state);
        let mut tx = begin_immediate(&self.pool).await?;
        record_in_tx(&mut tx, &receipt).await?;
        tx.commit().await?;
        Ok(receipt)
    }
}

/// Today's Task-step recorders are bound to the step lease, not a queue lease.
pub fn is_task_step(request: &IntegrationEffectRequest) -> bool {
    request.fence.lease_owner.starts_with("task-step:")
}

/// Terminal settlement of a Task-step intent whose guard is gone before its
/// receipt. It records that this owner made no claim about the effect; the
/// Task step's own recovery decides whether to repeat it.
pub fn superseded_result() -> Value {
    serde_json::json!({"kind":"infrastructure","message":"task step ended before its effect receipt; step recovery decides whether to repeat","head_sha":null,"rebase_in_progress":true})
}

fn make_receipt(
    request: IntegrationEffectRequest,
    result: Value,
    operation_state: IntegrationOperationState,
) -> IntegrationEffectReceipt {
    IntegrationEffectReceipt {
        request,
        result,
        operation_state,
        recorded_at: now_rfc3339(),
    }
}

async fn record_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    receipt: &IntegrationEffectReceipt,
) -> Result<()> {
    if !matches!(
        receipt.operation_state,
        IntegrationOperationState::Succeeded
            | IntegrationOperationState::Failed
            | IntegrationOperationState::Uncertain
    ) {
        return Err(DbError::Check(
            "receipt must describe a settled or uncertain effect".into(),
        ));
    }
    let payload = serde_json::to_string(receipt).map_err(|e| DbError::Check(e.to_string()))?;
    if payload.len() > 131072 {
        return Err(DbError::Check("effect receipt exceeds bound".into()));
    }
    let intent: Option<String> =
        sqlx::query_scalar("SELECT effect_intent_json FROM integration_attempt WHERE id=?")
            .bind(&receipt.request.fence.attempt_id)
            .fetch_one(&mut **tx)
            .await?;
    let intent = intent.map(parse_json).transpose()?;
    if intent.as_ref().and_then(|i| i.get("request"))
        != Some(&serde_json::to_value(&receipt.request).map_err(|e| DbError::Check(e.to_string()))?)
    {
        return Err(DbError::VersionConflict);
    }
    // Retain the original effect's receipt even after observation ownership has
    // transferred. The frozen intent, not the new slot generation, is its key.
    let mut receipts = receipts_in_tx(tx, &receipt.request.fence.attempt_id).await?;
    if let Some(existing) = receipts.iter_mut().find(|existing| {
        existing.request.fence == receipt.request.fence
            && existing.request.kind == receipt.request.kind
    }) {
        if existing.request != receipt.request {
            return Err(DbError::IdempotencyConflict);
        }
        if existing.operation_state != IntegrationOperationState::Uncertain {
            return if existing == receipt {
                Ok(())
            } else {
                Err(DbError::IdempotencyConflict)
            };
        }
        *existing = receipt.clone();
    } else {
        receipts.push(receipt.clone());
    }
    let receipts = serde_json::to_string(&receipts).map_err(|e| DbError::Check(e.to_string()))?;
    sqlx::query("UPDATE integration_attempt SET effect_receipts_json=?,effect_intent_json=CASE WHEN ?='uncertain' THEN effect_intent_json ELSE NULL END,current_operation_state=?,updated_at=?,revision=revision+1 WHERE id=?")
        .bind(receipts).bind(receipt.operation_state.to_string()).bind(receipt.operation_state.to_string()).bind(&receipt.recorded_at).bind(&receipt.request.fence.attempt_id).execute(&mut **tx).await?;
    Ok(())
}

/// Permanent infrastructure loss settles attempts in the remover's transaction.
/// Quarantine is retained; settlement cannot grant a fresh effect or Task action.
pub(crate) async fn settle_removed_integration_owner(
    tx: &mut Transaction<'_, Sqlite>,
    location: Option<&str>,
    daemon: Option<&str>,
) -> Result<()> {
    let rows: Vec<String> = sqlx::query_scalar("SELECT effect_intent_json FROM integration_attempt WHERE effect_intent_json IS NOT NULL AND ((? IS NOT NULL AND json_extract(effect_intent_json,'$.request.fence.target_owner.location_id')=?) OR (? IS NOT NULL AND json_extract(effect_intent_json,'$.request.fence.target_owner.daemon_id')=?))")
        .bind(location).bind(location).bind(daemon).bind(daemon).fetch_all(&mut **tx).await?;
    let message = if daemon.is_some() {
        "integration owner machine removed"
    } else {
        "integration owner location removed"
    };
    for raw in rows {
        let intent = parse_json(raw)?;
        let request: IntegrationEffectRequest = serde_json::from_value(intent["request"].clone())
            .map_err(|e| DbError::Check(e.to_string()))?;
        let receipt = make_receipt(
            request,
            serde_json::json!({"kind":"infrastructure","message":message,"head_sha":null,"rebase_in_progress":true}),
            IntegrationOperationState::Failed,
        );
        record_in_tx(tx, &receipt).await?;
        sqlx::query("UPDATE integration_attempt SET state=CASE WHEN state='quarantined' OR EXISTS(SELECT 1 FROM integration_queue q WHERE q.id=integration_attempt.queue_id AND q.state='quarantined') THEN 'quarantined' ELSE 'parked' END,failure_kind='infrastructure',failure_message=?,revision=revision+1,updated_at=? WHERE id=?")
            .bind(message).bind(now_rfc3339()).bind(&receipt.request.fence.attempt_id).execute(&mut **tx).await?;
    }
    // Location deletion's existing queue update owns its single revision
    // increment and target-unconfigured projection in this transaction.
    if daemon.is_some() {
        sqlx::query("UPDATE integration_queue SET state=CASE WHEN state='quarantined' THEN state ELSE 'suspended' END,lease_owner=NULL,lease_until=NULL,last_error_kind='infrastructure',last_error=?,revision=revision+1,updated_at=? WHERE state<>'closed' AND ((? IS NOT NULL AND target_location_id=?) OR (? IS NOT NULL AND json_extract(target_owner_json,'$.daemon_id')=?))")
        .bind(message).bind(now_rfc3339()).bind(location).bind(location).bind(daemon).bind(daemon).execute(&mut **tx).await?;
    }
    Ok(())
}

async fn receipts_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    id: &str,
) -> Result<Vec<IntegrationEffectReceipt>> {
    let raw: String =
        sqlx::query_scalar("SELECT effect_receipts_json FROM integration_attempt WHERE id=?")
            .bind(id)
            .fetch_one(&mut **tx)
            .await?;
    serde_json::from_str(&raw).map_err(|e| DbError::Check(format!("invalid effect receipts: {e}")))
}

async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    fence: &IntegrationOwnerFence,
) -> Result<Option<IntegrationEffectRefusal>> {
    if let Some(step_id) = fence.lease_owner.strip_prefix("task-step:") {
        if !crate::task_writer::owns_step(step_id) {
            return Ok(Some(IntegrationEffectRefusal::StaleFence));
        }
        let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM integration_attempt a JOIN task_step s ON s.task_id=a.task_ref JOIN task t ON t.id=s.task_id JOIN workspace_placement p ON p.task_id=a.task_ref AND (a.workspace_ref IS NULL OR p.workspace_id=a.workspace_ref) JOIN repo_location l ON l.id=p.repo_location_id WHERE a.id=? AND a.queue_id=? AND a.current=1 AND s.id=? AND s.status='claimed' AND s.attempts=? AND (s.entry_fenced=0 OR (t.status=s.expected_status AND t.status_epoch=s.expected_epoch)) AND p.state='ready' AND l.status='ready' AND l.id=? AND l.version=?)")
            .bind(&fence.attempt_id).bind(&fence.queue_id).bind(step_id).bind(fence.generation).bind(fence.target_owner["location_id"].as_str()).bind(fence.target_owner["generation"].as_i64()).fetch_one(&mut **tx).await?;
        return Ok((!valid).then_some(IntegrationEffectRefusal::StaleFence));
    }
    let q = queue_in_tx(tx, &fence.queue_id).await?;
    let a = attempt_in_tx(tx, &fence.attempt_id).await?;
    if q.lease_until
        .as_deref()
        .map(integration_time)
        .transpose()?
        .is_none_or(|deadline| deadline <= chrono::Utc::now())
        || !matches!(
            q.state,
            IntegrationQueueState::Open | IntegrationQueueState::Quarantined
        )
        || q.head_attempt_id.as_deref() != Some(&fence.attempt_id)
        || a.queue_id.as_deref() != Some(&fence.queue_id)
        || !a.current
        || q.fence_generation != fence.generation
        || a.slot_generation != fence.generation
        || q.lease_owner.as_deref() != Some(&fence.lease_owner)
    {
        return Ok(Some(IntegrationEffectRefusal::StaleFence));
    }
    let stored: Option<String> =
        sqlx::query_scalar("SELECT owner_fence_json FROM integration_attempt WHERE id=?")
            .bind(&fence.attempt_id)
            .fetch_one(&mut **tx)
            .await?;
    if stored
        .as_deref()
        .map(serde_json::from_str::<IntegrationOwnerFence>)
        .transpose()
        .map_err(|e| DbError::Check(e.to_string()))?
        .as_ref()
        != Some(fence)
    {
        return Ok(Some(IntegrationEffectRefusal::ForeignOwner));
    }
    let target = resolve_integration_target_in_tx(tx, &q.repo_id).await?;
    if target.failure.is_some() || target.owner.as_ref() != Some(&fence.target_owner) {
        return Ok(Some(IntegrationEffectRefusal::ForeignOwner));
    }
    Ok(None)
}

/// The original owner may reconcile after its lease expired or was replaced.
/// This guard grants observation/receipt authority only, never effect authority.
pub struct IntegrationReconciliationGuard {
    pool: sqlx::SqlitePool,
    pub request: IntegrationEffectRequest,
    pub started: bool,
    _owner_locks: Vec<tokio::sync::OwnedMutexGuard<()>>,
}
impl IntegrationReconciliationGuard {
    pub async fn record(
        self,
        result: Value,
        state: IntegrationOperationState,
    ) -> Result<IntegrationEffectReceipt> {
        let receipt = make_receipt(self.request, result, state);
        let mut tx = begin_immediate(&self.pool).await?;
        record_in_tx(&mut tx, &receipt).await?;
        tx.commit().await?;
        Ok(receipt)
    }
    pub async fn record_owner_receipt(self, receipt: IntegrationEffectReceipt) -> Result<()> {
        if receipt.request != self.request {
            return Err(DbError::IdempotencyConflict);
        }
        let mut tx = begin_immediate(&self.pool).await?;
        record_in_tx(&mut tx, &receipt).await?;
        tx.commit().await?;
        Ok(())
    }
}

impl SqliteDb {
    /// Same physical checkout lock as merge/rebase, without admitting an
    /// integration effect or manufacturing integration authority.
    pub async fn lock_server_check_checkout(&self, path: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let request = IntegrationEffectRequest {
            fence: IntegrationOwnerFence {
                queue_id: String::new(),
                attempt_id: String::new(),
                generation: 0,
                lease_owner: String::new(),
                target_owner: serde_json::json!({"owner_kind":"server","daemon_id":null,"runtime_id":null}),
            },
            kind: IntegrationOperationKind::Check,
            witness: serde_json::json!({"workspace":{"handle":path}}),
        };
        lock_owner(&request)
            .await
            .pop()
            .expect("one workspace owner lock")
    }

    /// Bind the existing Task step to its shadow attempt without claiming or
    /// activating the queue. Placement follows today's Task, including when
    /// its location differs from the configured future queue target.
    pub async fn task_step_integration_request(
        &self,
        workspace_id: &str,
        kind: IntegrationOperationKind,
        witness: Value,
    ) -> Result<Option<IntegrationEffectRequest>> {
        let Some(step) = crate::task_writer::current_task_step() else {
            return Ok(None);
        };
        let row = sqlx::query("SELECT a.id,a.queue_id,l.id AS location_id,l.owner_kind,l.daemon_id,l.runtime_id,l.version FROM integration_attempt a JOIN workspace_placement p ON p.workspace_id=? AND p.task_id=a.task_ref JOIN repo_location l ON l.id=p.repo_location_id WHERE a.current=1 AND a.task_ref=? AND a.queue_id IS NOT NULL")
            .bind(workspace_id).bind(&step.task_id).fetch_optional(self.pool()).await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let fence = IntegrationOwnerFence {
            queue_id: row.try_get("queue_id")?,
            attempt_id: row.try_get("id")?,
            generation: step.attempts,
            lease_owner: format!("task-step:{}", step.id),
            target_owner: serde_json::json!({"location_id":row.try_get::<String,_>("location_id")?,"owner_kind":row.try_get::<String,_>("owner_kind")?,"daemon_id":row.try_get::<Option<String>,_>("daemon_id")?,"runtime_id":row.try_get::<Option<String>,_>("runtime_id")?,"generation":row.try_get::<i64,_>("version")?}),
        };
        Ok(Some(IntegrationEffectRequest {
            fence,
            kind,
            witness,
        }))
    }

    /// Bounded restart/reconnect input, indexed by the retained owner witness.
    pub async fn outstanding_integration_effects(
        &self,
        owner_kind: &str,
        daemon_id: Option<&str>,
    ) -> Result<Vec<IntegrationEffectRequest>> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT effect_intent_json FROM integration_attempt WHERE effect_intent_json IS NOT NULL AND json_extract(effect_intent_json,'$.request.fence.target_owner.owner_kind')=? AND (? IS NULL OR json_extract(effect_intent_json,'$.request.fence.target_owner.daemon_id')=?) ORDER BY updated_at,id LIMIT 128")
            .bind(owner_kind).bind(daemon_id).bind(daemon_id).fetch_all(self.pool()).await?;
        rows.into_iter()
            .map(|raw| {
                let intent = parse_json(raw)?;
                serde_json::from_value(intent["request"].clone())
                    .map_err(|e| DbError::Check(e.to_string()))
            })
            .collect()
    }

    pub async fn lock_integration_reconciliation(
        &self,
        request: &IntegrationEffectRequest,
    ) -> Result<Option<IntegrationReconciliationGuard>> {
        let locks = lock_owner(request).await;
        let raw: Option<String> =
            sqlx::query_scalar("SELECT effect_intent_json FROM integration_attempt WHERE id=?")
                .bind(&request.fence.attempt_id)
                .fetch_optional(self.pool())
                .await?
                .flatten();
        let Some(raw) = raw else {
            return Ok(None);
        };
        let intent = parse_json(raw)?;
        if intent["request"]
            != serde_json::to_value(request).map_err(|e| DbError::Check(e.to_string()))?
        {
            return Ok(None);
        }
        Ok(Some(IntegrationReconciliationGuard {
            pool: self.pool().clone(),
            request: request.clone(),
            started: intent["started"] != false,
            _owner_locks: locks,
        }))
    }

    pub async fn integration_owner_fence(
        &self,
        attempt_id: &str,
    ) -> Result<Option<IntegrationOwnerFence>> {
        let raw: Option<String> =
            sqlx::query_scalar("SELECT owner_fence_json FROM integration_attempt WHERE id=?")
                .bind(attempt_id)
                .fetch_one(self.pool())
                .await?;
        raw.map(|s| serde_json::from_str(&s).map_err(|e| DbError::Check(e.to_string())))
            .transpose()
    }

    /// Lookup precedes every send/effect, including after reconnect. Historical
    /// receipts remain readable under their original fence after a takeover.
    pub async fn integration_effect_receipt(
        &self,
        request: &IntegrationEffectRequest,
    ) -> Result<Option<IntegrationEffectReceipt>> {
        let raw: String =
            sqlx::query_scalar("SELECT effect_receipts_json FROM integration_attempt WHERE id=?")
                .bind(&request.fence.attempt_id)
                .fetch_one(self.pool())
                .await?;
        let receipts: Vec<IntegrationEffectReceipt> =
            serde_json::from_str(&raw).map_err(|e| DbError::Check(e.to_string()))?;
        Ok(receipts
            .into_iter()
            .find(|r| r.request.fence == request.fence && r.request.kind == request.kind))
    }

    pub async fn begin_integration_effect(
        &self,
        request: IntegrationEffectRequest,
    ) -> Result<IntegrationEffectAdmission> {
        let json = serde_json::to_string(&request).map_err(|e| DbError::Check(e.to_string()))?;
        if json.len() > 64512 || !request.witness.is_object() {
            return Err(DbError::Check(
                "effect intent exceeds bound or has no witness".into(),
            ));
        }
        let locks = lock_owner(&request).await;
        let mut tx = begin_immediate(self.pool()).await?;
        if let Some(receipt) = receipts_in_tx(&mut tx, &request.fence.attempt_id)
            .await?
            .into_iter()
            .find(|r| r.request.fence == request.fence && r.request.kind == request.kind)
        {
            return Ok(if receipt.request == request {
                IntegrationEffectAdmission::Replay(receipt)
            } else {
                IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::RequestConflict)
            });
        }
        if let Some(refusal) = verify(&mut tx, &request.fence).await? {
            return Ok(IntegrationEffectAdmission::Refused(refusal));
        }
        let size: i64 = sqlx::query_scalar(
            "SELECT length(CAST(effect_receipts_json AS BLOB)) FROM integration_attempt WHERE id=?",
        )
        .bind(&request.fence.attempt_id)
        .fetch_one(&mut *tx)
        .await?;
        // Reserve the array entry delimiter as well as the bounded receipt.
        if size > 1048576 - 131072 - 1 {
            return Err(DbError::Check("attempt receipt capacity exhausted".into()));
        }
        let existing: Option<String> =
            sqlx::query_scalar("SELECT effect_intent_json FROM integration_attempt WHERE id=?")
                .bind(&request.fence.attempt_id)
                .fetch_one(&mut *tx)
                .await?;
        if let Some(existing) = existing {
            let intent = parse_json(existing)?;
            let original: IntegrationEffectRequest =
                serde_json::from_value(intent["request"].clone())
                    .map_err(|e| DbError::Check(e.to_string()))?;
            let started = intent["started"] != false;
            let task_step = is_task_step(&original);
            // A queue lease's started intent is an unknown effect: only its
            // owner's receipt may settle it. A Task-step intent is different:
            // this caller holds the owner lock that the step's guard held, so
            // that guard is gone, and today's step recovery (durable hook
            // effects, idempotent merge, interrupted-rebase recovery) decides
            // whether the effect is repeated. It settles here, never blocks.
            if started && !task_step {
                return Ok(IntegrationEffectAdmission::Refused(
                    IntegrationEffectRefusal::ReconciliationRequired,
                ));
            }
            let same_request = original == request;
            let same_key = original.fence == request.fence && original.kind == request.kind;
            let receipt = make_receipt(
                original,
                if started {
                    superseded_result()
                } else {
                    serde_json::json!({"kind":"not_performed"})
                },
                IntegrationOperationState::Failed,
            );
            record_in_tx(&mut tx, &receipt).await?;
            if same_key || !task_step {
                tx.commit().await?;
                return Ok(if same_request {
                    IntegrationEffectAdmission::Replay(receipt)
                } else {
                    IntegrationEffectAdmission::Refused(if same_key {
                        IntegrationEffectRefusal::RequestConflict
                    } else {
                        IntegrationEffectRefusal::ReconciliationRequired
                    })
                });
            }
        }
        let other_intents: Vec<String> = sqlx::query_scalar("SELECT effect_intent_json FROM integration_attempt WHERE id<>? AND effect_intent_json IS NOT NULL AND json_extract(effect_intent_json,'$.request.fence.target_owner.location_id')=? AND json_extract(effect_intent_json,'$.request.fence.target_owner.owner_kind')=?")
            .bind(&request.fence.attempt_id).bind(request.fence.target_owner["location_id"].as_str()).bind(request.fence.target_owner["owner_kind"].as_str()).fetch_all(&mut *tx).await?;
        for raw in other_intents {
            let intent = parse_json(raw)?;
            let original: IntegrationEffectRequest =
                serde_json::from_value(intent["request"].clone())
                    .map_err(|e| DbError::Check(e.to_string()))?;
            // Only an intent whose owner lock this caller holds can be judged
            // here. A rebase or check locks its own workspace alone, so it
            // neither blocks nor is blocked by another workspace's effect
            // (two Tasks, one merging and one rebasing, is the normal case).
            if (matches!(
                original.kind,
                IntegrationOperationKind::Rebase | IntegrationOperationKind::Check
            ) || matches!(
                request.kind,
                IntegrationOperationKind::Rebase | IntegrationOperationKind::Check
            )) && original.witness["workspace"]["handle"]
                != request.witness["workspace"]["handle"]
            {
                continue;
            }
            let started = intent["started"] != false;
            if started && !is_task_step(&original) {
                return Ok(IntegrationEffectAdmission::Refused(
                    IntegrationEffectRefusal::ReconciliationRequired,
                ));
            }
            record_in_tx(
                &mut tx,
                &make_receipt(
                    original,
                    if started {
                        superseded_result()
                    } else {
                        serde_json::json!({"kind":"not_performed"})
                    },
                    IntegrationOperationState::Failed,
                ),
            )
            .await?;
        }
        // Keep the digest in the frozen intent to diagnose reused input keys.
        let digest = Sha256::digest(json.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let intent = serde_json::json!({"request":request,"digest":digest,"started":false});
        sqlx::query("UPDATE integration_attempt SET effect_intent_json=?,operation_kind=?,current_operation_state='running',revision=revision+1,updated_at=? WHERE id=?")
            .bind(intent.to_string()).bind(request.kind.to_string()).bind(now_rfc3339()).bind(&request.fence.attempt_id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(IntegrationEffectAdmission::Started(
            IntegrationEffectGuard {
                pool: self.pool().clone(),
                request,
                _owner_locks: locks,
            },
        ))
    }
}

impl IntegrationEffectGuard {
    /// Resolve owner-local paths from the retained placement under this guard.
    /// The requested witness cannot substitute another workspace or checkout.
    pub async fn server_workspace_paths(
        &mut self,
    ) -> Result<Option<(std::path::PathBuf, std::path::PathBuf)>> {
        if self.request.fence.target_owner["owner_kind"] != "server" {
            return Ok(None);
        }
        let w = &self.request.witness["workspace"];
        let row=sqlx::query("SELECT p.workspace_id,p.generation,p.owner_kind,p.workspace_handle,w.worktree_path,l.path FROM workspace_placement p JOIN workspace w ON w.id=p.workspace_id JOIN integration_attempt a ON a.task_ref=p.task_id JOIN integration_queue q ON q.id=a.queue_id JOIN repo_location l ON l.id=? WHERE p.id=? AND a.id=? AND p.state='ready' AND w.repo_id=q.repo_id AND q.target_branch=? AND (a.workspace_ref IS NULL OR a.workspace_ref=p.workspace_id) AND (a.placement_ref IS NULL OR a.placement_ref=p.id)")
            .bind(self.request.fence.target_owner["location_id"].as_str()).bind(w["placement_id"].as_str()).bind(&self.request.fence.attempt_id).bind(self.request.witness["target_branch"].as_str()).fetch_optional(&self.pool).await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let workspace_id: String = row.try_get("workspace_id")?;
        let generation: i64 = row.try_get("generation")?;
        let owner: String = row.try_get("owner_kind")?;
        let handle: Option<String> = row.try_get("workspace_handle")?;
        if w["workspace_id"].as_str() != Some(&workspace_id)
            || w["generation"].as_i64() != Some(generation)
            || owner != "server"
            || w["owner"]["kind"] != "server"
            || w["handle"].as_str() != handle.as_deref()
        {
            return Ok(None);
        }
        Ok(Some((
            std::path::PathBuf::from(row.try_get::<String, _>("worktree_path")?),
            std::path::PathBuf::from(row.try_get::<String, _>("path")?),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::integration_queue::tests::{admission, fixture};
    async fn claimed() -> (SqliteDb, IntegrationOwnerFence) {
        let db = fixture().await;
        let q = db
            .create_or_get_integration_queue("r", "main")
            .await
            .unwrap();
        let a = db
            .admit_integration_attempt(admission(&q, "a", "fence"))
            .await
            .unwrap();
        let q = db.integration_queue(&q.id).await.unwrap().unwrap();
        db.claim_integration_queue(
            &q.id,
            q.revision,
            "owner",
            "2026-10-08T00:00:00Z",
            "2099-01-01T00:00:00Z",
        )
        .await
        .unwrap();
        let f = db.integration_owner_fence(&a.id).await.unwrap().unwrap();
        (db, f)
    }
    fn request(f: IntegrationOwnerFence) -> IntegrationEffectRequest {
        IntegrationEffectRequest {
            fence: f,
            kind: IntegrationOperationKind::Rebase,
            witness: serde_json::json!({"workspace":"frozen","head":"head","target":"target"}),
        }
    }
    #[tokio::test]
    async fn stale_and_foreign_fences_are_typed_refusals_before_intent() {
        let (db, f) = claimed().await;
        let mut stale = f.clone();
        stale.generation += 1;
        assert!(matches!(
            db.begin_integration_effect(request(stale)).await.unwrap(),
            IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::StaleFence)
        ));
        let mut foreign = f.clone();
        foreign.target_owner["generation"] = serde_json::json!(99);
        assert!(matches!(
            db.begin_integration_effect(request(foreign)).await.unwrap(),
            IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::ForeignOwner)
        ));
        let raw: Option<String> =
            sqlx::query_scalar("SELECT effect_intent_json FROM integration_attempt WHERE id=?")
                .bind(f.attempt_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert!(raw.is_none());
    }
    #[tokio::test]
    async fn completed_duplicate_replays_and_reused_key_with_changed_input_is_refused() {
        let (db, f) = claimed().await;
        let r = request(f);
        let IntegrationEffectAdmission::Started(g) =
            db.begin_integration_effect(r.clone()).await.unwrap()
        else {
            panic!("not admitted")
        };
        let receipt = g
            .record(
                serde_json::json!({"kind":"rebased"}),
                IntegrationOperationState::Succeeded,
            )
            .await
            .unwrap();
        let IntegrationEffectAdmission::Replay(replayed) =
            db.begin_integration_effect(r.clone()).await.unwrap()
        else {
            panic!("not replayed")
        };
        assert_eq!(receipt, replayed);
        let mut changed = r.clone();
        changed.witness["head"] = serde_json::json!("different");
        assert!(matches!(
            db.begin_integration_effect(changed).await.unwrap(),
            IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::RequestConflict)
        ));
        assert_eq!(
            db.integration_effect_receipt(&r).await.unwrap(),
            Some(receipt)
        );
        let n: i64 = sqlx::query_scalar(
            "SELECT json_array_length(effect_receipts_json) FROM integration_attempt WHERE id=?",
        )
        .bind(&r.fence.attempt_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(n, 1);
    }
    #[tokio::test]
    async fn dropped_owner_guard_keeps_durable_intent_and_never_repeats_an_unknown_effect() {
        let (db, f) = claimed().await;
        let r = request(f);
        let IntegrationEffectAdmission::Started(mut g) =
            db.begin_integration_effect(r.clone()).await.unwrap()
        else {
            panic!("not admitted")
        };
        assert!(g.start().await.unwrap().is_none());
        drop(g);
        assert!(db.integration_effect_receipt(&r).await.unwrap().is_none());
        assert!(matches!(
            db.begin_integration_effect(r.clone()).await.unwrap(),
            IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::ReconciliationRequired)
        ));
        let mut attempt = db
            .integration_attempt(&r.fence.attempt_id)
            .await
            .unwrap()
            .unwrap();
        assert!(attempt.effect_intent_json.is_some());
        attempt.state = IntegrationAttemptState::Parked;
        attempt.current_operation_state = Some(IntegrationOperationState::Succeeded);
        assert!(matches!(
            db.transition_integration_attempt(attempt).await,
            Err(DbError::InvalidTransition)
        ));
    }
    #[tokio::test]
    async fn task_step_binds_unobserved_shadow_workspace_without_claiming_queue() {
        use crate::TaskStepRepo;
        let db = fixture().await;
        crate::integration_queue::tests::seed_delivery(&db, "a").await;
        let path: String = sqlx::query_scalar("SELECT worktree_path FROM workspace WHERE id='w-a'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        let now = now_rfc3339();
        sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES('pl','w-a','a','server','l',?,1,'ready','scheduler','{}',?,?)").bind(&path).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        let q = db
            .create_or_get_integration_queue("r", "main")
            .await
            .unwrap();
        let attempt = db
            .admit_integration_attempt(admission(&q, "a", "unobserved-workspace"))
            .await
            .unwrap();
        assert!(attempt.workspace_ref.is_none());
        db.enqueue_step(&crate::EnqueueTaskStep {
            id: "nullable-step".into(),
            task_id: "a".into(),
            kind: "hooks".into(),
            payload_json: "{}".into(),
            causation_step_id: None,
            causation_key: "nullable-step".into(),
            chain_id: "nullable-chain".into(),
            chain_position: 1,
            expected_status: "merging".into(),
            expected_version: 1,
            expected_epoch: Some(0),
            lane: "long".into(),
            available_at: now,
        })
        .await
        .unwrap();
        let step = db
            .claim_step("owner", Some("a"), "2099-01-01T00:00:00Z")
            .await
            .unwrap()
            .unwrap();
        crate::task_writer::in_task_step(step, async {
            let witness = serde_json::json!({"workspace":{"workspace_id":"w-a","placement_id":"pl","generation":1,"owner":{"kind":"server"},"handle":path},"target_branch":"main","expected_head_sha":"head","expected_target_sha":"target"});
            let request = db.task_step_integration_request("w-a", IntegrationOperationKind::Merge, witness).await.unwrap().unwrap();
            let IntegrationEffectAdmission::Started(mut guard) = db.begin_integration_effect(request).await.unwrap() else { panic!("not admitted"); };
            assert!(guard.start().await.unwrap().is_none());
            assert!(guard.server_workspace_paths().await.unwrap().is_some());
            guard.record(serde_json::json!({"kind":"not_performed"}), IntegrationOperationState::Failed).await.unwrap();
        }).await;
        assert!(db
            .integration_queue(&q.id)
            .await
            .unwrap()
            .unwrap()
            .head_attempt_id
            .is_none());
    }

    /// One Task-step fixture: a ready server placement, a shadow attempt and
    /// a claimable merging step for `task`.
    async fn task_step_fixture(db: &SqliteDb, task: &str) {
        use crate::TaskStepRepo;
        crate::integration_queue::tests::seed_delivery(db, task).await;
        let now = now_rfc3339();
        sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES(?,?,?,'server','l',?,1,'ready','scheduler','{}',?,?)").bind(format!("pl-{task}")).bind(format!("w-{task}")).bind(task).bind(format!("tree-{task}")).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        let q = db
            .create_or_get_integration_queue("r", "main")
            .await
            .unwrap();
        db.admit_integration_attempt(admission(&q, task, &format!("shadow-{task}")))
            .await
            .unwrap();
        db.enqueue_step(&crate::EnqueueTaskStep {
            id: format!("step-{task}"),
            task_id: task.into(),
            kind: "hooks".into(),
            payload_json: "{}".into(),
            causation_step_id: None,
            causation_key: format!("step-{task}"),
            chain_id: format!("chain-{task}"),
            chain_position: 1,
            expected_status: "merging".into(),
            expected_version: 1,
            expected_epoch: Some(0),
            lane: "long".into(),
            available_at: now,
        })
        .await
        .unwrap();
    }
    fn task_step_witness(task: &str, head: &str) -> Value {
        serde_json::json!({"workspace":{"workspace_id":format!("w-{task}"),"placement_id":format!("pl-{task}"),"generation":1,"owner":{"kind":"server"},"handle":format!("tree-{task}")},"target_branch":"main","expected_head_sha":head,"expected_target_sha":"target"})
    }
    async fn claim(db: &SqliteDb, task: &str) -> crate::TaskStep {
        use crate::TaskStepRepo;
        sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00Z' WHERE task_id=? AND status='claimed'")
            .bind(task)
            .execute(db.pool())
            .await
            .unwrap();
        db.claim_step("owner", Some(task), "2099-01-01T00:00:00Z")
            .await
            .unwrap()
            .unwrap()
    }

    /// The wedge this guards: a Task-step merge that started and never wrote a
    /// receipt (crash, dropped future, failed receipt write) used to leave an
    /// intent that refused the Task's next claim and every other Task's merge
    /// on the same checkout with `ReconciliationRequired`, forever.
    #[tokio::test]
    async fn orphaned_task_step_intent_never_refuses_a_later_claim_or_another_task() {
        let db = fixture().await;
        task_step_fixture(&db, "a").await;
        task_step_fixture(&db, "b").await;
        let first = claim(&db, "a").await;
        let orphan = crate::task_writer::in_task_step(first, async {
            let request = db
                .task_step_integration_request(
                    "w-a",
                    IntegrationOperationKind::Merge,
                    task_step_witness("a", "head-1"),
                )
                .await
                .unwrap()
                .unwrap();
            let IntegrationEffectAdmission::Started(mut guard) =
                db.begin_integration_effect(request.clone()).await.unwrap()
            else {
                panic!("first claim not admitted");
            };
            assert!(guard.start().await.unwrap().is_none());
            // The step dies between the started intent and its receipt.
            drop(guard);
            request
        })
        .await;

        // Another Task merges into the same checkout: admitted, not refused.
        let other = claim(&db, "b").await;
        crate::task_writer::in_task_step(other, async {
            let request = db
                .task_step_integration_request(
                    "w-b",
                    IntegrationOperationKind::Merge,
                    task_step_witness("b", "head-b"),
                )
                .await
                .unwrap()
                .unwrap();
            let IntegrationEffectAdmission::Started(mut guard) =
                db.begin_integration_effect(request).await.unwrap()
            else {
                panic!("another Task's merge was refused by an orphaned intent");
            };
            assert!(guard.start().await.unwrap().is_none());
            guard
                .record(
                    serde_json::json!({"kind":"completed"}),
                    IntegrationOperationState::Succeeded,
                )
                .await
                .unwrap();
        })
        .await;
        let settled = db
            .integration_effect_receipt(&orphan)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(settled.operation_state, IntegrationOperationState::Failed);
        assert_eq!(settled.result["kind"], "infrastructure");
        let attempt = db
            .integration_attempt(&orphan.fence.attempt_id)
            .await
            .unwrap()
            .unwrap();
        assert!(attempt.effect_intent_json.is_none());

        // The same Task's next claim (a new step attempt) is admitted too,
        // both after a settled orphan and directly over a fresh one.
        for head in ["head-2", "head-3"] {
            let next = claim(&db, "a").await;
            crate::task_writer::in_task_step(next, async {
                let request = db
                    .task_step_integration_request(
                        "w-a",
                        IntegrationOperationKind::Merge,
                        task_step_witness("a", head),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                let IntegrationEffectAdmission::Started(mut guard) =
                    db.begin_integration_effect(request).await.unwrap()
                else {
                    panic!("the Task's next claim was refused by its own orphaned intent");
                };
                assert!(guard.start().await.unwrap().is_none());
            })
            .await;
        }
    }

    /// Two Tasks on one checkout, one merging and one rebasing after a target
    /// move, is the normal contended case. A rebase locks only its workspace,
    /// so another workspace's in-flight merge must not refuse it.
    #[tokio::test]
    async fn in_flight_merge_does_not_refuse_another_workspaces_rebase() {
        let db = fixture().await;
        task_step_fixture(&db, "a").await;
        task_step_fixture(&db, "b").await;
        let merging = claim(&db, "a").await;
        let held = crate::task_writer::in_task_step(merging, async {
            let request = db
                .task_step_integration_request(
                    "w-a",
                    IntegrationOperationKind::Merge,
                    task_step_witness("a", "head"),
                )
                .await
                .unwrap()
                .unwrap();
            let IntegrationEffectAdmission::Started(mut guard) =
                db.begin_integration_effect(request).await.unwrap()
            else {
                panic!("merge not admitted");
            };
            assert!(guard.start().await.unwrap().is_none());
            guard
        })
        .await;
        let rebasing = claim(&db, "b").await;
        crate::task_writer::in_task_step(rebasing, async {
            let request = db
                .task_step_integration_request(
                    "w-b",
                    IntegrationOperationKind::Rebase,
                    task_step_witness("b", "head-b"),
                )
                .await
                .unwrap()
                .unwrap();
            let admission = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                db.begin_integration_effect(request),
            )
            .await
            .expect("a rebase waited on another workspace's merge")
            .unwrap();
            assert!(matches!(admission, IntegrationEffectAdmission::Started(_)));
        })
        .await;
        // The in-flight merge was neither settled nor disturbed.
        held.record(
            serde_json::json!({"kind":"completed"}),
            IntegrationOperationState::Succeeded,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn intent_without_effect_settles_not_performed() {
        let (db, f) = claimed().await;
        let r = request(f);
        let IntegrationEffectAdmission::Started(g) =
            db.begin_integration_effect(r.clone()).await.unwrap()
        else {
            panic!("not admitted")
        };
        drop(g);
        let IntegrationEffectAdmission::Replay(receipt) =
            db.begin_integration_effect(r.clone()).await.unwrap()
        else {
            panic!("not settled")
        };
        assert_eq!(receipt.result["kind"], "not_performed");
        assert_eq!(receipt.operation_state, IntegrationOperationState::Failed);
        assert!(db
            .integration_attempt(&r.fence.attempt_id)
            .await
            .unwrap()
            .unwrap()
            .effect_intent_json
            .is_none());
    }

    #[tokio::test]
    async fn refusal_after_intent_commits_settles_not_performed() {
        let (db, f) = claimed().await;
        let r = request(f);
        let IntegrationEffectAdmission::Started(mut g) =
            db.begin_integration_effect(r.clone()).await.unwrap()
        else {
            panic!("not admitted")
        };
        sqlx::query("UPDATE integration_queue SET lease_until='2000-01-01T00:00:00Z' WHERE id=?")
            .bind(&r.fence.queue_id)
            .execute(db.pool())
            .await
            .unwrap();
        assert_eq!(
            g.start().await.unwrap(),
            Some(IntegrationEffectRefusal::StaleFence)
        );
        drop(g);
        let receipt = db.integration_effect_receipt(&r).await.unwrap().unwrap();
        assert_eq!(receipt.result["kind"], "not_performed");
        assert!(db
            .integration_attempt(&r.fence.attempt_id)
            .await
            .unwrap()
            .unwrap()
            .effect_intent_json
            .is_none());
    }

    #[tokio::test]
    async fn effect_guard_does_not_hold_sqlite_writer() {
        let (db, f) = claimed().await;
        let IntegrationEffectAdmission::Started(mut g) =
            db.begin_integration_effect(request(f)).await.unwrap()
        else {
            panic!("not admitted")
        };
        assert!(g.start().await.unwrap().is_none());
        tokio::time::timeout(std::time::Duration::from_millis(250), async {
            let mut tx = begin_immediate(db.pool()).await.unwrap();
            sqlx::query("UPDATE repo SET name='second writer' WHERE id='r'")
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        })
        .await
        .expect("effect retained SQLite's writer");
        g.record(
            serde_json::json!({"kind":"rebased"}),
            IntegrationOperationState::Succeeded,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn deleting_the_repo_removes_an_attempt_that_holds_a_fence_and_a_receipt() {
        let (db, f) = claimed().await;
        let IntegrationEffectAdmission::Started(g) = db
            .begin_integration_effect(request(f.clone()))
            .await
            .unwrap()
        else {
            panic!("not admitted")
        };
        g.record(
            serde_json::json!({"kind":"rebased"}),
            IntegrationOperationState::Succeeded,
        )
        .await
        .unwrap();
        crate::RepoRepo::delete(&db, "r").await.unwrap();
        for table in ["integration_queue", "integration_attempt"] {
            let rows: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(db.pool())
                .await
                .unwrap();
            assert_eq!(rows, 0, "{table}");
        }
    }
    #[tokio::test]
    async fn a_refused_claim_keeps_a_quarantined_queue_quarantined() {
        let (db, f) = claimed().await;
        sqlx::query(
            "UPDATE integration_queue SET state='quarantined',lease_until='2026-10-08T00:00:30Z' WHERE id=?",
        )
        .bind(&f.queue_id)
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE repo_location SET status='unavailable' WHERE id='l'")
            .execute(db.pool())
            .await
            .unwrap();
        let q = db.integration_queue(&f.queue_id).await.unwrap().unwrap();
        assert!(matches!(
            db.claim_integration_queue(
                &q.id,
                q.revision,
                "next",
                "2026-10-09T00:00:00Z",
                "2026-10-09T00:01:00Z",
            )
            .await,
            Err(DbError::VersionConflict)
        ));
        let refused = db.integration_queue(&f.queue_id).await.unwrap().unwrap();
        assert_eq!(refused.state, IntegrationQueueState::Quarantined);
        assert_eq!(
            refused.last_error_kind,
            Some(IntegrationFailureKind::TargetUnavailable)
        );
        assert_eq!(refused.fence_generation, q.fence_generation);
    }
    #[tokio::test]
    async fn location_removal_settles_inflight_effect_and_preserves_quarantine() {
        for quarantined in [false, true] {
            let (db, f) = claimed().await;
            let request = request(f.clone());
            let IntegrationEffectAdmission::Started(mut guard) =
                db.begin_integration_effect(request.clone()).await.unwrap()
            else {
                panic!("not admitted");
            };
            assert!(guard.start().await.unwrap().is_none());
            if quarantined {
                sqlx::query("UPDATE integration_queue SET state='quarantined' WHERE id=?")
                    .bind(&f.queue_id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            crate::RepoLocationRepo::delete(&db, "l").await.unwrap();
            let receipt = db
                .integration_effect_receipt(&request)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(receipt.result["kind"], "infrastructure");
            assert_eq!(receipt.operation_state, IntegrationOperationState::Failed);
            let attempt = db
                .integration_attempt(&f.attempt_id)
                .await
                .unwrap()
                .unwrap();
            assert!(attempt.effect_intent_json.is_none());
            assert_eq!(
                attempt.failure_kind,
                Some(IntegrationFailureKind::Infrastructure)
            );
            let queue = db.integration_queue(&f.queue_id).await.unwrap().unwrap();
            assert_eq!(
                queue.state,
                if quarantined {
                    IntegrationQueueState::Quarantined
                } else {
                    IntegrationQueueState::Suspended
                }
            );
            assert_eq!(
                queue.last_error_kind,
                Some(IntegrationFailureKind::TargetUnconfigured)
            );
            // A late original owner cannot overwrite the permanent-loss receipt.
            assert!(guard
                .record(
                    serde_json::json!({"kind":"rebased"}),
                    IntegrationOperationState::Succeeded
                )
                .await
                .is_err());
        }
    }

    #[tokio::test]
    async fn machine_removal_settles_attempt_and_preserves_quarantine() {
        for quarantined in [false, true] {
            let db = fixture().await;
            let now = now_rfc3339();
            sqlx::query("INSERT INTO daemon (id,machine_id,hostname,os,arch,status,created_at,updated_at) VALUES ('d','remote-machine','host','linux','aarch64','offline',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
            sqlx::query("INSERT INTO runtime(id,daemon_id,kind,workspace_root,status,labels_json,created_at,updated_at) VALUES ('rt','d','codex','root','offline','{}',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
            sqlx::query("UPDATE repo_location SET owner_kind='daemon',daemon_id='d',runtime_id='rt' WHERE id='l'").execute(db.pool()).await.unwrap();
            let q = db
                .create_or_get_integration_queue("r", "main")
                .await
                .unwrap();
            let a = db
                .admit_integration_attempt(admission(&q, "a", "removed-machine"))
                .await
                .unwrap();
            let q = db.integration_queue(&q.id).await.unwrap().unwrap();
            db.claim_integration_queue(&q.id, q.revision, "worker", &now, "2099-01-01T00:00:00Z")
                .await
                .unwrap();
            let fence = db.integration_owner_fence(&a.id).await.unwrap().unwrap();
            let request = request(fence.clone());
            let IntegrationEffectAdmission::Started(mut guard) =
                db.begin_integration_effect(request.clone()).await.unwrap()
            else {
                panic!("not admitted");
            };
            assert!(guard.start().await.unwrap().is_none());
            if quarantined {
                sqlx::query("UPDATE integration_queue SET state='quarantined' WHERE id=?")
                    .bind(&q.id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            db.remove_daemon("d", "admin", true, "local-host", false)
                .await
                .unwrap();
            let settled = db.integration_attempt(&a.id).await.unwrap().unwrap();
            assert!(settled.effect_intent_json.is_none());
            assert_eq!(
                settled.failure_kind,
                Some(IntegrationFailureKind::Infrastructure)
            );
            assert_eq!(
                settled.current_operation_state,
                Some(IntegrationOperationState::Failed)
            );
            let receipt = db
                .integration_effect_receipt(&request)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(receipt.result["kind"], "infrastructure");
            let q = db.integration_queue(&q.id).await.unwrap().unwrap();
            assert_eq!(
                q.state,
                if quarantined {
                    IntegrationQueueState::Quarantined
                } else {
                    IntegrationQueueState::Suspended
                }
            );
            assert!(q.lease_owner.is_none());
            drop(guard);
        }
    }

    #[tokio::test]
    async fn uncertain_receipt_reconciliation_keeps_one_attempt_key() {
        let (db, f) = claimed().await;
        let request = request(f);
        let IntegrationEffectAdmission::Started(mut guard) =
            db.begin_integration_effect(request.clone()).await.unwrap()
        else {
            panic!("not admitted");
        };
        assert!(guard.start().await.unwrap().is_none());
        guard
            .record(
                serde_json::json!({"kind":"infrastructure"}),
                IntegrationOperationState::Uncertain,
            )
            .await
            .unwrap();
        let reconciliation = db
            .lock_integration_reconciliation(&request)
            .await
            .unwrap()
            .unwrap();
        reconciliation
            .record(
                serde_json::json!({"kind":"completed"}),
                IntegrationOperationState::Succeeded,
            )
            .await
            .unwrap();
        let count: i64 = sqlx::query_scalar(
            "SELECT json_array_length(effect_receipts_json) FROM integration_attempt WHERE id=?",
        )
        .bind(&request.fence.attempt_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(count, 1);
        assert!(db
            .integration_attempt(&request.fence.attempt_id)
            .await
            .unwrap()
            .unwrap()
            .effect_intent_json
            .is_none());
    }

    #[tokio::test]
    async fn fencing_json_bounds_and_effect_enum_sql_parity() {
        let (db, f) = claimed().await;
        let too_large = "x".repeat(1048577);
        for column in [
            "owner_fence_json",
            "effect_intent_json",
            "effect_receipts_json",
        ] {
            let json = if column == "effect_receipts_json" {
                serde_json::json!([too_large]).to_string()
            } else {
                serde_json::json!({"data":too_large}).to_string()
            };
            assert!(sqlx::query(&format!(
                "UPDATE integration_attempt SET {column}=? WHERE id=?"
            ))
            .bind(json)
            .bind(&f.attempt_id)
            .execute(db.pool())
            .await
            .is_err());
        }
        let sql = include_str!("../../migrations/V202610080123__integration_queue.sql");
        let list = sql
            .split("operation_kind IN (")
            .nth(1)
            .unwrap()
            .split(')')
            .next()
            .unwrap();
        let mut sql_values: Vec<_> = list
            .split(',')
            .map(|s| s.trim().trim_matches('\'').to_owned())
            .collect();
        sql_values.sort();
        let mut rust_values: Vec<_> = IntegrationOperationKind::ALL
            .iter()
            .map(ToString::to_string)
            .collect();
        rust_values.sort();
        assert_eq!(sql_values, rust_values);
    }
}

#[cfg(test)]
mod concurrency_tests {
    use super::*;
    use crate::integration_queue::tests::{admission, seed};
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_real_claimants_install_exactly_one_durable_owner_fence() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            dir.path().join("fences.sqlite").display()
        );
        let pool = crate::create_sqlite_pool(&url).await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        seed(&pool).await;
        let db = std::sync::Arc::new(SqliteDb::new(pool));
        let q = db
            .create_or_get_integration_queue("r", "main")
            .await
            .unwrap();
        let a = db
            .admit_integration_attempt(admission(&q, "a", "race"))
            .await
            .unwrap();
        let q = db.integration_queue(&q.id).await.unwrap().unwrap();
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let run = |owner: &'static str| {
            let db = db.clone();
            let q = q.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                db.claim_integration_queue(
                    &q.id,
                    q.revision,
                    owner,
                    "2026-10-08T00:00:00Z",
                    "2099-01-01T00:00:00Z",
                )
                .await
            })
        };
        let first = run("first");
        let second = run("second");
        let first = first.await.unwrap();
        let second = second.await.unwrap();
        assert_ne!(first.is_ok(), second.is_ok());
        let winner = match (first, second) {
            (Ok(q), Err(DbError::VersionConflict)) | (Err(DbError::VersionConflict), Ok(q)) => q,
            other => panic!("unexpected race outcome: {other:?}"),
        };
        let fence = db.integration_owner_fence(&a.id).await.unwrap().unwrap();
        assert_eq!(fence.generation, 1);
        assert_eq!(Some(fence.lease_owner), winner.lease_owner);
        assert_eq!(fence.target_owner, winner.target_owner_json.unwrap());
    }
}

#[cfg(test)]
mod takeover_tests {
    use super::*;
    use crate::integration_queue::tests::{admission, fixture};
    #[tokio::test]
    async fn takeover_keeps_original_intent_and_refuses_old_fence_and_new_effect() {
        let db = fixture().await;
        let q = db
            .create_or_get_integration_queue("r", "main")
            .await
            .unwrap();
        let a = db
            .admit_integration_attempt(admission(&q, "a", "takeover"))
            .await
            .unwrap();
        let q = db.integration_queue(&q.id).await.unwrap().unwrap();
        let first = db
            .claim_integration_queue(
                &q.id,
                q.revision,
                "first",
                "2026-10-08T00:00:00Z",
                "2099-01-01T00:00:00Z",
            )
            .await
            .unwrap();
        sqlx::query("UPDATE integration_queue SET last_error_kind='infrastructure',last_error='owner reply missing' WHERE id=?").bind(&q.id).execute(db.pool()).await.unwrap();
        let f = db.integration_owner_fence(&a.id).await.unwrap().unwrap();
        let request = IntegrationEffectRequest {
            fence: f.clone(),
            kind: IntegrationOperationKind::Rebase,
            witness: serde_json::json!({"workspace":"w"}),
        };
        let IntegrationEffectAdmission::Started(mut g) =
            db.begin_integration_effect(request.clone()).await.unwrap()
        else {
            panic!("not admitted")
        };
        assert!(g.start().await.unwrap().is_none());
        drop(g);
        let next = db
            .claim_integration_queue(
                &q.id,
                first.revision,
                "next",
                "2099-01-01T00:00:01Z",
                "2099-01-01T00:01:00Z",
            )
            .await
            .unwrap();
        assert_eq!(next.fence_generation, 2);
        assert_eq!(
            next.last_error_kind,
            Some(IntegrationFailureKind::Infrastructure)
        );
        assert_eq!(next.last_error.as_deref(), Some("owner reply missing"));
        let attempt = db.integration_attempt(&a.id).await.unwrap().unwrap();
        assert_eq!(attempt.state, IntegrationAttemptState::Reconciling);
        assert!(matches!(
            db.begin_integration_effect(request).await.unwrap(),
            IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::StaleFence)
        ));
        let raw: String =
            sqlx::query_scalar("SELECT effect_intent_json FROM integration_attempt WHERE id=?")
                .bind(&a.id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        let old: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(old["request"]["fence"]["generation"], 1);
        let new_request = IntegrationEffectRequest {
            fence: db.integration_owner_fence(&a.id).await.unwrap().unwrap(),
            kind: IntegrationOperationKind::Rebase,
            witness: serde_json::json!({"workspace":"w"}),
        };
        assert!(matches!(
            db.begin_integration_effect(new_request).await.unwrap(),
            IntegrationEffectAdmission::Refused(IntegrationEffectRefusal::ReconciliationRequired)
        ));
    }
}
