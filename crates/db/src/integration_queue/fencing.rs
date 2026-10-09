//! Durable attempt ownership and effect receipts; no Git or Task mutation.
use super::*;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IntegrationOwnerFence {
    pub queue_id: String,
    pub attempt_id: String,
    pub generation: i64,
    pub lease_owner: String,
    pub target_owner: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrationEffectRefusal {
    StaleFence,
    ForeignOwner,
    WitnessMismatch,
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

/// The write guard prevents claim/takeover from invalidating a fence between
/// its last check and Git. Intent was committed before acquiring this guard.
/// Dropping it leaves that intent unresolved, never permission to repeat Git.
pub struct IntegrationEffectGuard {
    transaction: Transaction<'static, Sqlite>,
    request: IntegrationEffectRequest,
}
impl IntegrationEffectGuard {
    pub async fn record(
        mut self,
        result: Value,
        operation_state: IntegrationOperationState,
    ) -> Result<IntegrationEffectReceipt> {
        if !matches!(
            operation_state,
            IntegrationOperationState::Succeeded
                | IntegrationOperationState::Failed
                | IntegrationOperationState::Uncertain
        ) {
            return Err(DbError::Check(
                "receipt must describe a settled or uncertain effect".into(),
            ));
        }
        let receipt = IntegrationEffectReceipt {
            request: self.request,
            result,
            operation_state,
            recorded_at: now_rfc3339(),
        };
        let payload = serde_json::to_string(&receipt).map_err(|e| DbError::Check(e.to_string()))?;
        if payload.len() > 131072 {
            return Err(DbError::Check("effect receipt exceeds bound".into()));
        }
        let changed = sqlx::query("UPDATE integration_attempt SET effect_receipts_json=json_insert(effect_receipts_json,'$[#]',json(?)),effect_intent_json=CASE WHEN ?='uncertain' THEN effect_intent_json ELSE NULL END,current_operation_state=?,updated_at=?,revision=revision+1 WHERE id=? AND slot_generation=?")
            .bind(payload).bind(operation_state.to_string()).bind(operation_state.to_string()).bind(&receipt.recorded_at).bind(&receipt.request.fence.attempt_id).bind(receipt.request.fence.generation)
            .execute(&mut *self.transaction).await?.rows_affected();
        if changed != 1 {
            return Err(DbError::NotFound);
        }
        self.transaction.commit().await?;
        Ok(receipt)
    }
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

impl SqliteDb {
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
        if existing.is_some() {
            return Ok(IntegrationEffectAdmission::Refused(
                IntegrationEffectRefusal::ReconciliationRequired,
            ));
        }
        // Keep the digest in the frozen intent to diagnose reused input keys.
        let digest = Sha256::digest(json.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let intent = serde_json::json!({"request":request,"digest":digest});
        sqlx::query("UPDATE integration_attempt SET effect_intent_json=?,operation_kind=?,current_operation_state='running',revision=revision+1,updated_at=? WHERE id=?")
            .bind(intent.to_string()).bind(request.kind.to_string()).bind(now_rfc3339()).bind(&request.fence.attempt_id).execute(&mut *tx).await?;
        tx.commit().await?;
        let mut tx = begin_immediate(self.pool()).await?;
        if let Some(refusal) = verify(&mut tx, &request.fence).await? {
            return Ok(IntegrationEffectAdmission::Refused(refusal));
        }
        Ok(IntegrationEffectAdmission::Started(
            IntegrationEffectGuard {
                transaction: tx,
                request,
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
        let row=sqlx::query("SELECT p.workspace_id,p.generation,p.owner_kind,p.workspace_handle,w.worktree_path,l.path FROM workspace_placement p JOIN workspace w ON w.id=p.workspace_id JOIN integration_attempt a ON a.task_ref=p.task_id JOIN integration_queue q ON q.id=a.queue_id JOIN repo_location l ON l.id=q.target_location_id WHERE p.id=? AND a.id=? AND p.state='ready' AND w.repo_id=q.repo_id AND q.target_branch=? AND (a.workspace_ref IS NULL OR a.workspace_ref=p.workspace_id) AND (a.placement_ref IS NULL OR a.placement_ref=p.id)")
            .bind(w["placement_id"].as_str()).bind(&self.request.fence.attempt_id).bind(self.request.witness["target_branch"].as_str()).fetch_optional(&mut *self.transaction).await?;
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
        let IntegrationEffectAdmission::Started(g) =
            db.begin_integration_effect(r.clone()).await.unwrap()
        else {
            panic!("not admitted")
        };
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
        let IntegrationEffectAdmission::Started(g) =
            db.begin_integration_effect(request.clone()).await.unwrap()
        else {
            panic!("not admitted")
        };
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
