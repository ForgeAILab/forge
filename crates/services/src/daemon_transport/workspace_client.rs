//! Workspace RPCs retain structured daemon errors on the existing command stream.

use std::{sync::Arc, time::Duration};

use api_types::*;
use db::{CommandReceiptRepo, DomainEventRepo, SqliteDb, WorkspacePlacementRepo};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::DaemonConnectionRegistry;
use crate::{integration_effects::rpc::validate_merge_result, ServiceError};

#[derive(Debug, thiserror::Error)]
pub enum WorkspaceClientError {
    #[error(transparent)]
    Transport(#[from] ServiceError),
    #[error("daemon rejected workspace request: {0:?}")]
    Daemon(DaemonErrorPayload),
}

pub type Result<T> = std::result::Result<T, WorkspaceClientError>;

fn redact_remote_error(error: WorkspaceClientError, remote: &str) -> WorkspaceClientError {
    match error {
        WorkspaceClientError::Daemon(mut error) => {
            error.message = git::redact_remote_credentials(&error.message, remote);
            error.details = error.details.and_then(|value| {
                serde_json::from_str(&git::redact_remote_credentials(&value.to_string(), remote))
                    .ok()
            });
            WorkspaceClientError::Daemon(error)
        }
        WorkspaceClientError::Transport(ServiceError::InvalidOperation { message }) => {
            ServiceError::invalid_operation(git::redact_remote_credentials(&message, remote)).into()
        }
        error => error,
    }
}

#[derive(Clone)]
pub struct DaemonWorkspaceClient {
    registry: Arc<DaemonConnectionRegistry>,
    timeout: Duration,
    db: Option<Arc<SqliteDb>>,
}

impl DaemonWorkspaceClient {
    pub(crate) fn registry(&self) -> &DaemonConnectionRegistry {
        &self.registry
    }

    /// The daemon's live connection, if it has one.
    pub fn connection_id(&self, daemon_id: &str) -> Option<u64> {
        self.registry
            .get(daemon_id)
            .filter(|connection| !connection.is_stale())
            .map(|connection| connection.id())
    }

    pub async fn machine_probe(
        &self,
        daemon_id: &str,
        params: MachineProbeParams,
    ) -> Result<MachineProbeResult> {
        let seconds = params
            .commands
            .iter()
            .map(|command| command.timeout_seconds.clamp(1, 300) + 3)
            .sum::<u64>();
        self.request(
            daemon_id,
            METHOD_MACHINE_PROBE,
            &params,
            Duration::from_secs(seconds).saturating_add(self.timeout),
            false,
        )
        .await
    }

    pub async fn provision_location(
        &self,
        daemon_id: &str,
        params: RepoLocationProvisionParams,
    ) -> Result<RepoLocationProvisionResult> {
        self.request(
            daemon_id,
            METHOD_REPO_LOCATION_PROVISION,
            &params,
            Duration::from_secs(params.timeout_seconds).saturating_add(self.timeout),
            false,
        )
        .await
        .map_err(|error| redact_remote_error(error, &params.remote_url))
    }
    /// Persistence-free check transport. The stage-C runner retains the intent,
    /// certifies the returned receipt and owns all acknowledgment/application.
    pub async fn run_check(
        &self,
        daemon_id: &str,
        params: DaemonCheckRunParams,
    ) -> Result<DaemonCheckResult> {
        let deadline = chrono::DateTime::parse_from_rfc3339(&params.deadline)
            .map_err(|_| ServiceError::invalid_operation("invalid check deadline"))?;
        let remaining = (deadline.with_timezone(&chrono::Utc) - chrono::Utc::now())
            .to_std()
            .unwrap_or_default();
        let result = self
            .check_request(
                daemon_id,
                METHOD_CHECK_RUN,
                &params,
                remaining
                    .saturating_add(Duration::from_millis(params.cleanup_timeout_ms))
                    .saturating_add(self.timeout),
            )
            .await?;
        validate_check_result(daemon_id, &params.operation_id, &result)?;
        Ok(result)
    }
    pub async fn lookup_check(
        &self,
        daemon_id: &str,
        operation_id: &str,
    ) -> Result<DaemonCheckResult> {
        let result = self
            .check_request(
                daemon_id,
                METHOD_CHECK_LOOKUP,
                &DaemonCheckOperationParams {
                    daemon_id: daemon_id.into(),
                    operation_id: operation_id.into(),
                },
                self.timeout,
            )
            .await?;
        validate_check_result(daemon_id, operation_id, &result)?;
        Ok(result)
    }
    pub async fn cancel_check(
        &self,
        daemon_id: &str,
        operation_id: &str,
    ) -> Result<DaemonCheckResult> {
        let result = self
            .check_request(
                daemon_id,
                METHOD_CHECK_CANCEL,
                &DaemonCheckOperationParams {
                    daemon_id: daemon_id.into(),
                    operation_id: operation_id.into(),
                },
                self.timeout.saturating_add(Duration::from_secs(31)),
            )
            .await?;
        validate_check_result(daemon_id, operation_id, &result)?;
        Ok(result)
    }

    async fn check_request<P: Serialize + Sync>(
        &self,
        daemon_id: &str,
        method: &str,
        params: &P,
        timeout: Duration,
    ) -> Result<DaemonCheckResult> {
        let params = serde_json::to_value(params).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid check request: {error}"))
        })?;
        // Bypass workspace mutation recorders and their entry_id-shaped decoder.
        // The durable check runner owns intent/result persistence and ACKs.
        let value = self
            .request_once(daemon_id, method, params, Some(timeout))
            .await?;
        serde_json::from_value(value).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid check owner reply: {error}")).into()
        })
    }

    pub fn new(registry: Arc<DaemonConnectionRegistry>) -> Self {
        Self {
            registry,
            timeout: Duration::from_secs(DEFAULT_DAEMON_COMMAND_TIMEOUT_SECS),
            db: None,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_receipts(mut self, db: Arc<SqliteDb>) -> Self {
        self.db = Some(db);
        self
    }

    pub async fn verify_location(
        &self,
        daemon_id: &str,
        params: RepoLocationVerifyParams,
    ) -> Result<RepoLocationVerifyResult> {
        let remote = params.remote_url.as_deref().unwrap_or_default();
        let mut result: RepoLocationVerifyResult = self
            .request(
                daemon_id,
                METHOD_REPO_LOCATION_VERIFY,
                &params,
                self.timeout,
                false,
            )
            .await
            .map_err(|error| redact_remote_error(error, remote))?;
        result.origin_url = result.origin_url.map(|origin| {
            git::redact_remote_credentials(
                &git::redact_remote_credentials(&origin, &origin),
                remote,
            )
        });
        Ok(result)
    }

    pub async fn prepare(
        &self,
        daemon_id: &str,
        params: WorkspacePrepareParams,
    ) -> Result<WorkspacePrepareResult> {
        self.request(
            daemon_id,
            METHOD_WORKSPACE_PREPARE,
            &params,
            self.timeout,
            true,
        )
        .await
    }

    pub async fn describe(
        &self,
        daemon_id: &str,
        params: WorkspaceDescribeParams,
    ) -> Result<WorkspaceDescribeResult> {
        self.request(
            daemon_id,
            METHOD_WORKSPACE_DESCRIBE,
            &params,
            self.timeout,
            false,
        )
        .await
    }

    pub async fn run(
        &self,
        daemon_id: &str,
        params: WorkspaceRunParams,
    ) -> Result<WorkspaceRunResult> {
        // The response includes the completed command, rather than a start ack.
        let timeout = (params.timeout_secs != 0)
            .then(|| Duration::from_secs(params.timeout_secs).saturating_add(self.timeout));
        self.request_with_timeout(daemon_id, METHOD_WORKSPACE_RUN, &params, timeout, true)
            .await
    }

    pub async fn cancel_workspace_operation(
        &self,
        daemon_id: &str,
        operation_id: &str,
    ) -> Result<api_types::WorkspaceCancelResult> {
        let result: api_types::WorkspaceCancelResult = self
            .request(
                daemon_id,
                api_types::METHOD_WORKSPACE_CANCEL,
                &api_types::WorkspaceCancelParams {
                    operation_id: operation_id.to_owned(),
                },
                Duration::from_secs(10),
                false,
            )
            .await
            .map_err(|error| match error {
                WorkspaceClientError::Daemon(error)
                    if error.code == api_types::UNSUPPORTED_METHOD =>
                {
                    WorkspaceClientError::Transport(ServiceError::DaemonUnavailable {
                        daemon_id: daemon_id.to_owned(),
                    })
                }
                error => error,
            })?;
        if result.operation_id != operation_id {
            return Err(ServiceError::invalid_operation(
                "workspace.cancel acknowledged another operation",
            )
            .into());
        }
        Ok(result)
    }

    pub async fn inspect(
        &self,
        daemon_id: &str,
        params: WorkspaceInspectParams,
    ) -> Result<WorkspaceInspectResult> {
        self.request(
            daemon_id,
            METHOD_WORKSPACE_READ,
            &params,
            self.timeout,
            false,
        )
        .await
    }

    pub async fn review_diff(
        &self,
        daemon_id: &str,
        params: WorkspaceReviewDiffParams,
    ) -> Result<WorkspaceReviewDiffResult> {
        self.request(
            daemon_id,
            METHOD_WORKSPACE_DIFF,
            &params,
            self.timeout,
            false,
        )
        .await
    }

    pub async fn describe_for_plan(
        &self,
        daemon_id: &str,
        params: WorkspaceDescribeParams,
    ) -> Result<WorkspaceDescribeResult> {
        self.request_with_timeout(daemon_id, METHOD_WORKSPACE_DESCRIBE, &params, None, false)
            .await
    }

    pub async fn owner_operation(
        &self,
        daemon_id: &str,
        params: WorkspaceOwnerOperationParams,
    ) -> Result<WorkspaceOwnerOperationResult> {
        if matches!(
            &params.operation,
            WorkspaceOwnerOperation::PublishPlan { .. }
                | WorkspaceOwnerOperation::RestorePlan { .. }
                | WorkspaceOwnerOperation::DiscardPlan { .. }
        ) {
            // Metadata RPCs may wait behind an unbounded owner CI operation.
            self.request_with_timeout(daemon_id, METHOD_WORKSPACE_RESET, &params, None, false)
                .await
        } else {
            self.request(
                daemon_id,
                METHOD_WORKSPACE_RESET,
                &params,
                self.timeout,
                true,
            )
            .await
        }
    }

    pub async fn diff(
        &self,
        daemon_id: &str,
        params: WorkspaceDiffParams,
    ) -> Result<WorkspaceDiffResult> {
        self.request(
            daemon_id,
            METHOD_WORKSPACE_DIFF,
            &params,
            self.timeout,
            false,
        )
        .await
    }

    pub async fn read(
        &self,
        daemon_id: &str,
        params: WorkspaceReadParams,
    ) -> Result<WorkspaceReadResult> {
        self.request(
            daemon_id,
            METHOD_WORKSPACE_READ,
            &params,
            self.timeout,
            false,
        )
        .await
    }

    pub async fn reset(
        &self,
        daemon_id: &str,
        params: WorkspaceResetParams,
    ) -> Result<WorkspaceResetResult> {
        self.request(
            daemon_id,
            METHOD_WORKSPACE_RESET,
            &params,
            self.timeout,
            true,
        )
        .await
    }

    pub async fn cleanup(
        &self,
        daemon_id: &str,
        params: WorkspaceCleanupParams,
    ) -> Result<WorkspaceCleanupResult> {
        self.request(
            daemon_id,
            METHOD_WORKSPACE_CLEANUP,
            &params,
            self.timeout,
            true,
        )
        .await
    }

    /// Call only after the caller has durably applied the operation result.
    pub async fn acknowledge(&self, daemon_id: &str, entry_id: String) -> Result<JournalAckResult> {
        let params = serde_json::to_value(JournalAckParams { entry_id }).expect("ack serializes");
        let result = self
            .request_once(daemon_id, METHOD_JOURNAL_ACK, params, Some(self.timeout))
            .await?;
        serde_json::from_value(result).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid journal acknowledgement: {error}"))
                .into()
        })
    }

    async fn request<P: Serialize + Sync, R: DeserializeOwned>(
        &self,
        daemon_id: &str,
        method: &str,
        params: &P,
        timeout: Duration,
        retry_timeout: bool,
    ) -> Result<R> {
        self.request_with_timeout(daemon_id, method, params, Some(timeout), retry_timeout)
            .await
    }

    async fn request_with_timeout<P: Serialize + Sync, R: DeserializeOwned>(
        &self,
        daemon_id: &str,
        method: &str,
        params: &P,
        timeout: Option<Duration>,
        retry_timeout: bool,
    ) -> Result<R> {
        let mut params = serde_json::to_value(params).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid daemon request params: {error}"))
        })?;
        if params.get("operation_id").is_some()
            && !matches!(method, METHOD_WORKSPACE_MERGE | METHOD_WORKSPACE_CANCEL)
        {
            params = self
                .remember_mutation(daemon_id, method, params, None)
                .await?;
        }
        let admission = self
            .bind_task_step_integration(daemon_id, method, &mut params)
            .await?;
        let receipt_guard = match admission {
            Some(db::IntegrationEffectAdmission::Replay(receipt)) => {
                let value = receipt.result.get("owner_result").cloned().ok_or_else(|| {
                    ServiceError::invalid_operation("prior owner effect did not complete")
                })?;
                return crate::integration_effects::rpc::decode_reply(method, &params, &value);
            }
            Some(db::IntegrationEffectAdmission::Started(guard)) => Some(guard),
            Some(db::IntegrationEffectAdmission::Refused(reason)) => {
                return Err(ServiceError::invalid_operation(format!(
                    "integration owner refused {reason:?}"
                ))
                .into())
            }
            None => None,
        };
        let mut receipt_guard = receipt_guard;
        let mut retried = false;
        let value = loop {
            match self
                .request_once(daemon_id, method, params.clone(), timeout)
                .await
            {
                Err(WorkspaceClientError::Transport(ServiceError::DaemonTimeout { .. }))
                    if retry_timeout && !retried =>
                {
                    // Reuse the complete serialized mutation, including its operation id.
                    retried = true;
                }
                Err(WorkspaceClientError::Daemon(error))
                    if matches!(method, METHOD_WORKSPACE_RUN | METHOD_WORKSPACE_MERGE)
                        && error.code == DAEMON_UNAVAILABLE
                        && error
                            .details
                            .as_ref()
                            .is_some_and(|details| details["interrupted"] == true) =>
                {
                    break self.reconcile(daemon_id, method, &params).await?;
                }
                Err(WorkspaceClientError::Daemon(error)) => {
                    if let Some(guard) = receipt_guard.take() {
                        let receipt = error
                            .details
                            .as_ref()
                            .and_then(|details| details.get("integration_receipt"))
                            .cloned();
                        settle_task_step_receipt(guard, receipt, &error.message).await;
                    }
                    if method != METHOD_WORKSPACE_MERGE {
                        self.retain_error(daemon_id, method, &params, &error)
                            .await?;
                    }
                    return Err(WorkspaceClientError::Daemon(error));
                }
                result => break result?,
            }
        };
        let result = crate::integration_effects::rpc::decode_reply(method, &params, &value)?;
        if let Some(guard) = receipt_guard {
            settle_task_step_receipt(
                guard,
                value.get("integration_receipt").cloned(),
                "owner reply carried no attempt receipt",
            )
            .await;
        }
        if !matches!(method, METHOD_WORKSPACE_MERGE | METHOD_WORKSPACE_CANCEL) {
            self.retain_result(daemon_id, method, &params, &value)
                .await?;
            if method != METHOD_WORKSPACE_CLEANUP {
                self.acknowledge_recorded(daemon_id, &value).await;
            }
        }
        Ok(result)
    }

    async fn reconcile(&self, daemon_id: &str, method: &str, params: &Value) -> Result<Value> {
        let operation_id = params["operation_id"].as_str().ok_or_else(|| {
            ServiceError::invalid_operation("interrupted workspace operation has no id")
        })?;
        let request = WorkspaceReconcileParams {
            integration: api_types::WorkspaceIntegrationBinding::TaskStep,
            workspace: serde_json::from_value(params.clone()).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid workspace reference: {error}"))
            })?,
            operation: WorkspaceReconcileOperation::Reconcile,
            operation_id: operation_id.to_owned(),
        };
        let value = self
            .request_once(
                daemon_id,
                METHOD_WORKSPACE_DESCRIBE,
                serde_json::to_value(request).expect("reconciliation serializes"),
                Some(self.timeout),
            )
            .await?;
        let result: WorkspaceReconcileResult = serde_json::from_value(value).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid workspace reconciliation: {error}"))
        })?;
        if result.operation_id != operation_id || result.entry_id.is_empty() {
            return Err(ServiceError::invalid_operation(
                "workspace reconciliation identity changed",
            )
            .into());
        }
        match result.outcome {
            WorkspaceReconcileOutcome::Result { result } => Ok(result),
            WorkspaceReconcileOutcome::Error { mut error } => {
                let receipt = error
                    .details
                    .as_ref()
                    .and_then(|details| details.get("integration_receipt"))
                    .cloned();
                error.details = Some(
                    serde_json::json!({"entry_id":result.entry_id,"operation_id":operation_id}),
                );
                if let Some(receipt) = receipt {
                    error.details.as_mut().expect("settled details")["integration_receipt"] =
                        receipt;
                }
                if method != METHOD_WORKSPACE_MERGE {
                    self.retain_error(daemon_id, method, params, &error).await?;
                }
                Err(WorkspaceClientError::Daemon(error))
            }
        }
    }

    /// Retain the owner response before acknowledging its journal entry.
    /// Merge outcomes commit execution evidence through retain_merge_result.
    pub(crate) async fn retain_result(
        &self,
        daemon_id: &str,
        method: &str,
        params: &Value,
        value: &Value,
    ) -> Result<()> {
        self.retain_receipt(daemon_id, method, params, value, "result")
            .await
    }

    /// A caller interrupted by timeout or restart reuses the complete durable
    /// request, including its original expected SHA and operation id.
    pub(crate) async fn remember_mutation(
        &self,
        daemon_id: &str,
        method: &str,
        params: Value,
        execution_id: Option<&str>,
    ) -> Result<Value> {
        let Some(db) = &self.db else {
            return Ok(params);
        };
        let placement_id = params["placement_id"]
            .as_str()
            .ok_or_else(|| ServiceError::invalid_operation("mutation has no placement"))?;
        let placement = WorkspacePlacementRepo::get_by_id(&**db, placement_id)
            .await
            .map_err(ServiceError::from)?
            .ok_or_else(|| ServiceError::not_found("workspace placement", placement_id))?;
        let rows = db
            .workspace_mutation_outstanding_intents(db::WorkspaceMutationOutstandingIntentInput {
                task_id: &placement.task_id,
                intent_operation: format!("daemon.{method}.intent"),
                placement_id,
                generation: params["generation"]
                    .as_i64()
                    .unwrap_or(placement.generation),
                result_operation: format!("daemon.{method}"),
            })
            .await
            .map_err(ServiceError::from)?;
        for row in rows {
            let saved: Value = serde_json::from_str(&row)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
            let request = &saved["owner_result"]["request"];
            if mutation_identity(method, request) == mutation_identity(method, &params)
                && saved["owner_result"]["implementation_execution_id"].as_str() == execution_id
            {
                let mut restored = request.clone();
                if let Some(content) = params.pointer("/operation/content") {
                    restored["operation"]["content"] = content.clone();
                    let operation = restored["operation"]
                        .as_object_mut()
                        .expect("plan operation object");
                    operation.remove("content_digest");
                    operation.remove("content_length");
                }
                return Ok(restored);
            }
        }
        let operation_id = params["operation_id"]
            .as_str()
            .ok_or_else(|| ServiceError::invalid_operation("mutation has no operation id"))?;
        let intent = serde_json::json!({"entry_id": format!("intent:{operation_id}"), "operation_id": operation_id,
            "request": params, "implementation_execution_id": execution_id});
        self.retain_receipt(daemon_id, method, &params, &intent, "intent")
            .await?;
        Ok(params)
    }

    pub(crate) async fn retain_merge_result(
        &self,
        daemon_id: &str,
        params: &Value,
        value: &Value,
        execution_id: &str,
    ) -> Result<()> {
        self.retain_receipt_with_execution(
            daemon_id,
            METHOD_WORKSPACE_MERGE,
            params,
            value,
            "result",
            Some(execution_id),
        )
        .await
    }

    pub(crate) async fn retain_error(
        &self,
        daemon_id: &str,
        method: &str,
        params: &Value,
        error: &DaemonErrorPayload,
    ) -> Result<()> {
        let Some(entry_id) = error
            .details
            .as_ref()
            .and_then(|details| details["entry_id"].as_str())
        else {
            return Ok(());
        };
        let value = serde_json::json!({"entry_id": entry_id, "operation_id": params["operation_id"], "error": error});
        self.retain_receipt(daemon_id, method, params, &value, "error")
            .await?;
        self.acknowledge_recorded(daemon_id, &value).await;
        Ok(())
    }

    async fn retain_receipt(
        &self,
        daemon_id: &str,
        method: &str,
        params: &Value,
        value: &Value,
        status: &str,
    ) -> Result<()> {
        self.retain_receipt_with_execution(daemon_id, method, params, value, status, None)
            .await
    }

    async fn retain_receipt_with_execution(
        &self,
        daemon_id: &str,
        method: &str,
        params: &Value,
        value: &Value,
        status: &str,
        execution_id: Option<&str>,
    ) -> Result<()> {
        let Some(db) = &self.db else { return Ok(()) };
        let Some(entry_id) = value["entry_id"].as_str() else {
            return Ok(());
        };
        let operation_id = params["operation_id"].as_str().ok_or_else(|| {
            ServiceError::invalid_operation("workspace receipt has no operation id")
        })?;
        if entry_id.is_empty() || value["operation_id"].as_str() != Some(operation_id) {
            return Err(
                ServiceError::invalid_operation("workspace result identity changed").into(),
            );
        }
        let placement_id = params["placement_id"].as_str().ok_or_else(|| {
            ServiceError::invalid_operation("workspace receipt has no placement id")
        })?;
        let placement = WorkspacePlacementRepo::get_by_id(&**db, placement_id)
            .await
            .map_err(ServiceError::from)?
            .ok_or_else(|| ServiceError::not_found("workspace placement", placement_id))?;
        if placement.daemon_id.as_deref() != Some(daemon_id)
            || placement.runtime_id.as_deref() != params["runtime_id"].as_str()
            || !(status == "error"
                || params["generation"].as_u64() == u64::try_from(placement.generation).ok()
                || (method == METHOD_WORKSPACE_RESET
                    && params.get("operation").is_none()
                    && params["generation"].as_u64()
                        == u64::try_from(placement.generation + 1).ok()))
        {
            return Err(ServiceError::conflict(
                "workspace receipt belongs to a different owner or generation",
            )
            .into());
        }
        let digest = format!(
            "sha256:{}",
            hex::encode(Sha256::digest(
                serde_json::to_vec(params).expect("params serialize")
            ))
        );
        let operation = if status == "intent" {
            format!("daemon.{method}.intent")
        } else {
            format!("daemon.{method}")
        };
        let mut transaction = db::begin_immediate(db.pool())
            .await
            .map_err(ServiceError::from)?;
        let removed: bool = db
            .workspace_receipt_daemon_removed_in_tx(&mut transaction, daemon_id)
            .await
            .map_err(ServiceError::from)?;
        if removed {
            return Err(ServiceError::not_found("daemon", daemon_id).into());
        }
        if let Some(existing) = CommandReceiptRepo::get_command_receipt_in_tx(
            &**db,
            &mut transaction,
            "system",
            "workspace-backend",
            "task",
            &placement.task_id,
            &operation,
            entry_id,
            &digest,
        )
        .await
        .map_err(ServiceError::from)?
        {
            let saved: Value = serde_json::from_str(&existing.outcome_json)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
            if saved["owner_result"] != redact_result(value, params) {
                return Err(
                    ServiceError::conflict("daemon changed a retained workspace result").into(),
                );
            }
            transaction.commit().await.map_err(ServiceError::from)?;
            return Ok(());
        }
        let now = db::now_rfc3339();
        if let Some(execution_id) = execution_id {
            if let Some(after_sha) = value.pointer("/outcome/after_sha").and_then(Value::as_str) {
                let before_sha = params
                    .pointer("/expected/sha")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        ServiceError::invalid_operation("merge evidence has no candidate SHA")
                    })?;
                let execution_version = db
                    .workspace_merge_execution_version_in_tx(
                        &mut transaction,
                        execution_id,
                        &placement.workspace_id,
                    )
                    .await
                    .map_err(ServiceError::from)?
                    .ok_or_else(|| {
                        ServiceError::conflict("merge execution no longer owns this workspace")
                    })?;
                let updated = db
                    .record_workspace_merge_evidence_in_tx(
                        &mut transaction,
                        db::RecordWorkspaceMergeEvidenceInTxInput {
                            before_sha,
                            after_sha,
                            updated_at: &now,
                            execution_id,
                            version: execution_version,
                        },
                    )
                    .await
                    .map_err(ServiceError::from)?;
                if updated.rows_affected() != 1 {
                    return Err(ServiceError::from(db::DbError::VersionConflict).into());
                }
            }
        }
        let event_id = db::new_uuid_v4();
        let metadata = serde_json::json!({"placement_id": placement_id, "generation": params["generation"],
            "daemon_id": daemon_id, "method": method, "entry_id": entry_id, "operation_id": operation_id, "status": status});
        DomainEventRepo::append_event_in_tx(
            &**db,
            &mut transaction,
            &db::CreateDomainEvent {
                id: event_id.clone(),
                event_type: "workspace.operation_recorded".into(),
                entity_type: "workspace".into(),
                entity_id: placement.workspace_id.clone(),
                actor_type: "system".into(),
                actor_id: Some("workspace-backend".into()),
                scope_type: "task".into(),
                scope_id: placement.task_id.clone(),
                correlation_id: operation_id.into(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(format!("workspace-operation:{entry_id}")),
                payload_json: metadata.to_string(),
                created_at: now.clone(),
            },
        )
        .await
        .map_err(ServiceError::from)?;
        CommandReceiptRepo::create_command_receipt_in_tx(&**db, &mut transaction, db::CreateCommandReceipt {
            id: db::new_uuid_v4(), principal_type: "system".into(), principal_id: "workspace-backend".into(),
            scope_type: "task".into(), scope_id: placement.task_id, operation, idempotency_key: entry_id.into(),
            input_digest: digest, policy_result: "allowed".into(), correlation_id: operation_id.into(),
            causation_id: None, causation_depth: 0, event_id, agent_action_execution_id: None,
            outcome_json: serde_json::json!({"metadata": metadata, "owner_result": redact_result(value, params)}).to_string(),
            committed_at: now,
        }).await.map_err(ServiceError::from)?;
        transaction.commit().await.map_err(ServiceError::from)?;
        Ok(())
    }

    async fn acknowledge_receipt(&self, daemon_id: &str, value: &Value) -> Result<()> {
        if self.db.is_none() {
            return Ok(());
        }
        let entry_id = value["entry_id"].as_str().ok_or_else(|| {
            ServiceError::invalid_operation("workspace receipt has no journal entry")
        })?;
        let ack = self.acknowledge(daemon_id, entry_id.to_owned()).await?;
        if ack.entry_id != entry_id || !ack.acknowledged {
            return Err(ServiceError::invalid_operation(
                "owner did not acknowledge workspace journal entry",
            )
            .into());
        }
        self.record_acknowledgement(daemon_id, entry_id).await
    }

    pub(crate) async fn acknowledge_recorded(&self, daemon_id: &str, value: &Value) {
        // An acknowledgement failure never fails a successful owner operation.
        if let Err(error) = self.acknowledge_receipt(daemon_id, value).await {
            tracing::warn!(daemon_id, entry_id = ?value["entry_id"], %error, "workspace acknowledgement remains pending");
        }
    }

    /// Reconnect sweeps call this even for placements already marked cleaned.
    pub async fn retry_acknowledgements(&self, daemon_id: &str) -> Result<()> {
        let Some(db) = &self.db else { return Ok(()) };
        let results = db
            .workspace_pending_acknowledgement_results(daemon_id)
            .await
            .map_err(ServiceError::from)?;
        for result in results {
            let result: Value = match serde_json::from_str(&result) {
                Ok(result) => result,
                Err(error) => {
                    tracing::warn!(%daemon_id, %error, "invalid workspace acknowledgement receipt");
                    continue;
                }
            };
            self.retry_receipt_acknowledgement(daemon_id, &result["owner_result"])
                .await?;
        }
        Ok(())
    }

    async fn retry_receipt_acknowledgement(&self, daemon_id: &str, value: &Value) -> Result<()> {
        match self.acknowledge_receipt(daemon_id, value).await {
            Ok(()) => Ok(()),
            Err(
                error @ WorkspaceClientError::Transport(
                    ServiceError::DaemonUnavailable { .. }
                    | ServiceError::DaemonTimeout { .. }
                    | ServiceError::DaemonNotReady { .. }
                    | ServiceError::DaemonUpgradeRequired { .. },
                ),
            ) => Err(error),
            Err(error) => {
                tracing::warn!(%daemon_id, entry_id = ?value["entry_id"], %error, "workspace acknowledgement refused; continuing batch");
                Ok(())
            }
        }
    }

    /// Inspect retained run/merge intents on reconnect without rerunning either
    /// command. The owner must durably settle an intent before returning it.
    pub async fn reconcile_pending_operations(&self, daemon_id: &str) -> Result<()> {
        // Attempt receipts are bookkeeping. A lookup that fails must not stop
        // the run/merge intent reconciliation below (today's recovery); the
        // next admission on that checkout settles what is left.
        if let Err(error) = self.reconcile_integration_attempts(daemon_id).await {
            let error = match error {
                WorkspaceClientError::Transport(error) => error.to_string(),
                WorkspaceClientError::Daemon(error) => error.message,
            };
            tracing::warn!(target: "services::daemon_transport", %daemon_id, %error, "integration attempt reconciliation failed on reconnect");
        }
        let Some(db) = &self.db else { return Ok(()) };
        let intents = db
            .workspace_reconnect_unsettled_intents(daemon_id)
            .await
            .map_err(ServiceError::from)?;
        for intent in intents {
            let intent: Value = serde_json::from_str(&intent)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
            let params = &intent["owner_result"]["request"];
            let method = intent["metadata"]["method"]
                .as_str()
                .ok_or_else(|| ServiceError::invalid_operation("workspace intent has no method"))?;
            let value = match self.reconcile(daemon_id, method, params).await {
                Ok(value) => value,
                Err(WorkspaceClientError::Daemon(_)) => {
                    // reconcile() already retained and attempted this ACK;
                    // retry only when that acknowledgement was interrupted.
                    self.retry_acknowledgements(daemon_id).await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if value["operation_id"] != params["operation_id"] {
                return Err(ServiceError::invalid_operation(
                    "reconciled workspace operation identity changed",
                )
                .into());
            }
            match method {
                METHOD_WORKSPACE_RUN => {
                    let _: WorkspaceRunResult = serde_json::from_value(value.clone())
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                    self.retain_result(daemon_id, method, params, &value)
                        .await?;
                }
                METHOD_WORKSPACE_MERGE => {
                    let result: WorkspaceMergeResult = serde_json::from_value(value.clone())
                        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                    validate_merge_result(params, &result)?;
                    let placement_id =
                        intent["metadata"]["placement_id"].as_str().ok_or_else(|| {
                            ServiceError::invalid_operation("workspace intent has no placement")
                        })?;
                    let placement = WorkspacePlacementRepo::get_by_id(&**db, placement_id)
                        .await
                        .map_err(ServiceError::from)?
                        .ok_or_else(|| {
                            ServiceError::not_found("workspace placement", placement_id)
                        })?;
                    let execution_id = intent["owner_result"]["implementation_execution_id"]
                        .as_str()
                        .ok_or_else(|| {
                            ServiceError::invalid_operation(
                                "merge intent has no implementation execution",
                            )
                        })?;
                    let execution = db::ExecutionRepo::get_by_id(&**db, execution_id)
                        .await
                        .map_err(ServiceError::from)?
                        .filter(|execution| {
                            execution.workspace_id.as_deref() == Some(&placement.workspace_id)
                        })
                        .ok_or_else(|| {
                            ServiceError::conflict(
                                "interrupted merge lost its implementation execution",
                            )
                        })?;
                    self.retain_merge_result(daemon_id, params, &value, &execution.id)
                        .await?;
                }
                _ => unreachable!("query selects only run and merge intents"),
            }
            self.retry_receipt_acknowledgement(daemon_id, &value)
                .await?;
        }
        Ok(())
    }

    async fn record_acknowledgement(&self, daemon_id: &str, entry_id: &str) -> Result<()> {
        let Some(db) = &self.db else { return Ok(()) };
        let row = db
            .workspace_acknowledgement_result_identity(entry_id, daemon_id)
            .await
            .map_err(ServiceError::from)?;
        let Some((task_id, operation, digest)) = row else {
            return Ok(());
        };
        let result = CommandReceiptRepo::get_command_receipt(
            &**db,
            "system",
            "workspace-backend",
            "task",
            &task_id,
            &operation,
            entry_id,
            &digest,
        )
        .await
        .map_err(ServiceError::from)?
        .ok_or_else(|| ServiceError::invalid_operation("workspace result receipt disappeared"))?;
        let operation = format!("{operation}.ack");
        let mut transaction = db::begin_immediate(db.pool())
            .await
            .map_err(ServiceError::from)?;
        if CommandReceiptRepo::get_command_receipt_in_tx(
            &**db,
            &mut transaction,
            "system",
            "workspace-backend",
            "task",
            &task_id,
            &operation,
            entry_id,
            &digest,
        )
        .await
        .map_err(ServiceError::from)?
        .is_none()
        {
            CommandReceiptRepo::create_command_receipt_in_tx(
                &**db,
                &mut transaction,
                db::CreateCommandReceipt {
                    id: db::new_uuid_v4(),
                    principal_type: result.principal_type,
                    principal_id: result.principal_id,
                    scope_type: result.scope_type,
                    scope_id: result.scope_id,
                    operation,
                    idempotency_key: result.idempotency_key,
                    input_digest: result.input_digest,
                    policy_result: result.policy_result,
                    correlation_id: result.correlation_id,
                    causation_id: None,
                    causation_depth: 0,
                    event_id: result.event_id,
                    agent_action_execution_id: None,
                    outcome_json: serde_json::json!({"entry_id": entry_id, "acknowledged": true})
                        .to_string(),
                    committed_at: db::now_rfc3339(),
                },
            )
            .await
            .map_err(ServiceError::from)?;
        }
        transaction.commit().await.map_err(ServiceError::from)?;
        Ok(())
    }

    async fn request_once(
        &self,
        daemon_id: &str,
        method: &str,
        params: Value,
        timeout: Option<Duration>,
    ) -> Result<Value> {
        let connection = self
            .registry
            .get(daemon_id)
            .filter(|connection| !connection.is_stale())
            .ok_or_else(|| ServiceError::DaemonUnavailable {
                daemon_id: daemon_id.to_owned(),
            })?;
        self.registry
            .ensure_protocol_dispatchable(daemon_id, &connection)?;
        let mut exchange = crate::integration_effects::rpc::RpcExchange::prepare(
            connection,
            method,
            params.clone(),
        )?;
        let tracked = self.record_exchange_admission(method, &params).await?;
        let value = exchange.execute(timeout).await?;
        self.record_exchange_completion(tracked.as_ref()).await?;
        Ok(value)
    }

    async fn record_exchange_admission(
        &self,
        method: &str,
        params: &Value,
    ) -> Result<Option<(db::TaskStep, String)>> {
        if matches!(
            method,
            METHOD_WORKSPACE_PREPARE
                | METHOD_WORKSPACE_RUN
                | METHOD_WORKSPACE_MERGE
                | METHOD_WORKSPACE_RESET
                | METHOD_WORKSPACE_CLEANUP
        ) {
            if let (Some(db), Some(step), Some(placement_id), Some(operation_id)) = (
                &self.db,
                db::task_writer::current_task_step(),
                params["placement_id"].as_str(),
                params["operation_id"].as_str(),
            ) {
                let placement = WorkspacePlacementRepo::get_by_id(&**db, placement_id)
                    .await
                    .map_err(ServiceError::from)?
                    .ok_or_else(|| ServiceError::not_found("workspace placement", placement_id))?;
                db.register_remote_task_operation(&step, &placement, operation_id)
                    .await
                    .map_err(ServiceError::from)?;
                return Ok(Some((step, operation_id.to_owned())));
            }
        }
        Ok(None)
    }

    async fn record_exchange_completion(
        &self,
        tracked: Option<&(db::TaskStep, String)>,
    ) -> Result<()> {
        if let (Some(db), Some((step, operation_id))) = (&self.db, tracked) {
            db.finish_remote_task_operation(step, operation_id)
                .await
                .map_err(ServiceError::from)?;
        }
        Ok(())
    }
}

fn redact_result(value: &Value, params: &Value) -> Value {
    let mut result = value.clone();
    let env = params
        .get("env")
        .cloned()
        .and_then(|env| serde_json::from_value::<Vec<(String, String)>>(env).ok())
        .unwrap_or_default()
        .into_iter()
        .collect();
    for field in ["stdout", "stderr"] {
        if let Some(text) = result[field].as_str() {
            result[field] = Value::String(executors::environment::redact_environment_values(
                text, &env,
            ));
        }
    }
    if let Some(request) = result.get_mut("request") {
        *request = plan_request_metadata(request);
    }
    result
}

fn plan_request_metadata(params: &Value) -> Value {
    let mut metadata = params.clone();
    if metadata.pointer("/operation/kind").and_then(Value::as_str) == Some("publish_plan") {
        if let Some(content) = metadata
            .pointer("/operation/content")
            .and_then(Value::as_str)
            .map(str::to_owned)
        {
            metadata["operation"]["content_digest"] =
                Value::String(hex::encode(Sha256::digest(content.as_bytes())));
            metadata["operation"]["content_length"] = serde_json::json!(content.len());
            metadata["operation"]
                .as_object_mut()
                .unwrap()
                .remove("content");
        }
    }
    metadata
}

fn mutation_identity(method: &str, params: &Value) -> Value {
    let mut identity = plan_request_metadata(params);
    if let Some(object) = identity.as_object_mut() {
        object.remove("operation_id");
        if method == METHOD_WORKSPACE_MERGE {
            if object.get("reviewed_commit_sha").is_none_or(Value::is_null) {
                object.remove("expected_target_sha");
            }
        } else if method != METHOD_WORKSPACE_RUN {
            object.remove("expected");
        }
    }
    identity
}

/// Retain the owner's receipt for a Task-step effect that has already been
/// answered. The receipt records the effect and is never its result: a
/// missing, oversized or contradictory receipt settles the attempt terminally
/// with a bounded note, and the caller still returns what the owner said.
async fn settle_task_step_receipt(
    guard: db::IntegrationEffectGuard,
    receipt: Option<Value>,
    context: &str,
) {
    let request = guard.request().clone();
    let reason = match receipt.map(serde_json::from_value::<db::IntegrationEffectReceipt>) {
        Some(Ok(receipt)) => match crate::integration_owner::validate_attempt_receipt(&receipt) {
            Ok(()) if receipt.request != request => {
                "owner receipt belongs to another request".to_owned()
            }
            Ok(()) => match guard.record_owner_receipt(receipt).await {
                Ok(()) => return,
                Err(error) => {
                    tracing::warn!(target: "services::daemon_transport", attempt_id = %request.fence.attempt_id, %error, "owner attempt receipt was not retained");
                    return;
                }
            },
            Err(error) => error.to_string(),
        },
        Some(Err(error)) => format!("owner attempt receipt is invalid: {error}"),
        None => context.to_owned(),
    };
    let message = crate::integration_effects::check::tail_bytes(&reason, 4096);
    if let Err(error) = guard
        .record(
            serde_json::json!({"kind":"infrastructure","message":message,"head_sha":null,"rebase_in_progress":true}),
            db::IntegrationOperationState::Failed,
        )
        .await
    {
        tracing::warn!(target: "services::daemon_transport", attempt_id = %request.fence.attempt_id, %error, "attempt receipt was not recorded");
    }
}

impl DaemonWorkspaceClient {
    async fn bind_task_step_integration(
        &self,
        daemon_id: &str,
        method: &str,
        params: &mut Value,
    ) -> Result<Option<db::IntegrationEffectAdmission>> {
        let Some(db) = &self.db else {
            return Ok(None);
        };
        let rebase = method == METHOD_WORKSPACE_RESET
            && params.pointer("/operation/kind").and_then(Value::as_str) == Some("rebase_target");
        if method != METHOD_WORKSPACE_MERGE && !rebase {
            return Ok(None);
        }
        if params.pointer("/integration/kind").and_then(Value::as_str) != Some("task_step")
            || db::task_writer::current_task_step().is_none()
        {
            return Ok(None);
        }
        let placement = WorkspacePlacementRepo::get_by_id(
            &**db,
            params["placement_id"].as_str().unwrap_or_default(),
        )
        .await
        .map_err(ServiceError::from)?
        .ok_or_else(|| ServiceError::invalid_operation("integration placement missing"))?;
        if let Err(error) = self
            .reconcile_integration_attempts_on(daemon_id, Some(&placement.repo_location_id))
            .await
        {
            let error = match error {
                WorkspaceClientError::Transport(error) => error.to_string(),
                WorkspaceClientError::Daemon(error) => error.message,
            };
            tracing::warn!(target: "services::daemon_transport", %daemon_id, %error, "integration reconciliation before a Task-step effect failed");
        }
        let workspace = crate::workspace_backend::effect_workspace(&placement);
        let (kind, branch, target) = if rebase {
            let branch = params["operation"]["target_branch"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            let probe = WorkspaceInspectParams::Git {
                workspace: serde_json::from_value(params.clone())
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
                query: WorkspaceGitQuery::TargetHead {
                    branch: branch.clone(),
                },
                optional: false,
                limit: 4096,
            };
            let probe = self
                .request_once(
                    daemon_id,
                    METHOD_WORKSPACE_READ,
                    serde_json::to_value(probe).expect("probe serializes"),
                    Some(self.timeout),
                )
                .await?;
            let target = match serde_json::from_value::<WorkspaceInspectResult>(probe)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
            {
                WorkspaceInspectResult::Git {
                    output: Some(output),
                } => output.trim().to_owned(),
                _ => {
                    return Err(ServiceError::invalid_operation("rebase target has no HEAD").into())
                }
            };
            (db::IntegrationOperationKind::Rebase, branch, target)
        } else {
            (
                db::IntegrationOperationKind::Merge,
                params["target_branch"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                params["expected_target_sha"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
            )
        };
        let witness = serde_json::json!({"workspace":workspace,"target_branch":branch,"expected_head_sha":params["expected"]["sha"],"expected_target_sha":target,"handed_off_paths":params["handed_off_paths"],"handoff_conflicts":params["operation"]["handoff_conflicts"],"operation_id":params["operation_id"],"reviewed":params["reviewed_commit_sha"].as_str().map(|head| serde_json::json!({"commit_sha":head,"base_sha":target}))});
        let Some(request) = db
            .task_step_integration_request(&placement.workspace_id, kind, witness)
            .await
            .map_err(ServiceError::from)?
        else {
            return Ok(None);
        };
        let wire: WorkspaceIntegrationRequest =
            serde_json::from_value(serde_json::to_value(&request).expect("request serializes"))
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        params["integration"] =
            serde_json::to_value(WorkspaceIntegrationBinding::TaskStepEffect { request: wire })
                .expect("binding serializes");
        let mut admission = db
            .begin_integration_effect(request)
            .await
            .map_err(ServiceError::from)?;
        if let db::IntegrationEffectAdmission::Started(guard) = &mut admission {
            if let Some(reason) = guard.start().await.map_err(ServiceError::from)? {
                return Err(ServiceError::invalid_operation(format!(
                    "integration owner refused {reason:?}"
                ))
                .into());
            }
        }
        Ok(Some(admission))
    }

    /// Owner lookup is mandatory on reconnect and before a new integration
    /// request. No lookup runs Git effects; its receipt commits before ACK.
    pub async fn reconcile_integration_attempts(&self, daemon_id: &str) -> Result<()> {
        self.reconcile_integration_attempts_on(daemon_id, None)
            .await
    }

    async fn reconcile_integration_attempts_on(
        &self,
        daemon_id: &str,
        location: Option<&str>,
    ) -> Result<()> {
        let Some(db) = &self.db else {
            return Ok(());
        };
        for request in db
            .outstanding_integration_effects("daemon", Some(daemon_id))
            .await
            .map_err(ServiceError::from)?
        {
            if location.is_some_and(|location| {
                request.effect_owner()["location_id"].as_str() != Some(location)
            }) {
                continue;
            }
            let Some(guard) = db
                .lock_integration_reconciliation(&request)
                .await
                .map_err(ServiceError::from)?
            else {
                continue;
            };
            if !guard.started {
                guard
                    .record(
                        serde_json::json!({"kind":"not_performed"}),
                        db::IntegrationOperationState::Failed,
                    )
                    .await
                    .map_err(ServiceError::from)?;
                continue;
            }
            let wire: WorkspaceIntegrationRequest =
                serde_json::from_value(serde_json::to_value(&request).expect("request serializes"))
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
            // A transferred fast-forward ran in the default checkout alone:
            // its lookup names no worktree of the owner.
            let no_workspace = serde_json::json!({});
            let workspace = if request.witness["candidate_transfer_key"].is_string() {
                &no_workspace
            } else {
                &request.witness["workspace"]
            };
            let operation_id = request.witness["operation_id"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| wire.operation_id());
            let integration = if request.fence.lease_owner.starts_with("task-step:") {
                WorkspaceIntegrationBinding::TaskStepEffect { request: wire }
            } else {
                WorkspaceIntegrationBinding::Attempt { request: wire }
            };
            let params = WorkspaceReconcileParams {
                integration,
                workspace: WorkspaceHandleReference {
                    daemon_id: daemon_id.into(),
                    runtime_id: request.effect_owner()["runtime_id"]
                        .as_str()
                        .unwrap_or_default()
                        .into(),
                    placement_id: workspace["placement_id"]
                        .as_str()
                        .unwrap_or_default()
                        .into(),
                    workspace_handle: workspace["handle"].as_str().unwrap_or_default().into(),
                    generation: workspace["generation"].as_u64().unwrap_or_default(),
                },
                operation: WorkspaceReconcileOperation::Reconcile,
                operation_id,
            };
            let value = self
                .request_once(
                    daemon_id,
                    METHOD_WORKSPACE_DESCRIBE,
                    serde_json::to_value(&params).expect("lookup serializes"),
                    Some(self.timeout),
                )
                .await;
            let value = match value {
                Ok(value) => value,
                // The owner refused the lookup itself (workspace cleaned, a
                // newer generation): for a Task step that is its answer.
                Err(WorkspaceClientError::Daemon(error)) if db::is_task_step(&request) => {
                    tracing::warn!(target: "services::daemon_transport", %daemon_id, attempt_id = %request.fence.attempt_id, reason = %error.message, "owner refused an attempt lookup");
                    let message =
                        crate::integration_effects::check::tail_bytes(&error.message, 4096);
                    guard
                        .record(
                            serde_json::json!({"kind":"infrastructure","message":message,"head_sha":null,"rebase_in_progress":true}),
                            db::IntegrationOperationState::Failed,
                        )
                        .await
                        .map_err(ServiceError::from)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            // The owner answered. Whatever it said about this one attempt is
            // settled here; only a transport failure above stops the pass.
            let task_step = db::is_task_step(&request);
            let parsed = serde_json::from_value::<WorkspaceReconcileResult>(value)
                .map_err(|error| error.to_string());
            // What the owner's own records said about this intent before it
            // answered. Only a queue claim's lookup carries it.
            let owner_intent = parsed
                .as_ref()
                .ok()
                .and_then(|lookup| lookup.owner_fence.as_ref())
                .map(|fence| fence.intent);
            let lookup = parsed.and_then(|lookup| {
                if lookup.operation_id != params.operation_id {
                    return Err("owner lookup returned another attempt".to_owned());
                }
                let receipt = match &lookup.outcome {
                    WorkspaceReconcileOutcome::Result { result } => {
                        result.get("integration_receipt")
                    }
                    WorkspaceReconcileOutcome::Error { error } => error
                        .details
                        .as_ref()
                        .and_then(|details| details.get("integration_receipt")),
                }
                .cloned()
                .ok_or("owner lookup did not return an attempt receipt")?;
                let receipt: db::IntegrationEffectReceipt =
                    serde_json::from_value(receipt).map_err(|error| error.to_string())?;
                crate::integration_owner::validate_attempt_receipt(&receipt)
                    .map_err(|error| error.to_string())?;
                if receipt.request != request {
                    return Err("owner receipt belongs to another request".to_owned());
                }
                Ok((lookup.entry_id, receipt))
            });
            let (entry_id, receipt) = match lookup {
                Ok(found) => found,
                Err(reason) => {
                    tracing::warn!(target: "services::daemon_transport", %daemon_id, attempt_id = %request.fence.attempt_id, %reason, "owner attempt lookup was unusable");
                    // A queue lease's unknown effect stays for its owner. A
                    // Task-step intent settles: step recovery decides repeats.
                    if task_step {
                        let message = crate::integration_effects::check::tail_bytes(&reason, 4096);
                        guard
                            .record(
                                serde_json::json!({"kind":"infrastructure","message":message,"head_sha":null,"rebase_in_progress":true}),
                                db::IntegrationOperationState::Failed,
                            )
                            .await
                            .map_err(ServiceError::from)?;
                    } else if owner_intent == Some(IntegrationIntentRecord::NotPerformed) {
                        // The owner already knew this claim generation and
                        // holds no intent for the operation: it never ran
                        // there, and the owner has fenced it off. That is a
                        // settlement even without a usable receipt.
                        guard
                            .record(
                                serde_json::json!({"kind":"not_performed"}),
                                db::IntegrationOperationState::Failed,
                            )
                            .await
                            .map_err(ServiceError::from)?;
                    }
                    continue;
                }
            };
            // The owner had never been told of this claim generation, so its
            // empty journal proves nothing (lost or replaced owner state).
            // Whatever the receipt says, that is never a settlement and never
            // permission to repeat the effect: the intent stays uncertain for
            // its owner and the worker's reconcile timer keeps asking.
            if !task_step
                && owner_intent == Some(IntegrationIntentRecord::Unknown)
                && receipt.operation_state != db::IntegrationOperationState::Uncertain
            {
                tracing::warn!(target: "services::daemon_transport", %daemon_id, attempt_id = %request.fence.attempt_id, "owner has no record of the claim generation; its answer does not settle the attempt");
                continue;
            }
            // The owner could not prove a Task-step effect either way. Keeping
            // it uncertain would refuse every later effect on this checkout.
            if task_step && receipt.operation_state == db::IntegrationOperationState::Uncertain {
                guard
                    .record(
                        receipt.result.clone(),
                        db::IntegrationOperationState::Failed,
                    )
                    .await
                    .map_err(ServiceError::from)?;
            } else {
                guard
                    .record_owner_receipt(receipt)
                    .await
                    .map_err(ServiceError::from)?;
            }
            self.acknowledge(daemon_id, entry_id).await?;
        }
        Ok(())
    }
}

impl DaemonWorkspaceClient {
    /// Queue-only transport: writes the attempt sink, never execution/Task
    /// receipts. No worker calls this until queue activation is approved.
    pub async fn integration_effect(
        &self,
        daemon_id: &str,
        request: db::IntegrationEffectRequest,
        method: &str,
        mut params: Value,
        timeout: Duration,
    ) -> Result<db::IntegrationEffectReceipt> {
        let db = self.db.as_ref().ok_or_else(|| {
            ServiceError::invalid_operation("integration transport has no attempt sink")
        })?;
        // The effect is journaled by, and its receipt asked of, the owner
        // the request names. Sending it to any other daemon would leave a
        // receipt nobody looks for and an intent nobody can settle.
        let owner = request.effect_owner();
        if owner["owner_kind"] != "daemon" || owner["daemon_id"].as_str() != Some(daemon_id) {
            return Err(ServiceError::invalid_operation(
                "integration effect is addressed to another owner than the one it names",
            )
            .into());
        }
        self.reconcile_integration_attempts_on(
            daemon_id,
            request.effect_owner()["location_id"].as_str(),
        )
        .await?;
        let wire: WorkspaceIntegrationRequest =
            serde_json::from_value(serde_json::to_value(&request).expect("request serializes"))
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        params["integration"] = serde_json::to_value(WorkspaceIntegrationBinding::Attempt {
            request: wire.clone(),
        })
        .expect("binding serializes");
        params["operation_id"] = serde_json::json!(wire.operation_id());
        // The owner learns the claim generation before the intent can be
        // marked started. From then on an owner that answers a lookup of this
        // generation with "unknown" has lost state; an owner that merely never
        // received the effect frame answers "not performed". A refused or
        // undelivered announcement leaves no intent behind.
        self.announce_integration_fence(
            daemon_id,
            request.effect_owner()["runtime_id"]
                .as_str()
                .unwrap_or_default(),
            &wire.fence,
            None,
        )
        .await?;
        let mut guard = match db
            .begin_integration_effect(request.clone())
            .await
            .map_err(ServiceError::from)?
        {
            db::IntegrationEffectAdmission::Replay(receipt) => return Ok(receipt),
            db::IntegrationEffectAdmission::Refused(reason) => {
                return Err(ServiceError::invalid_operation(format!(
                    "integration owner refused {reason:?}"
                ))
                .into())
            }
            db::IntegrationEffectAdmission::Started(guard) => guard,
        };
        if let Some(reason) = guard.start().await.map_err(ServiceError::from)? {
            return Err(ServiceError::invalid_operation(format!(
                "integration owner refused {reason:?}"
            ))
            .into());
        }
        let value = match self
            .request_once(daemon_id, method, params, Some(timeout))
            .await
        {
            Ok(value) => value,
            Err(WorkspaceClientError::Daemon(error)) => {
                if let Some(receipt) = error
                    .details
                    .as_ref()
                    .and_then(|details| details.get("integration_receipt"))
                {
                    let receipt: db::IntegrationEffectReceipt =
                        serde_json::from_value(receipt.clone())
                            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
                    crate::integration_owner::validate_attempt_receipt(&receipt)?;
                    guard
                        .record_owner_receipt(receipt.clone())
                        .await
                        .map_err(ServiceError::from)?;
                    return Ok(receipt);
                }
                if error.code == "integration_owner_refused" {
                    return guard.record(serde_json::json!({"kind":"refused","reason":error.details.as_ref().map(|details| &details["refusal"])}), db::IntegrationOperationState::Failed).await.map_err(ServiceError::from).map_err(WorkspaceClientError::from);
                }
                return Err(WorkspaceClientError::Daemon(error));
            }
            Err(error) => return Err(error),
        };
        if value["operation_id"].as_str() != Some(&wire.operation_id()) {
            return Err(
                ServiceError::invalid_operation("owner returned another attempt identity").into(),
            );
        }
        let receipt: db::IntegrationEffectReceipt =
            serde_json::from_value(value["integration_receipt"].clone())
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        crate::integration_owner::validate_attempt_receipt(&receipt)?;
        guard
            .record_owner_receipt(receipt.clone())
            .await
            .map_err(ServiceError::from)?;
        self.acknowledge(
            daemon_id,
            value["entry_id"]
                .as_str()
                .ok_or_else(|| {
                    ServiceError::invalid_operation("owner receipt has no journal entry")
                })?
                .into(),
        )
        .await?;
        Ok(receipt)
    }
}

pub use object_transfer::DaemonObjectEndpoint;

fn validate_check_result(
    daemon_id: &str,
    operation_id: &str,
    result: &DaemonCheckResult,
) -> Result<()> {
    let matches = match result {
        DaemonCheckResult::Completed { receipt } => {
            receipt.operation_id == operation_id
                && receipt.owner.owner_kind == "daemon"
                && receipt.owner.machine_id.as_deref() == Some(daemon_id)
        }
        DaemonCheckResult::Running { operation_id: id }
        | DaemonCheckResult::Interrupted { operation_id: id }
        | DaemonCheckResult::Unknown { operation_id: id } => id == operation_id,
    };
    if !matches {
        return Err(
            ServiceError::invalid_operation("check owner receipt identity mismatch").into(),
        );
    }
    Ok(())
}

mod object_transfer;

#[cfg(test)]
pub(crate) mod tests;
