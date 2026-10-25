//! Object transfer endpoints and daemon integration transfer requests.

use super::*;

/// One daemon end of an object transfer.
#[derive(Debug, Clone, Copy)]
pub struct DaemonObjectEndpoint<'a> {
    pub daemon_id: &'a str,
    pub runtime_id: &'a str,
    pub repo_location_id: &'a str,
}

impl DaemonWorkspaceClient {
    fn transfer_refusal(error: WorkspaceClientError) -> Result<ObjectTransferRefusal> {
        match error {
            WorkspaceClientError::Daemon(error) if error.code == OBJECT_TRANSFER_REFUSED => error
                .details
                .as_ref()
                .and_then(|details| serde_json::from_value(details["refusal"].clone()).ok())
                .ok_or(WorkspaceClientError::Daemon(error)),
            error => Err(error),
        }
    }

    async fn integration_request<P: Serialize + Sync, R: DeserializeOwned>(
        &self,
        daemon_id: &str,
        method: &str,
        params: &P,
    ) -> Result<R> {
        let params = serde_json::to_value(params).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid integration request: {error}"))
        })?;
        // Like checks, these bypass the workspace mutation recorders: none of
        // them is a journaled workspace effect.
        let value = self
            .request_once(daemon_id, method, params, Some(self.timeout))
            .await?;
        serde_json::from_value(value).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid integration owner reply: {error}"))
                .into()
        })
    }

    /// The first message of a queue claim generation: the owner records the
    /// fence as its high-water mark and reports the one it held before.
    /// `live_queue_ids`, when given, lets the owner drop fences of queues that
    /// no longer target it. An older claim is refused
    /// (`integration_owner_refused`, refusal `stale_fence`).
    pub async fn announce_integration_fence(
        &self,
        daemon_id: &str,
        runtime_id: &str,
        fence: &IntegrationOwnerFence,
        live_queue_ids: Option<Vec<String>>,
    ) -> Result<IntegrationAnnounceResult> {
        self.integration_request(
            daemon_id,
            METHOD_INTEGRATION_ANNOUNCE,
            &IntegrationAnnounceParams {
                daemon_id: daemon_id.into(),
                runtime_id: runtime_id.into(),
                fence: fence.clone(),
                live_queue_ids,
            },
        )
        .await
    }

    /// The receipt of an import this key already completed on the owner.
    pub async fn imported_objects(
        &self,
        target: DaemonObjectEndpoint<'_>,
        fence: &IntegrationOwnerFence,
        key: &str,
        expected_tip_sha: &str,
    ) -> Result<crate::integration_owner::ObjectTransferOutcome<Option<ObjectImportReceipt>>> {
        use crate::integration_owner::ObjectTransferOutcome as Outcome;
        let params = ImportObjectsParams {
            daemon_id: target.daemon_id.into(),
            runtime_id: target.runtime_id.into(),
            fence: fence.clone(),
            key: key.into(),
            repo_location_id: target.repo_location_id.into(),
            expected_tip_sha: expected_tip_sha.into(),
            max_bytes: MAX_OBJECT_TRANSFER_BYTES,
            chunk: None,
        };
        match self
            .integration_request(target.daemon_id, METHOD_INTEGRATION_IMPORT_OBJECTS, &params)
            .await
        {
            Ok(ImportObjectsResult::Imported { receipt }) => Ok(Outcome::Done(Some(receipt))),
            Ok(_) => Ok(Outcome::Done(None)),
            Err(error) => Ok(Outcome::Refused(Self::transfer_refusal(error)?)),
        }
    }

    /// Pull the bundle for `want` minus `have` from a daemon checkout into
    /// `dest`, chunk by chunk, and verify its size and digest. A refused,
    /// failed or cancelled export removes `dest` and the owner's staging.
    #[allow(clippy::too_many_arguments)]
    pub async fn export_objects(
        &self,
        source: DaemonObjectEndpoint<'_>,
        fence: &IntegrationOwnerFence,
        key: &str,
        have: &[String],
        want: &str,
        dest: &std::path::Path,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<crate::integration_owner::ObjectTransferOutcome<ObjectExportReceipt>> {
        use crate::integration_owner::ObjectTransferOutcome as Outcome;
        let pull = async {
            let mut file: Option<tokio::fs::File> = None;
            let mut digest = Sha256::new();
            let mut offset = 0;
            loop {
                let chunk: ExportObjectsResult = self
                    .integration_request(
                        source.daemon_id,
                        METHOD_INTEGRATION_EXPORT_OBJECTS,
                        &ExportObjectsParams {
                            daemon_id: source.daemon_id.into(),
                            runtime_id: source.runtime_id.into(),
                            fence: fence.clone(),
                            key: key.into(),
                            repo_location_id: source.repo_location_id.into(),
                            have: have.to_vec(),
                            want: want.into(),
                            max_bytes: MAX_OBJECT_TRANSFER_BYTES,
                            offset,
                        },
                    )
                    .await?;
                let invalid = |reason: &str| {
                    WorkspaceClientError::from(ServiceError::invalid_operation(format!(
                        "invalid object export reply: {reason}"
                    )))
                };
                let data =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &chunk.data)
                        .map_err(|_| invalid("chunk is not base64"))?;
                if chunk.receipt.key != key
                    || chunk.receipt.tip_sha != want
                    || chunk.offset != offset
                    || chunk.receipt.total_bytes > MAX_OBJECT_TRANSFER_BYTES
                    || data.len() as u64 > MAX_OBJECT_TRANSFER_CHUNK_BYTES
                    || offset + data.len() as u64 > chunk.receipt.total_bytes
                    || (data.is_empty() && !chunk.eof)
                {
                    return Err(invalid("chunk does not continue the transfer"));
                }
                if !data.is_empty() {
                    use tokio::io::AsyncWriteExt;
                    if file.is_none() {
                        file = Some(
                            tokio::fs::File::create(dest)
                                .await
                                .map_err(crate::integration_owner::transfer_io)?,
                        );
                    }
                    let file = file.as_mut().expect("opened above");
                    file.write_all(&data)
                        .await
                        .map_err(crate::integration_owner::transfer_io)?;
                    digest.update(&data);
                    offset += data.len() as u64;
                }
                if chunk.eof {
                    if offset != chunk.receipt.total_bytes
                        || hex::encode(digest.finalize()) != chunk.receipt.sha256
                    {
                        return Err(invalid("bundle does not match its receipt"));
                    }
                    if let Some(file) = file.as_mut() {
                        file.sync_all()
                            .await
                            .map_err(crate::integration_owner::transfer_io)?;
                    }
                    return Ok(chunk.receipt);
                }
            }
        };
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => None,
            result = pull => Some(result),
        };
        let outcome = match result {
            Some(Ok(receipt)) => return Ok(Outcome::Done(receipt)),
            Some(Err(error)) => Self::transfer_refusal(error).map(Outcome::Refused),
            None => Ok(Outcome::Cancelled),
        };
        let _ = tokio::fs::remove_file(dest).await;
        self.release_objects(source.daemon_id, source.runtime_id, key)
            .await;
        outcome
    }

    /// Push a verified bundle to a daemon checkout and import it there under
    /// `refs/forge/integration/<key>`. `bundle` is ignored for an export of
    /// zero bytes. A refused, failed or cancelled import leaves no staging
    /// and no change on the owner. A transfer is never resumed part way: a
    /// repeated call (after a lost connection, say) sends the bundle again
    /// from its first byte, which makes the owner drop what it had staged.
    pub async fn import_objects(
        &self,
        target: DaemonObjectEndpoint<'_>,
        fence: &IntegrationOwnerFence,
        export: &ObjectExportReceipt,
        bundle: &std::path::Path,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<crate::integration_owner::ObjectTransferOutcome<ObjectImportReceipt>> {
        use crate::integration_owner::ObjectTransferOutcome as Outcome;
        if export.total_bytes > MAX_OBJECT_TRANSFER_BYTES {
            // Refused here, before a byte is sent to the target.
            return Ok(Outcome::Refused(ObjectTransferRefusal::TooLarge {
                bytes: export.total_bytes,
                max_bytes: MAX_OBJECT_TRANSFER_BYTES,
            }));
        }
        let push = async {
            use tokio::io::AsyncReadExt;
            let mut file = if export.total_bytes > 0 {
                Some(
                    tokio::fs::File::open(bundle)
                        .await
                        .map_err(crate::integration_owner::transfer_io)?,
                )
            } else {
                None
            };
            let mut offset = 0;
            loop {
                let length = (export.total_bytes - offset).min(MAX_OBJECT_TRANSFER_CHUNK_BYTES);
                let mut data = vec![0; length as usize];
                if let Some(file) = file.as_mut() {
                    file.read_exact(&mut data)
                        .await
                        .map_err(crate::integration_owner::transfer_io)?;
                }
                let last = offset + length >= export.total_bytes;
                let reply: ImportObjectsResult = self
                    .integration_request(
                        target.daemon_id,
                        METHOD_INTEGRATION_IMPORT_OBJECTS,
                        &ImportObjectsParams {
                            daemon_id: target.daemon_id.into(),
                            runtime_id: target.runtime_id.into(),
                            fence: fence.clone(),
                            key: export.key.clone(),
                            repo_location_id: target.repo_location_id.into(),
                            expected_tip_sha: export.tip_sha.clone(),
                            max_bytes: MAX_OBJECT_TRANSFER_BYTES,
                            chunk: Some(ObjectChunk {
                                total_bytes: export.total_bytes,
                                sha256: export.sha256.clone(),
                                offset,
                                data: base64::Engine::encode(
                                    &base64::engine::general_purpose::STANDARD,
                                    &data,
                                ),
                                last,
                            }),
                        },
                    )
                    .await?;
                offset += length;
                match reply {
                    ImportObjectsResult::Imported { receipt }
                        if receipt.key == export.key && receipt.tip_sha == export.tip_sha =>
                    {
                        return Ok(receipt)
                    }
                    ImportObjectsResult::Receiving { received_bytes }
                        if !last && received_bytes == offset => {}
                    _ => {
                        return Err(WorkspaceClientError::from(ServiceError::invalid_operation(
                            "invalid object import reply: it does not continue the transfer",
                        )))
                    }
                }
            }
        };
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => None,
            result = push => Some(result),
        };
        let outcome = match result {
            Some(Ok(receipt)) => return Ok(Outcome::Done(receipt)),
            Some(Err(error)) => Self::transfer_refusal(error).map(Outcome::Refused),
            None => Ok(Outcome::Cancelled),
        };
        self.release_objects(target.daemon_id, target.runtime_id, &export.key)
            .await;
        outcome
    }

    /// Drop a key's staging on an owner. Best effort: the owner also bounds
    /// staging by age.
    pub async fn release_objects(&self, daemon_id: &str, runtime_id: &str, key: &str) {
        let released: Result<ReleaseObjectsResult> = self
            .integration_request(
                daemon_id,
                METHOD_INTEGRATION_RELEASE_OBJECTS,
                &ReleaseObjectsParams {
                    daemon_id: daemon_id.into(),
                    runtime_id: runtime_id.into(),
                    key: key.into(),
                    attempt: None,
                },
            )
            .await;
        if let Err(error) = released {
            tracing::warn!(target: "services::daemon_transport", %daemon_id, %key, %error, "object transfer staging was not released");
        }
    }

    /// Read-only facts about a default checkout a daemon holds: the target
    /// branch's tip, whether `candidate_sha` has landed in it, and whether
    /// the checkout has uncommitted changes.
    pub async fn target_facts(
        &self,
        target: DaemonObjectEndpoint<'_>,
        target_branch: &str,
        candidate_sha: &str,
    ) -> Result<IntegrationTargetFactsResult> {
        self.integration_request(
            target.daemon_id,
            METHOD_INTEGRATION_TARGET_FACTS,
            &IntegrationTargetFactsParams {
                daemon_id: target.daemon_id.into(),
                runtime_id: target.runtime_id.into(),
                repo_location_id: target.repo_location_id.into(),
                target_branch: target_branch.into(),
                candidate_sha: candidate_sha.into(),
            },
        )
        .await
    }

    /// Delete every ref an attempt imported in one checkout of an owner
    /// (`refs/forge/integration/<attempt>-*`) and the attempt's staging.
    /// Idempotent. Returns how many refs were deleted.
    pub async fn release_attempt_objects(
        &self,
        target: DaemonObjectEndpoint<'_>,
        attempt_id: &str,
    ) -> Result<u32> {
        let released: ReleaseObjectsResult = self
            .integration_request(
                target.daemon_id,
                METHOD_INTEGRATION_RELEASE_OBJECTS,
                &ReleaseObjectsParams {
                    daemon_id: target.daemon_id.into(),
                    runtime_id: target.runtime_id.into(),
                    // The handler takes one key; the attempt's keys all start
                    // with its id.
                    key: api_types::object_transfer_key(
                        attempt_id,
                        0,
                        api_types::ObjectTransferDirection::Inbound,
                    ),
                    attempt: Some(api_types::ReleaseAttemptRefs {
                        attempt_id: attempt_id.into(),
                        repo_location_id: target.repo_location_id.into(),
                    }),
                },
            )
            .await?;
        Ok(released.removed_refs)
    }
}
