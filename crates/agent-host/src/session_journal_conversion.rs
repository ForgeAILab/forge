//! Foreground admission and reuse of the runtime's deferred v3 import path.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use crate::{AgentHostError, SqliteProtectedRuntimeStore};
use agent_runtime::{
    core::{
        catalog::{ModelLimits, ResolvedModelProfile},
        checkpoint::CheckpointStore,
        error::{ErrorKind, RuntimeError},
        ids::SessionId,
        journal::*,
        provider::*,
        store::SessionStore,
    },
    harness::{LCM_COMPONENT_ID, LcmCoordinator, LcmCoordinatorPolicy, StaticLcmTimelineResolver},
    runtime::{CheckpointRecoveryPolicy, RuntimeBuilder, StartSession},
};
use async_trait::async_trait;
use db::{AgentContextScopeRepo, AgentSessionRepo};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

#[derive(Default)]
pub(crate) struct ConversionAdmission {
    sessions: Mutex<HashMap<SessionId, Weak<RwLock<()>>>>,
    #[cfg(feature = "test-support")]
    barrier: Mutex<Option<Arc<tokio::sync::Barrier>>>,
    #[cfg(feature = "test-support")]
    failure: Mutex<Option<RuntimeError>>,
}
impl ConversionAdmission {
    fn session(&self, id: &SessionId) -> Result<Arc<RwLock<()>>, RuntimeError> {
        let mut entries = self
            .sessions
            .lock()
            .map_err(|_| RuntimeError::internal("conversion admission unavailable"))?;
        entries.retain(|_, value| value.strong_count() > 0);
        if let Some(lock) = entries.get(id).and_then(Weak::upgrade) {
            return Ok(lock);
        }
        let lock = Arc::new(RwLock::new(()));
        entries.insert(id.clone(), Arc::downgrade(&lock));
        Ok(lock)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConversionOutcome {
    Converted,
    AlreadyNative,
    SkippedActive,
}

impl SqliteProtectedRuntimeStore {
    #[cfg(feature = "test-support")]
    pub fn pause_conversion_for_test(&self, barrier: Arc<tokio::sync::Barrier>) {
        *self.conversion_admission.barrier.lock().unwrap() = Some(barrier);
    }

    #[cfg(feature = "test-support")]
    pub fn fail_conversion_for_test(&self, error: RuntimeError) {
        *self.conversion_admission.failure.lock().unwrap() = Some(error);
    }

    /// Read admission preserves the runtime's existing foreground writer-conflict
    /// semantics. Only background conversion takes exclusive host admission.
    pub async fn foreground_session(
        &self,
        id: &SessionId,
    ) -> Result<OwnedRwLockReadGuard<()>, AgentHostError> {
        Ok(self
            .conversion_admission
            .session(id)
            .map_err(|_| AgentHostError::ProtectedPersistence)?
            .read_owned()
            .await)
    }

    pub async fn convert_legacy_session(
        self: &Arc<Self>,
        owner: &str,
        id: &SessionId,
    ) -> Result<ConversionOutcome, RuntimeError> {
        #[cfg(feature = "test-support")]
        if let Some(error) = self.conversion_admission.failure.lock().unwrap().take() {
            return Err(error);
        }
        let Ok(_admission) = self.conversion_admission.session(id)?.try_write_owned() else {
            return Ok(ConversionOutcome::SkippedActive);
        };
        #[cfg(feature = "test-support")]
        {
            let barrier = self.conversion_admission.barrier.lock().unwrap().take();
            if let Some(barrier) = barrier {
                barrier.wait().await;
                barrier.wait().await;
            }
        }
        let flags = self
            .database()
            .session_persistence_flags(owner)
            .await
            .map_err(|_| RuntimeError::internal("conversion probe failed"))?;
        if flags.ordinary_published || flags.retired {
            return Ok(ConversionOutcome::AlreadyNative);
        }

        // Check runtime writers too, including absent-head import reservations
        // and callers outside NativeAgentRuntimeBackend. Never revoke a writer.
        let mut leases = WriterChecks::default();
        for engine in [self.journals().protected(), self.journals().ordinary()] {
            let nonce = db::new_uuid_v4();
            let opened = engine
                .open(
                    id,
                    JournalOpen {
                        root: JournalRootSelection::Latest,
                        expected_revision: None,
                        operation: JournalOperationId::new(format!("conversion-check:{nonce}")),
                        pin: JournalPinId::new(nonce),
                        intent: JournalOpenIntent::ExclusiveWriter,
                    },
                )
                .await;
            match opened {
                Ok(lease) => leases.0.push((engine, lease)),
                Err(error) => {
                    leases.close().await?;
                    return if error.kind == ErrorKind::Conflict {
                        Ok(ConversionOutcome::SkippedActive)
                    } else {
                        Err(error)
                    };
                }
            }
        }
        leases.close().await?;

        // Resolve LCM only when the real legacy state requires it. The runtime
        // importer still decodes and validates both stores itself before publish.
        let snapshot = SessionStore::load(&**self, id).await?;
        let checkpoint = CheckpointStore::load_latest(&**self, id).await?;
        let needs_lcm = snapshot
            .iter()
            .chain(checkpoint.iter().map(|c| &c.snapshot))
            .any(|s| {
                s.extension_state.contains_key(LCM_COMPONENT_ID)
                    || s.extension_state.contains_key("harness.semantic_summary")
            });
        drop(snapshot);
        drop(checkpoint);
        let model = ModelId::new("forge-conversion");
        let mut builder = RuntimeBuilder::new(model.clone())
            .provider_name("forge-conversion")
            .provider(Arc::new(ConversionProvider))
            .model_profile(ResolvedModelProfile::explicit(
                "forge-conversion",
                model,
                ModelLimits::new(32_000, 28_000, 4_000),
            ))
            .session_store(self.clone())
            .checkpoint_store(self.clone());
        if needs_lcm {
            let db = self.database();
            let session = db
                .get_agent_session(owner)
                .await
                .map_err(|_| RuntimeError::internal("conversion owner lookup failed"))?
                .ok_or_else(|| RuntimeError::not_found("conversion owner removed"))?;
            let scope = db
                .get_context_scope(&session.context_scope_id)
                .await
                .map_err(|_| RuntimeError::internal("conversion scope lookup failed"))?
                .ok_or_else(|| RuntimeError::not_found("conversion scope removed"))?;
            let store = self
                .lcm_store_for_runtime_session(id.as_str(), &scope.scope_type, &scope.scope_id)
                .await
                .map_err(|_| RuntimeError::conflict("conversion LCM binding deferred"))?;
            let binding = store
                .runtime_binding(id.clone())
                .map_err(|_| RuntimeError::conflict("conversion LCM binding deferred"))?;
            builder = builder.lcm(Arc::new(LcmCoordinator::new(
                Arc::new(store),
                Arc::new(crate::DeterministicLcmSummaryModel::default()),
                Arc::new(StaticLcmTimelineResolver::new(binding)),
                LcmCoordinatorPolicy {
                    input_budget_tokens: 28_000,
                    ..Default::default()
                },
            )?));
        }
        let runtime = builder.build()?;
        let result = runtime
            .start_session(
                StartSession::resume(id.clone())
                    .with_checkpoint_recovery(CheckpointRecoveryPolicy::Defer),
            )
            .await;
        // Drop, rather than shutdown/persist: conversion must not manufacture a
        // new boundary. The shared engine closes these I/O-free leases on drop.
        let error = match result {
            Ok(session) => {
                drop(session);
                None
            }
            Err(error) => Some(error),
        };
        drop(runtime);
        let flags = self
            .database()
            .session_persistence_flags(owner)
            .await
            .map_err(|_| RuntimeError::internal("conversion publication probe failed"))?;
        if flags.ordinary_published && !flags.retired {
            // Import validation/publication precedes live ability activation.
            // A missing current tool composition may refuse activation after a
            // successful conversion; the next real turn supplies that composition.
            return Ok(ConversionOutcome::Converted);
        }
        Err(error.unwrap_or_else(|| RuntimeError::not_found("conversion found no legacy state")))
    }
}

#[derive(Debug)]
struct ConversionProvider;
#[async_trait]
impl Provider for ConversionProvider {
    fn describe(&self) -> Vec<ModelDescriptor> {
        Vec::new()
    }
    fn capabilities(&self, _: &ModelId) -> Option<Capabilities> {
        Some(Capabilities::basic_streaming())
    }
    async fn stream(
        &self,
        _: ProviderRequest,
        _: ProviderCallContext,
    ) -> Result<ProviderStream, ProviderError> {
        Err(ProviderError::new(
            ProviderErrorKind::Unsupported,
            "conversion cannot call a provider",
        ))
    }
}

#[derive(Default)]
struct WriterChecks(
    Vec<(
        JournalEngine<crate::session_journal::ForgeJournalStorage>,
        JournalLease,
    )>,
);
impl WriterChecks {
    async fn close(&mut self) -> Result<(), RuntimeError> {
        while let Some((engine, lease)) = self.0.pop() {
            engine.close(lease).await?;
        }
        Ok(())
    }
}
impl Drop for WriterChecks {
    fn drop(&mut self) {
        for (engine, lease) in self.0.drain(..) {
            let mut close = Box::pin(async move {
                let _ = engine.close(lease).await;
            });
            if futures_util::FutureExt::now_or_never(&mut close).is_none() {
                tokio::spawn(close);
            }
        }
    }
}
