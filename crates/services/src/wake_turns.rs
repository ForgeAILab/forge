//! Core contract for preparing Agent Chat turns during Attention admission.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use db::{AdmitAgentChatTurn, AttentionProjection, DomainEvent};

use crate::Result;

type WakeAdmissionBuilder =
    dyn Fn(&DomainEvent, &AttentionProjection) -> Result<AdmitAgentChatTurn> + Send + Sync;

/// Prepared agents state stays behind a callback. Admission is built only
/// after Attention has read the current incident inside its transaction.
#[derive(Clone)]
pub struct PreparedWakeTurn {
    pub binding_id: Option<String>,
    pub binding_version: Option<i64>,
    pub dedupe_key: String,
    admission: Arc<WakeAdmissionBuilder>,
}

impl fmt::Debug for PreparedWakeTurn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedWakeTurn")
            .finish_non_exhaustive()
    }
}

impl PreparedWakeTurn {
    pub fn new(
        binding_id: Option<String>,
        binding_version: Option<i64>,
        dedupe_key: String,
        admission: impl Fn(&DomainEvent, &AttentionProjection) -> Result<AdmitAgentChatTurn>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self {
            binding_id,
            binding_version,
            dedupe_key,
            admission: Arc::new(admission),
        }
    }

    pub fn build_admission(
        &self,
        event: &DomainEvent,
        attention: &AttentionProjection,
    ) -> Result<AdmitAgentChatTurn> {
        (self.admission)(event, attention)
    }
}

#[async_trait]
pub trait WakeTurns: Send + Sync {
    async fn prepare_attention(
        &self,
        attention: &AttentionProjection,
        dedupe_key: &str,
        causation_id: Option<&str>,
        depth: i64,
        batch: &[AttentionProjection],
    ) -> Result<Option<PreparedWakeTurn>>;

    fn build_prepared_turn(
        &self,
        event: &DomainEvent,
        input: &PreparedWakeTurn,
        attention: &AttentionProjection,
    ) -> Result<AdmitAgentChatTurn>;

    fn consumer_name(&self) -> &'static str;
}
