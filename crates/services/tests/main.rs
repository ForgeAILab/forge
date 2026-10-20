// Integration suites share one migration template and one link step.
// Env-mutating provisioning and shared-root Project-create tests stay isolated.
// Keep the original target name as the module for focused test filters.

struct NoopWakeTurns;

#[async_trait::async_trait]
impl services::wake_turns::WakeTurns for NoopWakeTurns {
    async fn prepare_attention(
        &self,
        _attention: &db::AttentionProjection,
        _dedupe_key: &str,
        _causation_id: Option<&str>,
        _depth: i64,
        _batch: &[db::AttentionProjection],
    ) -> services::Result<Option<services::wake_turns::PreparedWakeTurn>> {
        Ok(None)
    }

    fn build_prepared_turn(
        &self,
        _event: &db::DomainEvent,
        _input: &services::wake_turns::PreparedWakeTurn,
        _attention: &db::AttentionProjection,
    ) -> services::Result<db::AdmitAgentChatTurn> {
        Err(services::ServiceError::invalid_operation(
            "the core integration fixture never prepares a wake turn",
        ))
    }

    fn consumer_name(&self) -> &'static str {
        "core-integration-noop-wake-turns"
    }
}

fn noop_wake_turns() -> std::sync::Arc<dyn services::wake_turns::WakeTurns> {
    std::sync::Arc::new(NoopWakeTurns)
}

#[path = "suite/attention_workflow_semantics.rs"]
mod attention_workflow_semantics;
#[path = "suite/audit2_relay.rs"]
mod audit2_relay;
#[path = "suite/compatibility_fixtures.rs"]
mod compatibility_fixtures;
#[path = "suite/coordination_consumer.rs"]
mod coordination_consumer;
#[path = "suite/domain_event_latency.rs"]
mod domain_event_latency;
#[path = "suite/execution_liveness_characterization.rs"]
mod execution_liveness_characterization;
#[path = "suite/project_charter_command.rs"]
mod project_charter_command;
#[path = "suite/project_execution_setup.rs"]
mod project_execution_setup;
#[path = "suite/project_milestone_command.rs"]
mod project_milestone_command;
#[path = "suite/project_reconciliation.rs"]
mod project_reconciliation;
#[path = "suite/project_settings_validation.rs"]
mod project_settings_validation;
#[path = "suite/readiness_evidence_freshness.rs"]
mod readiness_evidence_freshness;
#[path = "suite/task_dependency_command.rs"]
mod task_dependency_command;
#[path = "suite/task_propose_command.rs"]
mod task_propose_command;
#[path = "suite/workspace_backend_characterization.rs"]
mod workspace_backend_characterization;

#[path = "suite/attention_continuity.rs"]
mod attention_continuity;
