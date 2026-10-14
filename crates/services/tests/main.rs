// Integration suites share one migration template and one link step.
// Env-mutating provisioning and shared-root Project-create tests stay isolated.
// Keep the original target name as the module for focused test filters.

#[path = "suite/attention_service.rs"]
mod attention_service;
#[path = "suite/attention_workflow_semantics.rs"]
mod attention_workflow_semantics;
#[path = "suite/audit2_relay.rs"]
mod audit2_relay;
#[path = "suite/command_allowlist_resolution.rs"]
mod command_allowlist_resolution;
#[path = "suite/compatibility_fixtures.rs"]
mod compatibility_fixtures;
#[path = "suite/coordination_consumer.rs"]
mod coordination_consumer;
#[path = "suite/direct_command_classification.rs"]
mod direct_command_classification;
#[path = "suite/domain_event_latency.rs"]
mod domain_event_latency;
#[path = "suite/embedded_agent_authorization.rs"]
mod embedded_agent_authorization;
#[path = "suite/execution_liveness_characterization.rs"]
mod execution_liveness_characterization;
#[path = "suite/main_orchestration_queries.rs"]
mod main_orchestration_queries;
#[path = "suite/memory_service.rs"]
mod memory_service;
#[path = "suite/native_chat_lcm_compaction.rs"]
mod native_chat_lcm_compaction;
#[path = "suite/native_chat_tool_exchanges.rs"]
mod native_chat_tool_exchanges;
#[path = "suite/native_scope_composition.rs"]
mod native_scope_composition;
#[path = "suite/plan04_adaptive_envelope.rs"]
mod plan04_adaptive_envelope;
#[path = "suite/project_charter_command.rs"]
mod project_charter_command;
#[path = "suite/project_decision_command.rs"]
mod project_decision_command;
#[path = "suite/project_evidence_command.rs"]
mod project_evidence_command;
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
#[path = "suite/revised_chat_policy.rs"]
mod revised_chat_policy;
#[path = "suite/revised_scope_adversarial.rs"]
mod revised_scope_adversarial;
#[path = "suite/security_adversarial.rs"]
mod security_adversarial;
#[path = "suite/task02_agent_prose.rs"]
mod task02_agent_prose;
#[path = "suite/task_dependency_command.rs"]
mod task_dependency_command;
#[path = "suite/task_propose_command.rs"]
mod task_propose_command;
#[path = "suite/wake_turn_consumer.rs"]
mod wake_turn_consumer;
#[path = "suite/workspace_backend_characterization.rs"]
mod workspace_backend_characterization;
