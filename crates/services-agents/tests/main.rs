// Cross-service acceptance coverage lives above the core.
mod test_services {
    pub use ::services::*;
    pub use ::services_agents::*;
}

#[path = "suite/agent_chat_turn_worker.rs"]
mod agent_chat_turn_worker;
#[path = "suite/attention_service.rs"]
mod attention_service;
#[path = "suite/command_allowlist_resolution.rs"]
mod command_allowlist_resolution;
#[path = "suite/direct_command_classification.rs"]
mod direct_command_classification;
#[path = "suite/embedded_agent_authorization.rs"]
mod embedded_agent_authorization;
#[path = "suite/main_chat_topic.rs"]
mod main_chat_topic;
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
#[path = "suite/owner_outbox.rs"]
mod owner_outbox;
#[path = "suite/plan04_adaptive_envelope.rs"]
mod plan04_adaptive_envelope;
#[path = "suite/project_decision_command.rs"]
mod project_decision_command;
#[path = "suite/project_evidence_command.rs"]
mod project_evidence_command;
#[path = "suite/revised_chat_policy.rs"]
mod revised_chat_policy;
#[path = "suite/revised_scope_adversarial.rs"]
mod revised_scope_adversarial;
#[path = "suite/security_adversarial.rs"]
mod security_adversarial;
#[path = "suite/task02_agent_prose.rs"]
mod task02_agent_prose;
#[path = "suite/wake_turn_consumer.rs"]
mod wake_turn_consumer;

#[path = "suite/session_journal_retention.rs"]
mod session_journal_retention;

#[path = "suite/session_journal_conversion.rs"]
mod session_journal_conversion;

#[path = "suite/session_journal_dryrun.rs"]
mod session_journal_dryrun;
