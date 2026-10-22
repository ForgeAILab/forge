#![forbid(unsafe_code)]

// Core domain types remain directly available to the moved modules while the
// concrete agent implementations below own the crate's public surface.
use services::*;

#[cfg(test)]
mod receipt_characterization_tests;
#[cfg(test)]
mod test_support;

pub mod runtime;
pub use runtime::{
    ForgeRuntime, ForgeRuntimeBuilder, RuntimeAssemblyMode, RuntimeSupervisor, RuntimeTaskHandle,
};

pub mod agent_chat_memory_consumer;
pub mod agent_chat_policy;
pub mod agent_chat_service;
pub mod agent_chat_turn_policy;
pub mod agent_chat_turn_worker;
pub mod agent_inquiry_runner;
pub mod agent_inquiry_service;
pub mod agent_turn_admission;
pub(crate) mod chat_usage;
pub mod context_manifest;
pub mod default_agents;
pub mod embedded_agent_service;
pub mod embedded_task_executor;
pub mod main_chat_topic;
pub mod main_genesis_commands;
pub mod main_orchestration_actions;
pub mod main_orchestration_queries;
pub mod native_tools;
pub mod project_agent_actions;
pub mod prompt_preview;
pub mod provider_authorization;
pub mod topic_rotation;
pub mod wake_turn_consumer;

pub use agent_chat_memory_consumer::{memory_consumer_name, AgentChatMemoryConsumer};
pub use agent_chat_policy::{AgentChatOperation, AgentChatPolicyError, AgentChatScope};
pub use agent_chat_service::{
    AdmittedAgentChatMessage, AgentChatHandoffOutcome, AgentChatService, CancelAgentChatTurnInput,
    CommittedAgentChatResponse, CreateAgentHandoffInput, RetryAgentChatTurnInput,
    SendAgentChatMessageInput, SetMainAgentBindingInput, SetProjectAgentBindingInput,
};
pub use agent_chat_turn_policy::{
    bounded_error as bounded_agent_chat_error, claim as claim_agent_chat_turn,
    failure_after_claim as fail_agent_chat_turn_after_claim,
    recover_expired as recover_expired_agent_chat_turn,
    FailureDecision as AgentChatFailureDecision, LeaseDecision as AgentChatLeaseDecision,
};
pub use agent_chat_turn_worker::{
    AgentChatTurnLogRoot, AgentChatTurnRunOutcome, AgentChatTurnRunner, AgentChatTurnWorker,
    CliAgentChatSessionBackend, CompletedAgentChatTurn, FederatedAgentChatTurnRunner,
};
pub use agent_inquiry_runner::{
    EmbeddedInquiryRunner, InquiryOutcome, InquiryRequest, InquiryRunner,
};
pub use agent_turn_admission::{
    admission_digest, content_digest, handoff_admission_digest, handoff_content_digest,
    handoff_content_digest_with_sources, AgentResponderStore, AgentTurnAdmissionInput,
    AgentTurnAdmissionService, AgentTurnAdmitInput, AgentTurnPrepareInput, AgentTurnReadiness,
    AgentTurnTrigger, PreparedAgentTurnAdmission, ResolvedAgentResponder,
};
pub use context_manifest::{
    fragment_fingerprint, ContextManifestInput, ContextManifestService, ContextSourceInput,
};
pub use default_agents::ensure_default_agents;
pub use embedded_agent_service::{EmbeddedAgentService, ProviderEntryTestOutcome};
pub use embedded_task_executor::{EmbeddedTaskExecutor, TaskExecutorRouter};
pub use main_chat_topic::{MainChatTopicRotation, MainChatTopicService, StartMainChatTopicInput};
pub use main_genesis_commands::{
    MainGenesisCharterDraftRequest, MainGenesisCharterDraftResult, MainGenesisCommandService,
    MainGenesisDraftCommandInput, MainGenesisDraftPrincipal,
    MainGenesisProjectAgentSelectCommandInput, MainGenesisProjectAgentSelectRequest,
    MainGenesisProjectAgentSelectResult, MainGenesisStartCommandInput, MainGenesisStartPrincipal,
    MainGenesisStartRequest, MainGenesisStartResult,
};
pub use main_orchestration_actions::{
    ExecuteMainOrchestrationActionInput, MainOrchestrationActionService,
};
pub use main_orchestration_queries::MainOrchestrationQueryService;
pub use native_tools::CoordinationToolProvider;
pub use project_agent_actions::{
    DirectProjectCommandResult, ExecuteDirectProjectCommandInput,
    ExecuteProjectOrchestrationActionInput, ProjectOrchestrationActionService,
};
pub use prompt_preview::preview_effective_prompt;
pub use provider_authorization::ProviderAuthorizationService;
pub use topic_rotation::{TopicRotationCoordinator, TopicRotator};
pub use wake_turn_consumer::{wake_turn_consumer_name, WakeTurnConsumer, WakeTurnRun};
