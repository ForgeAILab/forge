use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Durable lifecycle of a typed Genesis discovery interaction.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProductGenesisLifecycle {
    Discovering,
    ReadyForProject,
    HandedOff,
    Cancelled,
}

/// The intended depth of a Product Genesis discovery session.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum ProductMaturity {
    Prototype,
    #[default]
    Mvp,
    Production,
    Critical,
}

impl ProductMaturity {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prototype => "prototype",
            Self::Mvp => "mvp",
            Self::Production => "production",
            Self::Critical => "critical",
        }
    }
}

impl ProductGenesisLifecycle {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Discovering => "discovering",
            Self::ReadyForProject => "ready_for_project",
            Self::HandedOff => "handed_off",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ProductGenesisSession {
    pub id: String,
    pub account_id: String,
    pub main_chat_id: String,
    pub prompt_revision: String,
    pub maturity: ProductMaturity,
    pub initial_idea: Option<String>,
    pub lifecycle: ProductGenesisLifecycle,
    pub source_message_ids: Vec<String>,
    pub preferred_project_agent_identity_id: Option<String>,
    pub charter_id: Option<String>,
    pub charter_revision_id: Option<String>,
    pub charter_approval_id: Option<String>,
    pub charter_version: i64,
    pub project_id: Option<String>,
    pub handoff_id: Option<String>,
    pub failure_reason: Option<String>,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct StartProductGenesisRequest {
    pub maturity: Option<ProductMaturity>,
    pub initial_idea: Option<String>,
    pub preferred_project_agent_identity_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct CancelProductGenesisRequest {
    pub expected_version: i64,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
#[serde(deny_unknown_fields)]
pub struct ApplyProductGenesisGuidedSetupRequest {
    pub expected_version: i64,
    pub maturity: Option<ProductMaturity>,
    pub preferred_project_agent_identity_id: Option<String>,
    pub provenance: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ProductGenesisStartResponse {
    pub session: ProductGenesisSession,
    pub main_chat_id: String,
    pub admitted_turn_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct ProductGenesisActiveResponse {
    pub session: Option<ProductGenesisSession>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn genesis_mutation_requests_reject_unknown_fields() {
        let start = serde_json::from_value::<StartProductGenesisRequest>(json!({
            "maturity": "mvp",
            "unknown": true,
        }));
        assert!(start.is_err());

        let cancel = serde_json::from_value::<CancelProductGenesisRequest>(json!({
            "expected_version": 1,
            "unknown": true,
        }));
        assert!(cancel.is_err());
    }
}
