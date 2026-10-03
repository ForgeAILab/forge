use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use ts_rs::TS;

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SettingsResponse {
    pub config_path: String,
    pub restart_required: bool,
    pub settings: Vec<ForgeSettingResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ForgeSettingResponse {
    pub key: String,
    #[ts(type = "unknown")]
    pub value: Value,
    #[ts(type = "unknown")]
    pub effective_value: Value,
    pub restart_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, Default)]
#[ts(export)]
pub struct UpdateSettingsRequest {
    pub forge: Option<UpdateForgePathsRequest>,
    pub server: Option<UpdateServerSettingsRequest>,
    pub workspace: Option<UpdateWorkspaceSettingsRequest>,
    pub agent: Option<UpdateAgentSettingsRequest>,
    pub project: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, Default)]
#[ts(export)]
pub struct UpdateForgePathsRequest {
    pub data_dir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, Default)]
#[ts(export)]
pub struct UpdateServerSettingsRequest {
    #[serde(
        default,
        deserialize_with = "deserialize_optional_u32_update",
        skip_serializing_if = "Option::is_none"
    )]
    #[ts(optional, type = "number | null")]
    pub max_concurrent_runs: Option<Option<u32>>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_u32_update",
        skip_serializing_if = "Option::is_none"
    )]
    #[ts(optional, type = "number | null")]
    pub build_jobs_per_run: Option<Option<u32>>,
    pub run_nice: Option<u32>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_u32_update",
        skip_serializing_if = "Option::is_none"
    )]
    #[ts(optional, type = "number | null")]
    pub usage_index_budget_mb: Option<Option<u32>>,
    pub bind: Option<String>,
    pub mcp_enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, Default)]
#[ts(export)]
pub struct UpdateWorkspaceSettingsRequest {
    pub root: Option<String>,
    pub cleanup_delay_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, Default)]
#[ts(export)]
pub struct UpdateAgentSettingsRequest {
    pub max_concurrent_tasks: Option<u32>,
    pub heartbeat_interval_seconds: Option<u64>,
    pub max_missed_heartbeats: Option<u32>,
}

fn deserialize_optional_u32_update<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<u32>>, D::Error> {
    Option::<u32>::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn usage_index_budget_distinguishes_omitted_default_and_zero() {
        for (body, expected) in [
            ("{}", None),
            (r#"{"usage_index_budget_mb":null}"#, Some(None)),
            (r#"{"usage_index_budget_mb":0}"#, Some(Some(0))),
            (r#"{"usage_index_budget_mb":64}"#, Some(Some(64))),
        ] {
            let update: UpdateServerSettingsRequest = serde_json::from_str(body).unwrap();
            assert_eq!(update.usage_index_budget_mb, expected);
            assert_eq!(
                serde_json::to_value(update)
                    .unwrap()
                    .get("usage_index_budget_mb")
                    .is_some(),
                expected.is_some()
            );
        }
        for value in ["-1", "1.5", "4294967296", r#""64""#] {
            assert!(
                serde_json::from_str::<UpdateServerSettingsRequest>(&format!(
                    r#"{{"usage_index_budget_mb":{value}}}"#
                ))
                .is_err()
            );
        }
    }
}
