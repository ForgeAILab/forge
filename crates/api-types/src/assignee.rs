use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
#[ts(export)]
pub enum AssigneeKind {
    Agent,
    User,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignee {
    pub kind: AssigneeKind,
    pub id: String,
}
