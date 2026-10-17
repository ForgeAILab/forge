#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Assignee {
    Agent(String),
    User(String),
}
