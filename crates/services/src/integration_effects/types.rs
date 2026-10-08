/// Placement authority is checked by the caller before constructing an effect.
/// This is an input witness, not an owner-side fencing token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectWorkspace {
    pub workspace_id: String,
    pub placement_id: String,
    pub generation: i64,
    pub owner: EffectOwner,
    pub handle: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectOwner {
    Server,
    Daemon {
        daemon_id: String,
        runtime_id: String,
    },
}

/// Execution evidence projected by today's consumer, never by the effect.
#[derive(Debug, Clone)]
pub struct MergeExecutionEvidence {
    pub before_sha: Option<String>,
    pub after_sha: Option<String>,
}
