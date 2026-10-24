-- Retain the exact legacy payload while recording the one LCM namespace a
-- fenced pre-topic ownership transfer removed from a sibling's import view.
-- The timeline id deliberately survives deletion of its new owner/timeline:
-- that deletion must not resurrect the sibling's stale LCM reference.
CREATE TABLE agent_legacy_lcm_relinquishment (
    session_id TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    timeline_id TEXT NOT NULL,
    transferred_to_runtime_session_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (session_id, timeline_id)
) WITHOUT ROWID;
