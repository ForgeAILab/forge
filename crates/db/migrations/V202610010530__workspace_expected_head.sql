-- Forge may move HEAD without a worker execution (clean rebase or conflict
-- handoff, or a knowledge-capture commit). Keep this evidence separate from
-- immutable execution outcomes.
CREATE TABLE workspace_expected_head (
    placement_id TEXT PRIMARY KEY REFERENCES workspace_placement(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    head_sha TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
