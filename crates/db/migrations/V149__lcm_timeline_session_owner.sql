-- An Agent Chat LCM timeline is written by one native runtime session at a
-- time. After a restart (or a session rotation) Forge opens a fresh runtime
-- session whose canonical history is rebuilt from the chat transcript, so it
-- no longer matches the old timeline. Once that timeline holds summary nodes
-- the runtime cannot truncate the diverged tail, and every turn failed with
-- "LCM source range overlaps an active node".
--
-- Timelines now record the runtime session that owns them. A different
-- session retires the old timeline instead of reusing it: the row keeps its
-- entries and nodes, its `scope_id` gains a `#retired:<id>` suffix so the
-- scope's unique binding is free for a fresh timeline, and
-- `canonical_scope_id` keeps the original scope so Project deletion still
-- tears it down. Rows already retired by hand with that suffix are
-- recognised by the backfill.

ALTER TABLE agent_lcm_timeline ADD COLUMN runtime_session_id TEXT;
ALTER TABLE agent_lcm_timeline ADD COLUMN retired_at TEXT;
ALTER TABLE agent_lcm_timeline ADD COLUMN canonical_scope_id TEXT;

UPDATE agent_lcm_timeline
SET canonical_scope_id = CASE
        WHEN instr(scope_id, '#retired:') > 0
            THEN substr(scope_id, 1, instr(scope_id, '#retired:') - 1)
        ELSE scope_id
    END,
    retired_at = CASE
        WHEN instr(scope_id, '#retired:') > 0 THEN updated_at
        ELSE NULL
    END;

DROP TRIGGER IF EXISTS agent_lcm_entry_truncate_guard;
CREATE TRIGGER agent_lcm_entry_truncate_guard
BEFORE DELETE ON agent_lcm_entry
WHEN NOT EXISTS (
    SELECT 1
    FROM project_deletion_guard g
    JOIN agent_lcm_timeline l ON l.id = OLD.timeline_id
    WHERE (l.scope_type = 'project' AND l.canonical_scope_id = g.project_id)
       OR (l.scope_type = 'task' AND EXISTS (
            SELECT 1 FROM task t
            WHERE t.id = l.canonical_scope_id AND t.project_id = g.project_id
       ))
       OR (l.scope_type = 'agent_chat' AND EXISTS (
            SELECT 1 FROM agent_chat c
            WHERE c.id = l.canonical_scope_id AND c.project_id = g.project_id
       ))
)
BEGIN
    SELECT RAISE(ABORT, 'LCM entries covered by summary nodes are immutable')
    WHERE EXISTS (
        SELECT 1 FROM agent_lcm_node
        WHERE agent_lcm_node.timeline_id = OLD.timeline_id
          AND agent_lcm_node.range_end >= OLD.sequence
    );
END;

DROP TRIGGER IF EXISTS agent_lcm_node_no_delete;
CREATE TRIGGER agent_lcm_node_no_delete
BEFORE DELETE ON agent_lcm_node
WHEN NOT EXISTS (
    SELECT 1
    FROM project_deletion_guard g
    JOIN agent_lcm_timeline l ON l.id = OLD.timeline_id
    WHERE (l.scope_type = 'project' AND l.canonical_scope_id = g.project_id)
       OR (l.scope_type = 'task' AND EXISTS (
            SELECT 1 FROM task t
            WHERE t.id = l.canonical_scope_id AND t.project_id = g.project_id
       ))
       OR (l.scope_type = 'agent_chat' AND EXISTS (
            SELECT 1 FROM agent_chat c
            WHERE c.id = l.canonical_scope_id AND c.project_id = g.project_id
       ))
)
BEGIN
    SELECT RAISE(ABORT, 'LCM nodes are immutable');
END;
