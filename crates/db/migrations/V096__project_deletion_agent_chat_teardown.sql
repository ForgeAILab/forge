-- Deleting a Project that was born from Product Genesis was impossible.
--
-- Three separate edges rejected the bounded teardown `ProjectRepo::delete`
-- performs:
--
-- 1. `product_genesis_session.project_id` is `ON DELETE SET NULL`, but the
--    table CHECK requires a non-NULL Project while `lifecycle = 'handed_off'`.
--    Removing the Project therefore aborted the transaction with a CHECK
--    failure before any cascade ran. A handed-off session exists only as that
--    Project's origin record, so the teardown now removes it outright rather
--    than leaving a session pointing at nothing.
--
-- 2. Deleting the Project cascades to its Agent Chat, which cascades to the
--    Chat's messages, instruction revisions, and handoffs. Every one of those
--    tables carried an unconditional `BEFORE DELETE ... RAISE(ABORT)` trigger,
--    so the cascade aborted the whole transaction. Any Project whose Agent had
--    exchanged a single message could never be deleted.
--
-- 3. The same applies to handoff delivery receipts, which cascade from the
--    handoff.
--
-- These follow the contract V077 established: immutability means "a committed
-- record cannot be rewritten or removed during ordinary operation", not "the
-- owning Project is permanent". Each trigger now aborts unless a
-- `project_deletion_guard` row covers the Project that owns the Chat, which
-- only `ProjectRepo::delete` installs, inside its transaction. Update
-- immutability is untouched and still unconditional.
--
-- No data is rewritten or removed by this migration.

DROP TRIGGER IF EXISTS agent_chat_message_immutable_delete;
CREATE TRIGGER agent_chat_message_immutable_delete
BEFORE DELETE ON agent_chat_message
WHEN NOT EXISTS (
    SELECT 1 FROM project_deletion_guard g
    JOIN agent_chat c ON c.project_id = g.project_id
    WHERE c.id = OLD.chat_id
)
BEGIN
    SELECT RAISE(ABORT, 'Agent Chat messages are immutable');
END;

DROP TRIGGER IF EXISTS agent_chat_instruction_immutable_delete;
CREATE TRIGGER agent_chat_instruction_immutable_delete
BEFORE DELETE ON agent_chat_instruction_revision
WHEN NOT EXISTS (
    SELECT 1 FROM project_deletion_guard g
    JOIN agent_chat c ON c.project_id = g.project_id
    WHERE c.id = OLD.chat_id
)
BEGIN
    SELECT RAISE(ABORT, 'Agent Chat instruction revisions are immutable');
END;

DROP TRIGGER IF EXISTS agent_handoff_immutable_delete;
CREATE TRIGGER agent_handoff_immutable_delete
BEFORE DELETE ON agent_handoff
WHEN NOT EXISTS (
    SELECT 1 FROM project_deletion_guard g
    JOIN agent_chat c ON c.project_id = g.project_id
    WHERE c.id = OLD.source_chat_id OR c.id = OLD.target_chat_id
)
BEGIN
    SELECT RAISE(ABORT, 'Agent handoffs are immutable');
END;

DROP TRIGGER IF EXISTS agent_handoff_delivery_immutable_delete;
CREATE TRIGGER agent_handoff_delivery_immutable_delete
BEFORE DELETE ON agent_handoff_delivery
WHEN NOT EXISTS (
    SELECT 1 FROM project_deletion_guard g
    JOIN agent_chat c ON c.project_id = g.project_id
    JOIN agent_handoff h ON c.id = h.source_chat_id OR c.id = h.target_chat_id
    WHERE h.id = OLD.handoff_id
)
BEGIN
    SELECT RAISE(ABORT, 'Handoff delivery receipts are immutable');
END;

-- LCM timelines deliberately use polymorphic canonical scope ids, so they
-- cannot carry ordinary foreign keys to Project/Task/Agent Chat. Their child
-- rows are immutable and would otherwise both survive Project deletion and
-- prevent a bounded timeline cascade. Admit deletes only while the owning
-- Project's transactional guard exists.
DROP TRIGGER IF EXISTS agent_lcm_entry_truncate_guard;
CREATE TRIGGER agent_lcm_entry_truncate_guard
BEFORE DELETE ON agent_lcm_entry
WHEN NOT EXISTS (
    SELECT 1
    FROM project_deletion_guard g
    JOIN agent_lcm_timeline l ON l.id = OLD.timeline_id
    WHERE (l.scope_type = 'project' AND l.scope_id = g.project_id)
       OR (l.scope_type = 'task' AND EXISTS (
            SELECT 1 FROM task t
            WHERE t.id = l.scope_id AND t.project_id = g.project_id
       ))
       OR (l.scope_type = 'agent_chat' AND EXISTS (
            SELECT 1 FROM agent_chat c
            WHERE c.id = l.scope_id AND c.project_id = g.project_id
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
    WHERE (l.scope_type = 'project' AND l.scope_id = g.project_id)
       OR (l.scope_type = 'task' AND EXISTS (
            SELECT 1 FROM task t
            WHERE t.id = l.scope_id AND t.project_id = g.project_id
       ))
       OR (l.scope_type = 'agent_chat' AND EXISTS (
            SELECT 1 FROM agent_chat c
            WHERE c.id = l.scope_id AND c.project_id = g.project_id
       ))
)
BEGIN
    SELECT RAISE(ABORT, 'LCM nodes are immutable');
END;
