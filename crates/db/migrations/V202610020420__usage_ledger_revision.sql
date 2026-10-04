-- Read-index invalidation metadata, not accounting totals. Historical ledger
-- rows are untouched. Indexed latest-change markers coalesce repeated updates.
CREATE TABLE usage_ledger_revision (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    invocation_rowid INTEGER NOT NULL,
    event_rowid INTEGER NOT NULL,
    estimate_rowid INTEGER NOT NULL,
    execution_rowid INTEGER NOT NULL,
    invocation_revision INTEGER NOT NULL DEFAULT 0,
    execution_revision INTEGER NOT NULL DEFAULT 0,
    deletion_generation INTEGER NOT NULL DEFAULT 0,
    invocation_count INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    estimate_count INTEGER NOT NULL,
    execution_count INTEGER NOT NULL,
    owned_execution_count INTEGER NOT NULL
);
INSERT INTO usage_ledger_revision (id, invocation_rowid, event_rowid, estimate_rowid, execution_rowid, invocation_count, event_count, estimate_count, execution_count, owned_execution_count)
SELECT 1,
    COALESCE((SELECT MAX(rowid) FROM usage_invocation), 0),
    COALESCE((SELECT MAX(rowid) FROM usage_event), 0),
    COALESCE((SELECT MAX(rowid) FROM cost_estimate_revision), 0),
    COALESCE((SELECT MAX(rowid) FROM execution), 0),
    (SELECT COUNT(*) FROM usage_invocation), (SELECT COUNT(*) FROM usage_event),
    (SELECT COUNT(*) FROM cost_estimate_revision), (SELECT COUNT(*) FROM execution),
    (SELECT COUNT(*) FROM execution WHERE agent_id IS NOT NULL);

CREATE TABLE usage_changed_invocation (
    id TEXT PRIMARY KEY REFERENCES usage_invocation(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL
);
CREATE INDEX idx_usage_changed_invocation_revision ON usage_changed_invocation(revision);
CREATE TABLE usage_changed_execution (
    id TEXT PRIMARY KEY,
    revision INTEGER NOT NULL
);
CREATE INDEX idx_usage_changed_execution_revision ON usage_changed_execution(revision, id);

CREATE TRIGGER usage_read_invocation_insert AFTER INSERT ON usage_invocation BEGIN
    UPDATE usage_ledger_revision SET invocation_count = invocation_count + 1, invocation_rowid = MAX(invocation_rowid, NEW.rowid) WHERE id = 1;
END;
CREATE TRIGGER usage_read_event_insert AFTER INSERT ON usage_event BEGIN
    UPDATE usage_ledger_revision SET event_count = event_count + 1, event_rowid = MAX(event_rowid, NEW.rowid) WHERE id = 1;
END;
CREATE TRIGGER usage_read_estimate_insert AFTER INSERT ON cost_estimate_revision BEGIN
    UPDATE usage_ledger_revision SET estimate_count = estimate_count + 1, estimate_rowid = MAX(estimate_rowid, NEW.rowid) WHERE id = 1;
END;
CREATE TRIGGER usage_read_execution_insert AFTER INSERT ON execution BEGIN
    UPDATE usage_ledger_revision SET owned_execution_count = owned_execution_count + (NEW.agent_id IS NOT NULL), execution_count = execution_count + 1, execution_rowid = MAX(execution_rowid, NEW.rowid) WHERE id = 1;
END;
CREATE TRIGGER usage_read_invocation_update AFTER UPDATE ON usage_invocation BEGIN
    UPDATE usage_ledger_revision SET invocation_revision = invocation_revision + 1 WHERE id = 1;
    INSERT INTO usage_changed_invocation (id, revision)
        SELECT NEW.id, invocation_revision FROM usage_ledger_revision WHERE id = 1
        ON CONFLICT(id) DO UPDATE SET revision = excluded.revision;
END;
-- Heartbeats don't change usage or terminal execution statistics. Ownership,
-- identity, status and terminal duration edits do, including below watermarks.
CREATE TRIGGER usage_read_execution_update AFTER UPDATE ON execution
WHEN OLD.id IS NOT NEW.id OR OLD.agent_id IS NOT NEW.agent_id
  OR OLD.status IS NOT NEW.status OR OLD.created_at IS NOT NEW.created_at
  OR (OLD.updated_at IS NOT NEW.updated_at AND NEW.status != 'running')
BEGIN
    UPDATE usage_ledger_revision SET execution_revision = execution_revision + 1,
        owned_execution_count = owned_execution_count + (NEW.agent_id IS NOT NULL) - (OLD.agent_id IS NOT NULL) WHERE id = 1;
    INSERT INTO usage_changed_execution (id, revision)
        SELECT OLD.id, execution_revision FROM usage_ledger_revision WHERE id = 1
        ON CONFLICT(id) DO UPDATE SET revision = excluded.revision;
    INSERT INTO usage_changed_execution (id, revision)
        SELECT NEW.id, execution_revision FROM usage_ledger_revision WHERE id = 1
        ON CONFLICT(id) DO UPDATE SET revision = excluded.revision;
END;
CREATE TRIGGER usage_read_invocation_delete AFTER DELETE ON usage_invocation BEGIN
    UPDATE usage_ledger_revision SET deletion_generation = deletion_generation + 1, invocation_count = invocation_count - 1,
        invocation_rowid = CASE WHEN OLD.rowid = invocation_rowid
            THEN COALESCE((SELECT MAX(rowid) FROM usage_invocation), 0) ELSE invocation_rowid END WHERE id = 1;
END;
CREATE TRIGGER usage_read_event_delete AFTER DELETE ON usage_event BEGIN
    UPDATE usage_ledger_revision SET deletion_generation = deletion_generation + 1, event_count = event_count - 1,
        event_rowid = CASE WHEN OLD.rowid = event_rowid
            THEN COALESCE((SELECT MAX(rowid) FROM usage_event), 0) ELSE event_rowid END WHERE id = 1;
END;
CREATE TRIGGER usage_read_estimate_delete AFTER DELETE ON cost_estimate_revision BEGIN
    UPDATE usage_ledger_revision SET deletion_generation = deletion_generation + 1, estimate_count = estimate_count - 1,
        estimate_rowid = CASE WHEN OLD.rowid = estimate_rowid
            THEN COALESCE((SELECT MAX(rowid) FROM cost_estimate_revision), 0) ELSE estimate_rowid END WHERE id = 1;
END;
CREATE TRIGGER usage_read_execution_delete AFTER DELETE ON execution BEGIN
    UPDATE usage_ledger_revision SET deletion_generation = deletion_generation + 1, execution_count = execution_count - 1,
        owned_execution_count = owned_execution_count - (OLD.agent_id IS NOT NULL),
        execution_rowid = CASE WHEN OLD.rowid = execution_rowid
            THEN COALESCE((SELECT MAX(rowid) FROM execution), 0) ELSE execution_rowid END WHERE id = 1;
    DELETE FROM usage_changed_execution WHERE id = OLD.id;
END;

-- Three partial indexes let occupancy/Operations probes seek running or failed
-- work without scanning terminal history. These index existing inputs, not rollups.
CREATE INDEX idx_execution_usage_running_agent ON execution(agent_id) WHERE status='running';
-- The live diagnostics must seek recent/last failed executions rather than
-- sort or filter all historical failures on each Operations poll.
CREATE INDEX idx_execution_usage_failed_recent
    ON execution(COALESCE(stopped_at, updated_at) DESC, id ASC) WHERE status='failed';
CREATE INDEX idx_execution_usage_failed_task
    ON execution(task_id, COALESCE(stopped_at, updated_at) DESC, id DESC) WHERE status='failed';
