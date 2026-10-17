-- Keep registrations and their foreign-key history; only active identities authenticate.
ALTER TABLE daemon ADD COLUMN removed_at TEXT;
CREATE INDEX idx_daemon_active ON daemon(created_at, id) WHERE removed_at IS NULL;

-- Daemon workspace receipts double as an eventual cleanup outbox. Removal is
-- the one explicit abandonment boundary; other immutable command history keeps
-- its delete protection. The tombstone must commit in the same transaction.
DROP TRIGGER command_receipt_immutable_delete;
CREATE TRIGGER command_receipt_immutable_delete
BEFORE DELETE ON command_receipt
WHEN NOT (
    OLD.principal_id = 'workspace-backend'
    AND OLD.operation LIKE 'daemon.workspace.%'
    AND EXISTS (SELECT 1 FROM daemon d
        WHERE d.id = json_extract(OLD.outcome_json, '$.metadata.daemon_id')
          AND d.removed_at IS NOT NULL)
)
BEGIN
    SELECT RAISE(ABORT, 'Command receipts are immutable');
END;
