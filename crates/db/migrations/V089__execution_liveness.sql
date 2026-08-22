-- Execution liveness is deliberately separate from semantic progress.
--
-- This migration only adds nullable lease/progress metadata and a monotonic
-- execution CAS version.  Existing history is retained as-is.  Terminal rows
-- are explicitly ownerless; a running row from before the lease contract is
-- given an already-expired hard deadline so recovery cannot mistake the row
-- for a live executor.  No heartbeat or owner is fabricated by the
-- migration.

ALTER TABLE execution ADD COLUMN execution_version INTEGER NOT NULL DEFAULT 1
    CHECK (execution_version >= 1);
ALTER TABLE execution ADD COLUMN lease_owner TEXT;
ALTER TABLE execution ADD COLUMN lease_expires_at TEXT;
ALTER TABLE execution ADD COLUMN hard_deadline_at TEXT;
ALTER TABLE execution ADD COLUMN last_heartbeat_at TEXT;
ALTER TABLE execution ADD COLUMN last_progress_at TEXT;

-- Preserve the old activity timestamp as semantic progress only.  It is not
-- promoted to a heartbeat, because streamed output never proved ownership.
UPDATE execution
SET last_progress_at = last_activity_at
WHERE last_activity_at IS NOT NULL;

-- Terminal history has no active lease by definition.  Existing running rows
-- cannot be attributed to a verifiable owner after migration; make them
-- immediately eligible for deterministic recovery without inventing a live
-- timestamp or owner.
UPDATE execution
SET lease_owner = NULL,
    lease_expires_at = NULL,
    last_heartbeat_at = NULL,
    hard_deadline_at = CASE
        WHEN hard_deadline_at IS NULL THEN updated_at
        ELSE hard_deadline_at
    END
WHERE status = 'running';

UPDATE execution
SET lease_owner = NULL,
    lease_expires_at = NULL,
    hard_deadline_at = NULL,
    last_heartbeat_at = NULL
WHERE status != 'running';

CREATE INDEX idx_execution_lease_expiry
    ON execution(status, lease_expires_at, hard_deadline_at, execution_version, id);
CREATE INDEX idx_execution_progress
    ON execution(status, last_progress_at, id);
