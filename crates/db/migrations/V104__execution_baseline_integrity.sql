-- Data-preserving integrity ledger for historical execution-baseline
-- revisions that predate the closed adaptive-operation vocabulary.
--
-- Revision and approval rows remain immutable.  The ledger records the exact
-- rejected values and reserves stable identities for the correction draft,
-- canonical conflict, and reconciliation row so a startup audit is safely
-- restartable after any intermediate process failure.

CREATE TABLE execution_baseline_revision_integrity (
    revision_id             TEXT PRIMARY KEY
                                REFERENCES project_execution_baseline_revision(id)
                                ON DELETE RESTRICT,
    baseline_id             TEXT NOT NULL
                                REFERENCES project_execution_baseline(id)
                                ON DELETE CASCADE,
    project_id              TEXT NOT NULL
                                REFERENCES project(id)
                                ON DELETE CASCADE,
    field_path              TEXT NOT NULL,
    invalid_values_json     TEXT NOT NULL CHECK (json_valid(invalid_values_json)),
    diagnostic              TEXT NOT NULL CHECK (length(trim(diagnostic)) > 0),
    successor_revision_id   TEXT,
    conflict_id             TEXT,
    reconciliation_id       TEXT,
    audited_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);

CREATE INDEX idx_execution_baseline_revision_integrity_project
    ON execution_baseline_revision_integrity(project_id, audited_at DESC, revision_id DESC);

-- The application boundary prevents new invalid revisions from being
-- written.  These guards are deliberately ledger-based instead of copying
-- the Rust enum into SQL: once the startup audit has marked a historical
-- candidate, no approval or activation path can accidentally promote it.
CREATE TRIGGER execution_baseline_approval_integrity_guard
BEFORE INSERT ON project_execution_baseline_approval
WHEN EXISTS (
    SELECT 1 FROM execution_baseline_revision_integrity integrity
    WHERE integrity.revision_id = NEW.revision_id
)
BEGIN
    SELECT RAISE(ABORT, 'Invalid execution baseline revision cannot be approved');
END;

CREATE TRIGGER execution_baseline_pointer_integrity_guard
BEFORE UPDATE OF current_revision_id ON project_execution_baseline
WHEN NEW.current_revision_id IS NOT NULL
 AND NEW.current_revision_id IS NOT OLD.current_revision_id
 AND EXISTS (
     SELECT 1 FROM execution_baseline_revision_integrity integrity
     WHERE integrity.revision_id = NEW.current_revision_id
 )
BEGIN
    SELECT RAISE(ABORT, 'Invalid execution baseline revision cannot be activated');
END;
