ALTER TABLE review ADD COLUMN reviewer_execution_id TEXT REFERENCES execution(id) ON DELETE SET NULL;
ALTER TABLE review ADD COLUMN auditor_execution_id TEXT REFERENCES execution(id) ON DELETE SET NULL;

CREATE UNIQUE INDEX idx_review_reviewer_execution
    ON review(reviewer_execution_id)
    WHERE reviewer_execution_id IS NOT NULL;

CREATE UNIQUE INDEX idx_review_auditor_execution
    ON review(auditor_execution_id)
    WHERE auditor_execution_id IS NOT NULL;
