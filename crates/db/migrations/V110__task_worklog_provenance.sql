-- A Task comment could say anything and prove nothing about who said it.
--
-- Comments already reach the next role's dispatch context, so the reviewer
-- reads what the coder wrote. But `task_comment` carried only author name and
-- text: nothing tied an entry to the execution that produced it, the role that
-- held the workspace at the time, or the kind of update it claimed to be. That
-- makes agent prose indistinguishable from provenance.
--
-- These columns make a worklog entry inspectable: which execution, which role,
-- what kind of update, and an idempotency key so a retried turn appends once.
-- They stay nullable because every historical comment predates the contract,
-- and user-authored comments never carry execution provenance at all.
--
-- A worklog entry is narration, not truth: it never moves a Task and never
-- satisfies an acceptance check. Milestone evidence remains a media asset
-- captured through `task.evidence`.

ALTER TABLE task_comment ADD COLUMN execution_id TEXT REFERENCES execution(id) ON DELETE SET NULL;
ALTER TABLE task_comment ADD COLUMN role TEXT;
ALTER TABLE task_comment ADD COLUMN worklog_kind TEXT
    CHECK (worklog_kind IS NULL OR worklog_kind IN ('progress', 'decision', 'validation', 'blocker'));
ALTER TABLE task_comment ADD COLUMN idempotency_key TEXT;

-- One append per (task, key). A retried or replayed turn re-presenting the
-- same key must not produce a second entry in the reviewer's context.
CREATE UNIQUE INDEX idx_task_comment_idempotency
    ON task_comment(task_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_task_comment_execution
    ON task_comment(execution_id, created_at)
    WHERE execution_id IS NOT NULL;
