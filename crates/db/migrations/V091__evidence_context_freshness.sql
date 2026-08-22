-- Pin Project evidence to the exact execution context observed when it was
-- attached.  Existing rows deliberately remain NULL and are treated as
-- stale for release-gating evidence until a new, context-bound attachment is
-- captured.  No historical evidence or media bytes are rewritten here.

ALTER TABLE project_media_attachment
    ADD COLUMN source_task_version INTEGER
        CHECK (source_task_version IS NULL OR source_task_version >= 1);

ALTER TABLE project_media_attachment
    ADD COLUMN source_context_digest TEXT;

ALTER TABLE project_media_attachment
    ADD COLUMN source_definition_revision_id TEXT;

CREATE INDEX idx_project_media_attachment_evidence_context
    ON project_media_attachment(
        milestone_id,
        source_task_id,
        source_execution_id,
        source_validation_id,
        source_definition_revision_id
    );
