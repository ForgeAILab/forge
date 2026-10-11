ALTER TABLE workspace
    ADD COLUMN cleanup_attempts INTEGER NOT NULL DEFAULT 0
    CHECK (cleanup_attempts >= 0);

ALTER TABLE workspace
    ADD COLUMN last_cleanup_error TEXT;
