-- Record the exact successor artifact a reconciliation resolution names for
-- a `revised`/`superseded` outcome.  The shared reconciliation service
-- validates the replacement reference before it is persisted; existing
-- resolutions predate this contract and remain NULL, which the service
-- treats as "no replacement was recorded" rather than inferring one.

ALTER TABLE project_reconciliation_resolution
    ADD COLUMN replacement_ref_type TEXT;

ALTER TABLE project_reconciliation_resolution
    ADD COLUMN replacement_ref_id TEXT;

ALTER TABLE project_reconciliation_resolution
    ADD COLUMN replacement_ref_revision TEXT;
