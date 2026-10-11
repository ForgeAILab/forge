-- Stage 1: legacy fields remain authoritative. No legacy value, version,
-- timestamp, event, queue intent, claim or receipt is changed by this migration.
--
-- The column default is the mapping of a Task with no condition facts. Existing
-- rows are backfilled by the migration runner's Rust post-step
-- (`task_condition::backfill`) in this same transaction, with the one mapping
-- the writer seams use. There is deliberately no mapping view and no trigger:
-- compiling the mapping into every Task write statement was the cost.
ALTER TABLE task ADD COLUMN condition_json TEXT NOT NULL
 DEFAULT '{"kind":"clear","evidence":{"error_annotation":null,"blocked_json":null,"failed_json":null,"entry_barrier_json":null,"metadata":{},"unparsed_metadata":null}}'
 CHECK (json_valid(condition_json));

CREATE INDEX idx_task_condition_kind ON task(json_extract(condition_json,'$.kind'));
