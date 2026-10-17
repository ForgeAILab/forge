-- Private execution artifacts; deliberately absent from execution API/config snapshots.
CREATE TABLE execution_plan_transport (
    execution_id TEXT PRIMARY KEY REFERENCES execution(id) ON DELETE CASCADE,
    seed_digest TEXT,
    seed_size INTEGER,
    candidate_text TEXT,
    candidate_error TEXT,
    candidate_size INTEGER,
    terminal_report_id TEXT,
    retry_operation TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    retry_at TEXT,
    retry_error TEXT,
    updated_at TEXT NOT NULL
);
INSERT INTO execution_plan_transport (execution_id, candidate_text, candidate_size, updated_at)
SELECT id, json_extract(executor_config_snapshot_json, '$.terminal_plan_text'),
       length(CAST(json_extract(executor_config_snapshot_json, '$.terminal_plan_text') AS BLOB)), updated_at
FROM execution
WHERE json_valid(executor_config_snapshot_json)
  AND json_type(executor_config_snapshot_json, '$.terminal_plan_text') = 'text';
UPDATE execution
SET executor_config_snapshot_json = json_remove(executor_config_snapshot_json, '$.terminal_plan_text')
WHERE json_valid(executor_config_snapshot_json)
  AND json_type(executor_config_snapshot_json, '$.terminal_plan_text') IS NOT NULL;
-- A receipt retains request identity/digest, never artifact content.
UPDATE command_receipt
SET outcome_json = json_remove(json_set(outcome_json,
    '$.owner_result.request.operation.content_length',
    length(CAST(json_extract(outcome_json, '$.owner_result.request.operation.content') AS BLOB))),
    '$.owner_result.request.operation.content')
WHERE json_valid(outcome_json)
  AND json_type(outcome_json, '$.owner_result.request.operation.content') = 'text'
  AND json_extract(outcome_json, '$.owner_result.request.operation.kind') = 'publish_plan';
