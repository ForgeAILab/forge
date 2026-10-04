ALTER TABLE project ADD COLUMN environment_pause_json TEXT;

UPDATE task
SET error_annotation = NULL,
    blocked_json = NULL,
    version = version + 1,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE CASE WHEN json_valid(error_annotation)
           THEN json_extract(error_annotation, '$.type') END = 'environment_not_ready';
