ALTER TABLE daemon ADD COLUMN max_concurrent_runs INTEGER CHECK (max_concurrent_runs IS NULL OR max_concurrent_runs >= 0);
ALTER TABLE daemon ADD COLUMN run_limit INTEGER CHECK (run_limit IS NULL OR run_limit > 0);

UPDATE daemon SET max_concurrent_runs = COALESCE(
    CASE WHEN json_type(labels_json, '$.max_concurrent_sessions') = 'integer' AND typeof(json_extract(labels_json, '$.max_concurrent_sessions')) = 'integer' AND json_extract(labels_json, '$.max_concurrent_sessions') > 0 THEN json_extract(labels_json, '$.max_concurrent_sessions') END,
    CASE WHEN json_type(labels_json, '$.max_sessions') = 'integer' AND typeof(json_extract(labels_json, '$.max_sessions')) = 'integer' AND json_extract(labels_json, '$.max_sessions') > 0 THEN json_extract(labels_json, '$.max_sessions') END,
    CASE WHEN json_type(labels_json, '$.active_session_cap') = 'integer' AND typeof(json_extract(labels_json, '$.active_session_cap')) = 'integer' AND json_extract(labels_json, '$.active_session_cap') > 0 THEN json_extract(labels_json, '$.active_session_cap') END,
    CASE WHEN json_type(labels_json, '$.max_concurrent_tasks') = 'integer' AND typeof(json_extract(labels_json, '$.max_concurrent_tasks')) = 'integer' AND json_extract(labels_json, '$.max_concurrent_tasks') > 0 THEN json_extract(labels_json, '$.max_concurrent_tasks') END
) WHERE json_valid(labels_json);
