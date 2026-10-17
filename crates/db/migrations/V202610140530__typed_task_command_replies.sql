-- Preserve replies written by the string-only command writer. Their original
-- type is unrecoverable; retain the exact generic DB check error they exposed.
-- Successful replies and DB mutation replies already use their current shape.
UPDATE task_step
SET result_json = json_set(result_json, '$.error',
    json_object('Db', json_object('code', 'check', 'message', json_extract(result_json, '$.error.message'))))
WHERE kind = 'command' AND json_valid(result_json)
  AND json_type(result_json, '$.error') = 'object'
  AND json_extract(result_json, '$.error.code') = 'check';
