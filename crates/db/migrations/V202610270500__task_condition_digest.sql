-- Channel freshness is independent of the cache body and its invalidation triggers.
ALTER TABLE task ADD COLUMN condition_digest TEXT;
-- A build that wrote revision 8 before this column still needs the digest walk.
DELETE FROM system_setting WHERE key='task_condition_mapping_revision' AND value='8';
-- A pre-column revision-8 cursor covered JSON-embedded digests, not this column.
DELETE FROM system_setting
WHERE key='task_condition_backfill_cursor'
  AND CASE WHEN json_valid(value) THEN json_extract(value,'$.revision') END = 8;
