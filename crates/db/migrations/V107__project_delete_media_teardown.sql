-- Project deletion could not get past the media graph.
--
-- `task_media.asset_id` and `media_asset.legacy_task_media_id` reference each
-- other with ON DELETE SET NULL, and both columns were guarded by triggers that
-- aborted on *any* change. Whichever side the cascade reached first, SQLite's
-- FK action wrote the NULL and the guard aborted the whole transaction, so
-- `DELETE FROM project` always failed with "Task media asset mapping is
-- immutable".
--
-- The invariant those guards are protecting is that a mapping never gets
-- re-pointed at a *different* row. Clearing the reference because the referenced
-- row is being deleted is the foreign key doing its job, so both guards now
-- allow the NULL-out and keep aborting every other rewrite.

DROP TRIGGER IF EXISTS task_media_asset_id_guard_update;
CREATE TRIGGER task_media_asset_id_guard_update
BEFORE UPDATE OF asset_id ON task_media
WHEN OLD.asset_id IS NOT NULL
  AND NEW.asset_id IS NOT OLD.asset_id
  AND NEW.asset_id IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'Task media asset mapping is immutable');
END;

DROP TRIGGER IF EXISTS media_asset_immutable_storage_update;
CREATE TRIGGER media_asset_immutable_storage_update
BEFORE UPDATE OF project_id, legacy_task_media_id, display_filename, content_type, byte_size, storage_key
ON media_asset
WHEN OLD.project_id IS NOT NEW.project_id
  OR (OLD.legacy_task_media_id IS NOT NEW.legacy_task_media_id
      AND NEW.legacy_task_media_id IS NOT NULL)
  OR OLD.display_filename IS NOT NEW.display_filename
  OR OLD.content_type IS NOT NEW.content_type
  OR OLD.byte_size IS NOT NEW.byte_size
  OR OLD.storage_key IS NOT NEW.storage_key
BEGIN
    SELECT RAISE(ABORT, 'Media asset storage metadata is immutable');
END;
