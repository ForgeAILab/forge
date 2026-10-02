-- The slot projection's child-existence probe is deliberately not Project
-- scoped. Preserve that behavior for persisted cross-Project parent links.
-- Ordinary same-Project links are already covered by the Task list triggers.
CREATE TRIGGER parent_project_slot_revision_insert AFTER INSERT ON task
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task
                 WHERE id = NEW.parent_task_id AND project_id != NEW.project_id);
END;

CREATE TRIGGER parent_project_slot_revision_delete AFTER DELETE ON task
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task
                 WHERE id = OLD.parent_task_id AND project_id != OLD.project_id);
END;

CREATE TRIGGER parent_project_slot_revision_update
AFTER UPDATE OF parent_task_id, project_id, deleted_at ON task
WHEN OLD.parent_task_id IS NOT NEW.parent_task_id
  OR OLD.project_id IS NOT NEW.project_id
  OR OLD.deleted_at IS NOT NEW.deleted_at
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task
                 WHERE (id = OLD.parent_task_id AND project_id != OLD.project_id)
                    OR (id = NEW.parent_task_id AND project_id != NEW.project_id));
END;
