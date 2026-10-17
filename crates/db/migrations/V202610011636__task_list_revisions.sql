-- The board's original optimistic-concurrency triggers remain unchanged.
ALTER TABLE project ADD COLUMN list_revision INTEGER NOT NULL DEFAULT 0;

-- The conditional path probes this index; task writes never scan the project.
CREATE INDEX IF NOT EXISTS idx_task_deferred_dispatch_project ON task(project_id)
WHERE json_valid(metadata_json) AND json_type(metadata_json, '$.deferred_dispatch') IS NOT NULL AND deleted_at IS NULL;

DROP TRIGGER IF EXISTS task_list_revision_task_insert;
CREATE TRIGGER task_list_revision_task_insert AFTER INSERT ON task
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (NEW.project_id);
END;
DROP TRIGGER IF EXISTS task_list_revision_task_delete;
CREATE TRIGGER task_list_revision_task_delete AFTER DELETE ON task
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (OLD.project_id);
END;
DROP TRIGGER IF EXISTS task_list_revision_task_update;
CREATE TRIGGER task_list_revision_task_update AFTER UPDATE OF project_id, parent_task_id, assignee_type, assignee_id, title, description, task_type, status, priority, board_position, subtask_order, error_annotation, blocked_json, failed_json, metadata_json, review_passed_at, archived_at, deleted_at, version, created_at, updated_at, task_state_config ON task
WHEN OLD.project_id IS NOT NEW.project_id
    OR OLD.parent_task_id IS NOT NEW.parent_task_id
    OR OLD.assignee_type IS NOT NEW.assignee_type
    OR OLD.assignee_id IS NOT NEW.assignee_id
    OR OLD.title IS NOT NEW.title
    OR OLD.description IS NOT NEW.description
    OR OLD.task_type IS NOT NEW.task_type
    OR OLD.status IS NOT NEW.status
    OR OLD.priority IS NOT NEW.priority
    OR OLD.board_position IS NOT NEW.board_position
    OR OLD.subtask_order IS NOT NEW.subtask_order
    OR OLD.error_annotation IS NOT NEW.error_annotation
    OR OLD.blocked_json IS NOT NEW.blocked_json
    OR OLD.failed_json IS NOT NEW.failed_json
    OR OLD.metadata_json IS NOT NEW.metadata_json
    OR OLD.review_passed_at IS NOT NEW.review_passed_at
    OR OLD.archived_at IS NOT NEW.archived_at
    OR OLD.deleted_at IS NOT NEW.deleted_at
    OR OLD.version IS NOT NEW.version
    OR OLD.created_at IS NOT NEW.created_at
    OR OLD.updated_at IS NOT NEW.updated_at
    OR OLD.task_state_config IS NOT NEW.task_state_config
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (OLD.project_id, NEW.project_id);
END;
DROP TRIGGER IF EXISTS task_list_revision_execution_insert;
CREATE TRIGGER task_list_revision_execution_insert AFTER INSERT ON execution
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (NEW.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_execution_delete;
CREATE TRIGGER task_list_revision_execution_delete AFTER DELETE ON execution
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (OLD.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_execution_update;
CREATE TRIGGER task_list_revision_execution_update AFTER UPDATE OF task_id, status, role, agent_id, agent_session_id, resume_policy, stop_reason, error, stopped_at, created_at, updated_at, executor_config_snapshot_json ON execution
WHEN OLD.task_id IS NOT NEW.task_id
    OR OLD.status IS NOT NEW.status
    OR OLD.role IS NOT NEW.role
    OR OLD.agent_id IS NOT NEW.agent_id
    OR OLD.agent_session_id IS NOT NEW.agent_session_id
    OR OLD.resume_policy IS NOT NEW.resume_policy
    OR OLD.stop_reason IS NOT NEW.stop_reason
    OR OLD.error IS NOT NEW.error
    OR OLD.stopped_at IS NOT NEW.stopped_at
    OR OLD.created_at IS NOT NEW.created_at
    OR (OLD.updated_at IS NOT NEW.updated_at AND NEW.status != 'running')
    OR OLD.executor_config_snapshot_json IS NOT NEW.executor_config_snapshot_json
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (OLD.task_id, NEW.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_review_insert;
CREATE TRIGGER task_list_revision_review_insert AFTER INSERT ON review
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (NEW.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_review_delete;
CREATE TRIGGER task_list_revision_review_delete AFTER DELETE ON review
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (OLD.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_review_update;
CREATE TRIGGER task_list_revision_review_update AFTER UPDATE OF task_id, status, finished_at, created_at, execution_id, step_results_json, attempt_number, reviewer_execution_id, auditor_execution_id ON review
WHEN OLD.task_id IS NOT NEW.task_id
    OR OLD.status IS NOT NEW.status
    OR OLD.finished_at IS NOT NEW.finished_at
    OR OLD.created_at IS NOT NEW.created_at
    OR OLD.execution_id IS NOT NEW.execution_id
    OR OLD.step_results_json IS NOT NEW.step_results_json
    OR OLD.attempt_number IS NOT NEW.attempt_number
    OR OLD.reviewer_execution_id IS NOT NEW.reviewer_execution_id
    OR OLD.auditor_execution_id IS NOT NEW.auditor_execution_id
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (OLD.task_id, NEW.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_task_role_assignment_insert;
CREATE TRIGGER task_list_revision_task_role_assignment_insert AFTER INSERT ON task_role_assignment
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (NEW.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_task_role_assignment_delete;
CREATE TRIGGER task_list_revision_task_role_assignment_delete AFTER DELETE ON task_role_assignment
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (OLD.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_task_role_assignment_update;
CREATE TRIGGER task_list_revision_task_role_assignment_update AFTER UPDATE ON task_role_assignment
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (OLD.task_id, NEW.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_transition_log_insert;
CREATE TRIGGER task_list_revision_transition_log_insert AFTER INSERT ON transition_log
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (NEW.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_transition_log_delete;
CREATE TRIGGER task_list_revision_transition_log_delete AFTER DELETE ON transition_log
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (OLD.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_transition_log_update;
CREATE TRIGGER task_list_revision_transition_log_update AFTER UPDATE ON transition_log
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (OLD.task_id, NEW.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_task_external_link_insert;
CREATE TRIGGER task_list_revision_task_external_link_insert AFTER INSERT ON task_external_link
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (NEW.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_task_external_link_delete;
CREATE TRIGGER task_list_revision_task_external_link_delete AFTER DELETE ON task_external_link
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (OLD.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_task_external_link_update;
CREATE TRIGGER task_list_revision_task_external_link_update AFTER UPDATE ON task_external_link
BEGIN
    UPDATE project SET list_revision = list_revision + 1
    WHERE id IN (SELECT project_id FROM task WHERE id IN (OLD.task_id, NEW.task_id));
END;
DROP TRIGGER IF EXISTS task_list_revision_workflow;
CREATE TRIGGER task_list_revision_workflow AFTER UPDATE OF workflow_definition ON project
WHEN OLD.workflow_definition IS NOT NEW.workflow_definition
BEGIN
    UPDATE project SET list_revision = list_revision + 1 WHERE id = NEW.id;
END;
