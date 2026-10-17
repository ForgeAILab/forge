-- Internal scheduler state. Legacy Task columns and public readers are unchanged.
CREATE INDEX idx_execution_running_task ON execution(task_id) WHERE status = 'running';
DROP INDEX idx_task_condition_kind;
-- The sweep pages over Tasks that are not settled; settled rows are never read.
CREATE INDEX idx_task_schedule_open ON task(id) WHERE json_extract(condition_json,'$.kind') != 'settled';
CREATE TABLE task_schedule_dirty (
    task_id TEXT PRIMARY KEY REFERENCES task(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL DEFAULT 1,
    dirty INTEGER NOT NULL DEFAULT 1 CHECK(dirty IN (0,1)),
    external INTEGER NOT NULL DEFAULT 0 CHECK(external IN (0,1))
);
CREATE INDEX task_schedule_dirty_ready ON task_schedule_dirty(task_id) WHERE dirty=1;
CREATE TABLE task_schedule_park (
    task_id TEXT PRIMARY KEY REFERENCES task(id) ON DELETE CASCADE,
    epoch INTEGER NOT NULL,
    reason_json TEXT NOT NULL CHECK(json_valid(reason_json))
);
-- `daemon_id` is set only while the Task waits on a machine: that machine's
-- readiness or reconnect, or '*' for a run slot on whichever machine can take
-- it. `project_capacity` is set only while it waits on the Project limit. A
-- capacity change therefore kicks the waiters it can admit and no others.
CREATE TABLE task_schedule_wait (
    task_id TEXT PRIMARY KEY REFERENCES task(id) ON DELETE CASCADE,
    project_id TEXT NOT NULL,
    agent_id TEXT,
    daemon_id TEXT,
    deadline TEXT,
    project_capacity INTEGER NOT NULL DEFAULT 0 CHECK(project_capacity IN (0,1))
);
CREATE INDEX task_schedule_wait_project ON task_schedule_wait(project_id);
CREATE INDEX task_schedule_wait_agent ON task_schedule_wait(agent_id);
CREATE INDEX task_schedule_wait_machine ON task_schedule_wait(daemon_id);
CREATE INDEX task_schedule_wait_deadline ON task_schedule_wait(deadline) WHERE deadline IS NOT NULL;

CREATE TRIGGER task_schedule_insert AFTER INSERT ON task BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=NEW.id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id=NEW.parent_task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_update AFTER UPDATE ON task WHEN OLD.condition_json IS NOT NEW.condition_json OR OLD.version IS NOT NEW.version OR OLD.status IS NOT NEW.status OR OLD.priority IS NOT NEW.priority OR OLD.plan IS NOT NEW.plan OR OLD.task_state_config IS NOT NEW.task_state_config OR (OLD.metadata_json IS NOT NEW.metadata_json AND (instr(COALESCE(OLD.metadata_json,'')||COALESCE(NEW.metadata_json,''),'plan_publication')>0 OR instr(COALESCE(OLD.metadata_json,'')||COALESCE(NEW.metadata_json,''),'queued_recovery')>0 OR instr(COALESCE(OLD.metadata_json,'')||COALESCE(NEW.metadata_json,''),'terminal_execution_settlement')>0) AND CASE WHEN json_valid(OLD.metadata_json) THEN json_extract(OLD.metadata_json,'$.plan_publication_claim','$.plan_publication_cleanup','$.queued_recovery','$.terminal_execution_settlement') END IS NOT CASE WHEN json_valid(NEW.metadata_json) THEN json_extract(NEW.metadata_json,'$.plan_publication_claim','$.plan_publication_cleanup','$.queued_recovery','$.terminal_execution_settlement') END) OR OLD.deleted_at IS NOT NEW.deleted_at OR OLD.archived_at IS NOT NEW.archived_at OR OLD.parent_task_id IS NOT NEW.parent_task_id BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=NEW.id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_relationship AFTER UPDATE ON task WHEN OLD.status IS NOT NEW.status OR OLD.deleted_at IS NOT NEW.deleted_at OR OLD.parent_task_id IS NOT NEW.parent_task_id OR OLD.subtask_order IS NOT NEW.subtask_order OR OLD.condition_json IS NOT NEW.condition_json BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id IN (OLD.parent_task_id,NEW.parent_task_id) OR parent_task_id IN (OLD.parent_task_id,NEW.parent_task_id) OR parent_task_id=NEW.id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT task_id FROM task_dependency WHERE depends_on_id=NEW.id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT task_id FROM task_schedule_wait WHERE project_id=NEW.project_id AND project_capacity=1) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_delete BEFORE DELETE ON task BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id=OLD.parent_task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT task_id FROM task_dependency WHERE depends_on_id=OLD.id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_dependency_insert AFTER INSERT ON task_dependency BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id=NEW.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_role_insert AFTER INSERT ON task_role_assignment BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id=NEW.task_id OR (parent_task_id=NEW.task_id AND NEW.role_name='coder')) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_step_insert AFTER INSERT ON task_step WHEN NEW.kind != 'mutation' BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=NEW.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_review_insert AFTER INSERT ON review BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=NEW.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

-- A new execution takes capacity; it never frees any. It marks its own Task
-- and the Tasks waiting on its Project's limit, whose recorded wait names the
-- count of active Tasks. Waiters on an Agent or a machine are kicked when an
-- execution ends.
CREATE TRIGGER task_schedule_execution_insert AFTER INSERT ON execution BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=NEW.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT task_id FROM task_schedule_wait WHERE project_capacity=1 AND project_id=(SELECT project_id FROM task WHERE id=NEW.task_id)) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_placement_insert AFTER INSERT ON workspace_placement BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id=NEW.task_id OR parent_task_id=NEW.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT task_id FROM task_schedule_wait WHERE agent_id=NEW.agent_id OR daemon_id IN (COALESCE(NEW.execution_daemon_id,NEW.daemon_id,''),'*')) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_cancel_insert AFTER INSERT ON pending_remote_cancel BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id IN (SELECT task_id FROM workspace WHERE id=NEW.workspace_id UNION SELECT task_id FROM execution WHERE workspace_id=NEW.workspace_id)) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_repo_insert AFTER INSERT ON repo BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE project_id=NEW.project_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_readiness_insert AFTER INSERT ON project_machine_readiness BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE project_id=NEW.project_id AND (id IN (SELECT task_id FROM task_schedule_wait WHERE daemon_id=NEW.daemon_id) OR NEW.owner_kind='server')) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_transition_insert AFTER INSERT ON transition_log BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=NEW.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_dependency_delete AFTER DELETE ON task_dependency BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id=OLD.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_role_delete AFTER DELETE ON task_role_assignment BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id=OLD.task_id OR (parent_task_id=OLD.task_id AND OLD.role_name='coder')) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_step_delete AFTER DELETE ON task_step WHEN OLD.kind != 'mutation' BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=OLD.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_review_delete AFTER DELETE ON review BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=OLD.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_execution_delete AFTER DELETE ON execution BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=OLD.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT task_id FROM task_schedule_wait WHERE agent_id=OLD.agent_id OR daemon_id IN (COALESCE((SELECT daemon_id FROM agent_current WHERE id=OLD.agent_id),''),'*') OR (project_capacity=1 AND project_id=(SELECT project_id FROM task WHERE id=OLD.task_id))) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_placement_delete AFTER DELETE ON workspace_placement BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id=OLD.task_id OR parent_task_id=OLD.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT task_id FROM task_schedule_wait WHERE agent_id=OLD.agent_id OR daemon_id IN (COALESCE(OLD.execution_daemon_id,OLD.daemon_id,''),'*')) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_cancel_delete AFTER DELETE ON pending_remote_cancel BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id IN (SELECT task_id FROM workspace WHERE id=OLD.workspace_id UNION SELECT task_id FROM execution WHERE workspace_id=OLD.workspace_id)) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_repo_delete AFTER DELETE ON repo BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE project_id=OLD.project_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_readiness_delete AFTER DELETE ON project_machine_readiness BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE project_id=OLD.project_id AND (id IN (SELECT task_id FROM task_schedule_wait WHERE daemon_id=OLD.daemon_id) OR OLD.owner_kind='server')) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_transition_delete AFTER DELETE ON transition_log BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=OLD.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_dependency_update AFTER UPDATE ON task_dependency BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id=NEW.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_role_update AFTER UPDATE ON task_role_assignment BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id=NEW.task_id OR (parent_task_id=NEW.task_id AND NEW.role_name='coder')) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_step_update AFTER UPDATE ON task_step WHEN NEW.kind != 'mutation' AND (OLD.status IS NOT NEW.status OR OLD.available_at IS NOT NEW.available_at) BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=NEW.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_review_update AFTER UPDATE ON review BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=NEW.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_execution_update AFTER UPDATE ON execution WHEN OLD.status IS NOT NEW.status OR OLD.agent_id IS NOT NEW.agent_id OR OLD.workspace_id IS NOT NEW.workspace_id OR OLD.executor_config_snapshot_json IS NOT NEW.executor_config_snapshot_json BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=NEW.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT task_id FROM task_schedule_wait WHERE agent_id=NEW.agent_id OR daemon_id IN (COALESCE((SELECT COALESCE(execution_daemon_id,daemon_id) FROM workspace_placement WHERE workspace_id=NEW.workspace_id),CASE WHEN json_valid(NEW.executor_config_snapshot_json) THEN json_extract(NEW.executor_config_snapshot_json,'$.daemon_id') END,(SELECT daemon_id FROM agent_current WHERE id=NEW.agent_id),''),'*') OR (project_capacity=1 AND project_id=(SELECT project_id FROM task WHERE id=NEW.task_id))) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_placement_update AFTER UPDATE ON workspace_placement WHEN OLD.state IS NOT NEW.state OR OLD.reserved_until IS NOT NEW.reserved_until OR OLD.generation IS NOT NEW.generation BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id=NEW.task_id OR parent_task_id=NEW.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT task_id FROM task_schedule_wait WHERE agent_id=NEW.agent_id OR daemon_id IN (COALESCE(NEW.execution_daemon_id,NEW.daemon_id,''),'*')) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_cancel_update AFTER UPDATE ON pending_remote_cancel BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE id IN (SELECT task_id FROM workspace WHERE id=NEW.workspace_id UNION SELECT task_id FROM execution WHERE workspace_id=NEW.workspace_id)) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_repo_update AFTER UPDATE ON repo BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE project_id=NEW.project_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_readiness_update AFTER UPDATE ON project_machine_readiness WHEN OLD.status IS NOT NEW.status OR OLD.checks_digest IS NOT NEW.checks_digest BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE project_id=NEW.project_id AND (id IN (SELECT task_id FROM task_schedule_wait WHERE daemon_id=NEW.daemon_id) OR NEW.owner_kind='server')) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_transition_update AFTER UPDATE ON transition_log BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 0 FROM (SELECT id FROM task WHERE id=NEW.task_id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_project AFTER UPDATE ON project WHEN OLD.version IS NOT NEW.version OR OLD.workflow_definition IS NOT NEW.workflow_definition OR OLD.settings IS NOT NEW.settings OR OLD.paused_at IS NOT NEW.paused_at OR OLD.primary_repo_id IS NOT NEW.primary_repo_id BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT id FROM task WHERE project_id=NEW.id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_daemon AFTER UPDATE ON daemon WHEN OLD.removed_at IS NOT NEW.removed_at OR OLD.status IS NOT NEW.status OR OLD.run_limit IS NOT NEW.run_limit OR OLD.max_concurrent_runs IS NOT NEW.max_concurrent_runs OR OLD.agent_version IS NOT NEW.agent_version OR OLD.detected_clis_json IS NOT NEW.detected_clis_json BEGIN
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT task_id FROM task_schedule_wait WHERE daemon_id IN (NEW.id,'*')) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
    INSERT INTO task_schedule_dirty(task_id,external) SELECT *, 1 FROM (SELECT DISTINCT a.task_id FROM task_role_assignment a JOIN agent_current g ON g.id=a.assignee_id WHERE g.daemon_id=NEW.id) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1, dirty=1, external=MAX(external,excluded.external);
END;

CREATE TRIGGER task_schedule_workspace_insert AFTER INSERT ON workspace BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT *,1 FROM (SELECT id FROM task WHERE id=NEW.task_id OR parent_task_id=NEW.task_id OR id IN (SELECT task_id FROM execution WHERE workspace_id=NEW.id)) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;

CREATE TRIGGER task_schedule_profile_insert AFTER INSERT ON agent_profile BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT *,1 FROM (SELECT id FROM task WHERE id IN (SELECT task_id FROM task_role_assignment WHERE assignee_id=NEW.identity_id) OR parent_task_id IN (SELECT task_id FROM task_role_assignment WHERE assignee_id=NEW.identity_id AND role_name='coder')) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;

CREATE TRIGGER task_schedule_workspace_update AFTER UPDATE ON workspace WHEN OLD.status IS NOT NEW.status BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT *,1 FROM (SELECT id FROM task WHERE id=NEW.task_id OR parent_task_id=NEW.task_id OR id IN (SELECT task_id FROM execution WHERE workspace_id=NEW.id)) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;

CREATE TRIGGER task_schedule_profile_update AFTER UPDATE ON agent_profile BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT *,1 FROM (SELECT id FROM task WHERE id IN (SELECT task_id FROM task_role_assignment WHERE assignee_id=NEW.identity_id) OR parent_task_id IN (SELECT task_id FROM task_role_assignment WHERE assignee_id=NEW.identity_id AND role_name='coder')) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;

CREATE TRIGGER task_schedule_workspace_delete AFTER DELETE ON workspace BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT *,1 FROM (SELECT id FROM task WHERE id=OLD.task_id OR parent_task_id=OLD.task_id OR id IN (SELECT task_id FROM execution WHERE workspace_id=OLD.id)) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;

CREATE TRIGGER task_schedule_profile_delete AFTER DELETE ON agent_profile BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT *,1 FROM (SELECT id FROM task WHERE id IN (SELECT task_id FROM task_role_assignment WHERE assignee_id=OLD.identity_id) OR parent_task_id IN (SELECT task_id FROM task_role_assignment WHERE assignee_id=OLD.identity_id AND role_name='coder')) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;

CREATE TRIGGER task_schedule_identity AFTER UPDATE ON agent_identity WHEN OLD.status IS NOT NEW.status OR OLD.paused IS NOT NEW.paused OR OLD.max_concurrent_tasks IS NOT NEW.max_concurrent_tasks BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT *,1 FROM (SELECT id FROM task WHERE id IN (SELECT task_id FROM task_role_assignment WHERE assignee_id=NEW.id) OR parent_task_id IN (SELECT task_id FROM task_role_assignment WHERE assignee_id=NEW.id AND role_name='coder')) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;

CREATE TRIGGER task_schedule_unpinned_daemon AFTER UPDATE ON daemon WHEN OLD.status IS NOT NEW.status OR OLD.detected_clis_json IS NOT NEW.detected_clis_json OR OLD.agent_version IS NOT NEW.agent_version BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT *,1 FROM (SELECT id FROM task WHERE id IN (SELECT a.task_id FROM task_role_assignment a JOIN agent_current g ON g.id=a.assignee_id WHERE g.daemon_id IS NULL AND g.executor_type IN (SELECT json_extract(value,'$.kind') FROM json_each(CASE WHEN json_valid(NEW.detected_clis_json) THEN NEW.detected_clis_json ELSE '[]' END)))) WHERE 1 ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;

-- Projects with no Tasks still own repository/readiness maintenance.
CREATE TABLE project_schedule_dirty (
    project_id TEXT PRIMARY KEY REFERENCES project(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL DEFAULT 1
);
CREATE TRIGGER project_schedule_insert AFTER INSERT ON project BEGIN
 INSERT INTO project_schedule_dirty(project_id) VALUES(NEW.id)
 ON CONFLICT(project_id) DO UPDATE SET generation=generation+1;
END;
CREATE TRIGGER project_schedule_update AFTER UPDATE ON project WHEN OLD.version IS NOT NEW.version OR OLD.primary_repo_id IS NOT NEW.primary_repo_id BEGIN
 INSERT INTO project_schedule_dirty(project_id) VALUES(NEW.id)
 ON CONFLICT(project_id) DO UPDATE SET generation=generation+1;
END;
CREATE TRIGGER project_schedule_repo_insert AFTER INSERT ON repo BEGIN
 INSERT INTO project_schedule_dirty(project_id) VALUES(NEW.project_id)
 ON CONFLICT(project_id) DO UPDATE SET generation=generation+1;
END;
CREATE TRIGGER project_schedule_repo_update AFTER UPDATE ON repo BEGIN
 INSERT INTO project_schedule_dirty(project_id) VALUES(NEW.project_id)
 ON CONFLICT(project_id) DO UPDATE SET generation=generation+1;
END;
CREATE TRIGGER project_schedule_repo_delete AFTER DELETE ON repo BEGIN
 INSERT INTO project_schedule_dirty(project_id) SELECT id FROM project WHERE id=OLD.project_id
 ON CONFLICT(project_id) DO UPDATE SET generation=generation+1;
END;

-- Chat turns consume the same machine slots as Task executions.
CREATE TRIGGER task_schedule_chat_insert AFTER INSERT ON agent_chat_turn_job WHEN NEW.status IN ('leased','running') BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT task_id,1 FROM task_schedule_wait WHERE daemon_id IN (COALESCE((SELECT daemon_id FROM agent_current WHERE id=NEW.responder_identity_id),''),'*') ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;
CREATE TRIGGER task_schedule_chat_update AFTER UPDATE ON agent_chat_turn_job WHEN (OLD.status IS NOT NEW.status OR OLD.responder_identity_id IS NOT NEW.responder_identity_id) AND (OLD.status IN ('leased','running') OR NEW.status IN ('leased','running')) BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT task_id,1 FROM task_schedule_wait WHERE daemon_id IN (COALESCE((SELECT daemon_id FROM agent_current WHERE id=NEW.responder_identity_id),''),'*') OR daemon_id IN (COALESCE((SELECT daemon_id FROM agent_current WHERE id=OLD.responder_identity_id),''),'*') ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;
CREATE TRIGGER task_schedule_chat_delete AFTER DELETE ON agent_chat_turn_job WHEN OLD.status IN ('leased','running') BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT task_id,1 FROM task_schedule_wait WHERE daemon_id IN (COALESCE((SELECT daemon_id FROM agent_current WHERE id=OLD.responder_identity_id),''),'*') ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;

-- Eligible queued admissions release Agent/machine references without creating
-- an execution when preparation refuses or is preempted.
CREATE TRIGGER task_schedule_admission_release AFTER UPDATE ON task_step WHEN OLD.kind='command' AND json_valid(OLD.payload_json) AND json_extract(OLD.payload_json,'$.admission_agent_id') IS NOT NULL AND (OLD.status IS NOT NEW.status OR OLD.available_at IS NOT NEW.available_at OR OLD.lease_until IS NOT NEW.lease_until) BEGIN
 INSERT INTO task_schedule_dirty(task_id,external) SELECT task_id,1 FROM task_schedule_wait WHERE agent_id=json_extract(OLD.payload_json,'$.admission_agent_id') OR daemon_id IN (COALESCE((SELECT COALESCE(p.execution_daemon_id,p.daemon_id) FROM workspace_placement p JOIN task t ON p.task_id=COALESCE(t.parent_task_id,t.id) WHERE t.id=OLD.task_id),(SELECT daemon_id FROM agent_current WHERE id=json_extract(OLD.payload_json,'$.admission_agent_id')),''),'*') ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1;
END;
