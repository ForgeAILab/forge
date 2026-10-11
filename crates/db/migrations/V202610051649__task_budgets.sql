-- Snapshot retained allowances once. Audit/authority evidence remains intact.
CREATE TABLE task_budget (
 task_id TEXT NOT NULL REFERENCES task(id) ON DELETE CASCADE,
 kind TEXT NOT NULL,
 window_id TEXT NOT NULL,
 spent INTEGER NOT NULL DEFAULT 0 CHECK(spent >= 0),
 PRIMARY KEY(task_id,kind)
);
CREATE TABLE task_budget_charge (
 task_id TEXT NOT NULL, kind TEXT NOT NULL, window_id TEXT NOT NULL, step_id TEXT NOT NULL,
 PRIMARY KEY(task_id,kind,window_id,step_id),
 FOREIGN KEY(task_id,kind) REFERENCES task_budget(task_id,kind) ON DELETE CASCADE
);
-- A Project whose stored workflow has no states array (the column default '{}',
-- or JSON the runtime cannot parse) runs the default workflow. Its gates as of
-- this release: planning 2, review 2, merging 1 rejections.
CREATE TEMP TABLE budget_workflow AS
SELECT p.id AS project_id,
 CASE WHEN json_valid(p.workflow_definition) AND json_type(p.workflow_definition,'$.states')='array'
 THEN p.workflow_definition
 ELSE '{"states":[{"name":"planning","kind":"gate","gate_config":{"max_rejections":2}},{"name":"review","kind":"gate","gate_config":{"max_rejections":2}},{"name":"merging","kind":"gate","gate_config":{"max_rejections":1}}]}'
 END AS definition
FROM project p;
-- Defaults have no spend; generic gates below are parameterized by their state.
INSERT INTO task_budget(task_id,kind,window_id,spent)
SELECT t.id,k.value,'initial',0 FROM task t CROSS JOIN json_each(
 '["review","merge_fix","execution","workflow_guard","target_moved_rebase","conflict_handoff","review_carry","automatic_review_recovery","review_ci_infrastructure"]') k;
INSERT OR IGNORE INTO task_budget(task_id,kind,window_id,spent)
SELECT t.id,CASE json_extract(s.value,'$.name') WHEN 'review' THEN 'review' ELSE 'gate:'||json_extract(s.value,'$.name') END,'initial',0
FROM task t JOIN budget_workflow w ON w.project_id=t.project_id JOIN json_each(w.definition,'$.states') s
WHERE json_extract(s.value,'$.kind')='gate' AND json_extract(s.value,'$.gate_config.max_rejections') IS NOT NULL;
INSERT OR IGNORE INTO task_budget(task_id,kind,window_id,spent)
SELECT t.id,'gate:review','initial',0 FROM task t JOIN budget_workflow w ON w.project_id=t.project_id JOIN json_each(w.definition,'$.states') s WHERE json_extract(s.value,'$.name')='review' AND json_extract(s.value,'$.gate_config.max_rejections') IS NOT NULL;
-- Match the old per-origin reset, not merely the last reset of another gate.
UPDATE task_budget AS b SET spent=(
 SELECT COUNT(*) FROM transition_log r WHERE r.task_id=b.task_id AND r.rejection=1
 AND r.from_state=CASE b.kind WHEN 'review' THEN 'review' WHEN 'merge_fix' THEN 'merging' ELSE substr(b.kind,6) END
 AND NOT EXISTS(SELECT 1 FROM transition_log boundary WHERE boundary.task_id=r.task_id
 AND boundary.from_state=r.from_state AND boundary.rejection=0 AND boundary.bridge_kind='retry_window_reset'
 AND (boundary.created_at>r.created_at OR (boundary.created_at=r.created_at AND boundary.rowid>r.rowid))))
WHERE kind IN ('review','merge_fix') OR kind LIKE 'gate:%';
UPDATE task_budget AS b SET window_id=COALESCE((SELECT boundary.id FROM transition_log boundary WHERE boundary.task_id=b.task_id AND boundary.rejection=0 AND boundary.bridge_kind='retry_window_reset'
AND boundary.from_state=CASE b.kind WHEN 'review' THEN 'review' WHEN 'merge_fix' THEN 'merging' ELSE substr(b.kind,6) END ORDER BY boundary.created_at DESC,boundary.rowid DESC LIMIT 1),'initial')
WHERE kind IN ('review','merge_fix') OR kind LIKE 'gate:%';
-- A parked final verdict was not logged as a bounce. Preserve its zero allowance.
UPDATE task_budget AS b SET spent=MAX(spent,COALESCE(
 (SELECT json_extract(t.task_state_config,'$.retry_budgets.review') FROM task t WHERE t.id=b.task_id AND json_valid(t.task_state_config)),
 (SELECT json_extract(t.task_state_config,'$.review.retry_budgets.review') FROM task t WHERE t.id=b.task_id AND json_valid(t.task_state_config)),
 (SELECT json_extract(s.value,'$.config.retry_budgets.review') FROM task t JOIN budget_workflow w ON w.project_id=t.project_id JOIN json_each(w.definition,'$.states') s WHERE t.id=b.task_id AND json_extract(s.value,'$.name')='review'),
 (SELECT json_extract(s.value,'$.gate_config.max_rejections') FROM task t JOIN budget_workflow w ON w.project_id=t.project_id JOIN json_each(w.definition,'$.states') s WHERE t.id=b.task_id AND json_extract(s.value,'$.name')='review'),2))
WHERE kind='review' AND EXISTS(SELECT 1 FROM task t WHERE t.id=b.task_id AND
 ((json_valid(t.error_annotation) AND json_extract(t.error_annotation,'$.type')='review_budget_exhausted') OR
 (json_valid(t.entry_barrier_json) AND json_extract(t.entry_barrier_json,'$.blocking_reason')='review retry budget exhausted')));
UPDATE task_budget AS b SET spent=COALESCE((SELECT CASE WHEN json_valid(t.metadata_json) AND json_type(t.metadata_json,'$.execution_retry_count')='integer' THEN MIN(9223372036854775807,MAX(0,json_extract(t.metadata_json,'$.execution_retry_count'))) ELSE 0 END FROM task t WHERE t.id=b.task_id),0) WHERE kind='execution';
UPDATE task_budget AS b SET spent=COALESCE((SELECT CASE WHEN json_valid(t.metadata_json) AND json_type(t.metadata_json,'$.workflow_guard_retry_count')='integer' THEN MIN(9223372036854775807,MAX(0,json_extract(t.metadata_json,'$.workflow_guard_retry_count'))) ELSE 0 END FROM task t WHERE t.id=b.task_id),0) WHERE kind='workflow_guard';
UPDATE task_budget AS b SET spent=(SELECT COUNT(*) FROM transition_log r WHERE r.task_id=b.task_id AND r.bridge_kind=b.kind AND r.triggered_by='system:workflow'
 AND NOT EXISTS(SELECT 1 FROM transition_log boundary WHERE boundary.task_id=r.task_id AND boundary.rejection=0 AND boundary.bridge_kind='retry_window_reset'
 AND (boundary.created_at>r.created_at OR (boundary.created_at=r.created_at AND boundary.rowid>r.rowid)))) WHERE kind IN ('target_moved_rebase','conflict_handoff');
UPDATE task_budget AS b SET window_id=COALESCE((SELECT boundary.id FROM transition_log boundary WHERE boundary.task_id=b.task_id AND boundary.rejection=0 AND boundary.bridge_kind='retry_window_reset' ORDER BY boundary.created_at DESC,boundary.rowid DESC LIMIT 1),'initial') WHERE kind IN ('execution','target_moved_rebase','conflict_handoff');
UPDATE task_budget AS b SET window_id=COALESCE((SELECT json_extract(r.step_results_json,'$.conformance.contract.execution_id') FROM review r WHERE r.task_id=b.task_id AND r.status='passed' AND json_valid(r.step_results_json) ORDER BY r.attempt_number DESC,r.id DESC LIMIT 1),'initial') WHERE kind='review_carry';
UPDATE task_budget AS b SET spent=(SELECT COUNT(*) FROM review_authority_carry c WHERE c.task_id=b.task_id AND c.contract_execution_id=b.window_id) WHERE kind='review_carry';
UPDATE task_budget AS b SET window_id=COALESCE((SELECT 'exhaustion:'||r.id FROM review r WHERE r.task_id=b.task_id AND r.status='failed' ORDER BY r.attempt_number DESC,r.id DESC LIMIT 1),'initial') WHERE kind='automatic_review_recovery';
UPDATE task_budget AS b SET spent=(SELECT COUNT(*) FROM execution e WHERE e.task_id=b.task_id AND e.purpose='automatic_review_recovery'
 AND (e.status='running' OR (e.status='cancelled' AND e.created_at>=COALESCE((SELECT r.finished_at FROM review r WHERE 'exhaustion:'||r.id=b.window_id),e.created_at)))) WHERE kind='automatic_review_recovery';
UPDATE task_budget AS b SET spent=COALESCE((SELECT CASE WHEN json_valid(t.entry_barrier_json) AND json_type(t.entry_barrier_json,'$.infrastructure_attempts')='integer' THEN MAX(0,json_extract(t.entry_barrier_json,'$.infrastructure_attempts')) ELSE 0 END FROM task t WHERE t.id=b.task_id),0),
window_id=COALESCE((SELECT CASE WHEN json_valid(t.entry_barrier_json) THEN json_extract(t.entry_barrier_json,'$.started_at') END FROM task t WHERE t.id=b.task_id),'initial') WHERE kind='review_ci_infrastructure';
-- Mark the latest already-routed/exhausted failed verdict paid. An unfinished
-- verdict with no rejection/disposition is charged when its durable step resumes.
INSERT INTO task_budget_charge(task_id,kind,window_id,step_id)
SELECT b.task_id,'review',b.window_id,'review:'||r.id FROM task_budget b JOIN review r ON r.task_id=b.task_id
WHERE b.kind='review' AND r.status='failed'
AND r.id=(SELECT latest.id FROM review latest WHERE latest.task_id=r.task_id ORDER BY latest.attempt_number DESC,latest.id DESC LIMIT 1)
AND (EXISTS(SELECT 1 FROM transition_log l WHERE l.task_id=r.task_id AND l.from_state='review' AND l.rejection=1 AND l.created_at>=COALESCE(r.finished_at,r.updated_at))
OR EXISTS(SELECT 1 FROM task t WHERE t.id=r.task_id AND ((json_valid(t.error_annotation) AND json_extract(t.error_annotation,'$.type')='review_budget_exhausted') OR (json_valid(t.entry_barrier_json) AND json_extract(t.entry_barrier_json,'$.blocking_reason')='review retry budget exhausted'))));
UPDATE task SET metadata_json=json_remove(metadata_json,'$.execution_retry_count','$.workflow_guard_retry_count') WHERE json_valid(metadata_json) AND (json_type(metadata_json,'$.execution_retry_count') IS NOT NULL OR json_type(metadata_json,'$.workflow_guard_retry_count') IS NOT NULL);
UPDATE task SET entry_barrier_json=json_remove(entry_barrier_json,'$.infrastructure_attempts') WHERE json_valid(entry_barrier_json) AND json_type(entry_barrier_json,'$.infrastructure_attempts') IS NOT NULL;
DROP TABLE budget_workflow;
CREATE TRIGGER task_list_revision_budget_insert AFTER INSERT ON task_budget BEGIN
 UPDATE project SET list_revision=list_revision+1 WHERE id=(SELECT project_id FROM task WHERE id=NEW.task_id);
END;
CREATE TRIGGER task_list_revision_budget_update AFTER UPDATE ON task_budget WHEN OLD.spent!=NEW.spent OR OLD.window_id!=NEW.window_id BEGIN
 UPDATE project SET list_revision=list_revision+1 WHERE id=(SELECT project_id FROM task WHERE id=NEW.task_id);
END;
CREATE TRIGGER task_list_revision_budget_delete AFTER DELETE ON task_budget BEGIN
 UPDATE project SET list_revision=list_revision+1 WHERE id=(SELECT project_id FROM task WHERE id=OLD.task_id);
END;
