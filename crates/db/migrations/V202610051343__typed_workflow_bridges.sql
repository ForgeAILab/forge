-- Audit prose is preserved. Only this migration recognizes the former protocol.
-- Kind and purpose are open TEXT: Rust validates them on write and decodes an
-- unknown value as a typed error, so adding a kind never rebuilds these tables.
ALTER TABLE transition_log ADD COLUMN bridge_kind TEXT;
ALTER TABLE transition_log ADD COLUMN bridge_payload TEXT
    CHECK (bridge_payload IS NULL OR json_valid(bridge_payload));
ALTER TABLE execution ADD COLUMN purpose TEXT;
UPDATE execution SET purpose = 'automatic_review_recovery'
    WHERE summary LIKE '[Forge automatic review recovery]%';

-- A uniform work table lets history, saved commands/cascades/hooks, and hook
-- checkpoint results use exactly the same one-time interpretation.
CREATE TEMP TABLE bridge_backfill (
    source TEXT NOT NULL, id TEXT NOT NULL, reason TEXT, actor TEXT,
    from_state TEXT, to_state TEXT, trigger_name TEXT, hooks TEXT,
    kind TEXT, payload TEXT, step_id TEXT, object_path TEXT, PRIMARY KEY(source,id)
);
INSERT INTO bridge_backfill(source,id,reason,actor,from_state,to_state,trigger_name,hooks)
SELECT 'log',id,trigger_reason,triggered_by,from_state,to_state,trigger_name,hook_results_json
FROM transition_log;

INSERT INTO bridge_backfill(source,id,reason,actor,from_state,to_state,trigger_name)
SELECT 'step',s.id,
    CASE WHEN s.kind IN ('cascade','hooks') THEN json_extract(s.payload_json,'$.reason')
         WHEN json_extract(s.payload_json,'$.operation')='engine_transition' THEN json_extract(s.payload_json,'$.arguments.reason')
         ELSE json_extract(s.payload_json,'$.arguments[2].reason') END,
    CASE WHEN s.kind IN ('cascade','hooks') THEN 'system:workflow'
         WHEN json_extract(s.payload_json,'$.arguments.actor.System.component')='Workflow'
              OR json_extract(s.payload_json,'$.arguments[2].triggered_by.System.component')='Workflow' THEN 'system:workflow'
         WHEN json_extract(s.payload_json,'$.arguments[2].triggered_by.System.component')='TaskDispatcher' THEN 'system:task_dispatcher'
         WHEN json_extract(s.payload_json,'$.arguments.actor.User.source.Action.verb')='approve'
              OR json_extract(s.payload_json,'$.arguments[2].triggered_by.User.source.Action.verb')='approve' THEN 'user:action:approve'
         WHEN json_extract(s.payload_json,'$.arguments.actor.User.source.Action.verb')='send_back'
              OR json_extract(s.payload_json,'$.arguments[2].triggered_by.User.source.Action.verb')='send_back' THEN 'user:action:send_back'
         ELSE 'user:action' END,
    CASE WHEN s.kind='hooks' THEN json_extract(s.payload_json,'$.from') ELSE s.expected_status END,
    CASE WHEN s.kind IN ('cascade','hooks') THEN json_extract(s.payload_json,'$.to')
         WHEN json_extract(s.payload_json,'$.operation')='engine_transition' THEN json_extract(s.payload_json,'$.arguments.target_state')
         ELSE json_extract(s.payload_json,'$.arguments[1]') END,
    NULL
FROM task_step s WHERE json_valid(s.payload_json) AND (
    s.kind IN ('cascade','hooks') OR
    (s.kind='command' AND json_extract(s.payload_json,'$.operation') IN
       ('engine_transition','transition','transition_with_plan_publication')));

INSERT INTO bridge_backfill(source,id,reason,actor,from_state,to_state)
SELECT 'checkpoint',c.step_id || ':' || c.hook_index,
    json_extract(c.result_json,'$.Cascade.reason'),'system:workflow',
    CASE WHEN s.kind='hooks' THEN json_extract(s.payload_json,'$.to') ELSE s.expected_status END,
    json_extract(c.result_json,'$.Cascade.to')
FROM task_hook_checkpoint c JOIN task_step s ON s.id=c.step_id
WHERE json_valid(c.result_json) AND json_type(c.result_json,'$.Cascade')='object';

-- The single-writer queue also saves typed repository mutations with nested
-- CreateTransitionLog markers. Annotate those objects, without rewriting input
-- versions, status epochs, operation identity or audit prose.
INSERT INTO bridge_backfill(source,id,reason,actor,from_state,to_state,trigger_name,hooks,step_id,object_path)
SELECT 'nested-step',s.id || ':' || j.fullkey,
    json_extract(j.value,'$.trigger_reason'),json_extract(j.value,'$.triggered_by'),
    json_extract(j.value,'$.from_state'),json_extract(j.value,'$.to_state'),
    json_extract(j.value,'$.trigger_name'),json_extract(j.value,'$.hook_results_json'),
    s.id,j.fullkey
FROM task_step s,json_tree(CASE WHEN json_valid(s.payload_json) THEN s.payload_json ELSE '{}' END) j
WHERE s.kind='mutation' AND j.type='object'
  AND json_type(j.value,'$.trigger_reason')='text'
  AND json_type(j.value,'$.from_state')='text'
  AND json_type(j.value,'$.to_state')='text';

UPDATE bridge_backfill SET kind=CASE
    -- Explicit reset authority outranks tags quoted in user guidance.
    WHEN trigger_name IN ('restart','reset_to_initial','reset_retry_window') THEN 'retry_window_reset'
    WHEN trigger_name='retry' AND EXISTS (
       SELECT 1 FROM json_each(CASE WHEN json_valid(hooks) THEN hooks ELSE '[]' END) h
       WHERE json_extract(CASE WHEN h.type='object' THEN h.value ELSE '{}' END,'$.action')='retry'
         AND json_extract(CASE WHEN h.type='object' THEN h.value ELSE '{}' END,'$.phase')='action'
         AND json_extract(CASE WHEN h.type='object' THEN h.value ELSE '{}' END,'$.outcome')='reset_budget') THEN 'retry_window_reset'
    WHEN from_state='merging' AND to_state='merge_failed' AND actor='system:workflow'
      AND (ltrim(reason) LIKE '[conflict-handoff]%'
           OR ltrim(reason) LIKE '[review-refresh] [conflict-handoff]%') THEN 'conflict_handoff'
    WHEN actor='system:workflow' AND
      (ltrim(reason) LIKE '[review-refresh] [target-moved-rebase]%'
       OR ltrim(reason) LIKE '[target-moved-rebase]%') THEN 'target_moved_rebase'
    WHEN ltrim(reason) LIKE '[review-refresh]%' AND actor LIKE 'system:%' THEN 'review_refresh'
    WHEN reason GLOB 'gate skipped:*' AND actor LIKE 'system:%' THEN 'gate_skipped'
    -- Exactly the rows base read as gate decisions: a case-sensitive prefix,
    -- any actor. Custom approval guidance and send-backs were not decisions.
    WHEN reason GLOB 'gate approved*' THEN 'gate_approved'
    WHEN reason GLOB 'gate rejected*' THEN 'gate_rejected'
    WHEN reason='CI-only re-review passed' AND actor='system:workflow' THEN 'ci_only_review_passed'
    WHEN ltrim(reason) LIKE '[review-carry]%' AND actor='system:workflow' THEN 'review_carry'
    -- The same-state retry marker, whoever applied the action. The gate->target
    -- move that follows it is an ordinary rejection.
    WHEN trigger_name='retry' AND from_state=to_state THEN 'recovery'
    ELSE NULL END;
UPDATE bridge_backfill SET payload=json_object('verb',trigger_name)
WHERE kind IN ('retry_window_reset','recovery');

-- Recursively discard through each delimiter; only the final suffix is decoded.
-- json_valid is insufficient: require an array consisting exclusively of strings.
WITH RECURSIVE suffix(source,id,tail) AS (
    SELECT source,id,reason FROM bridge_backfill
    WHERE kind='conflict_handoff' AND instr(reason,'; paths_json=')>0
    UNION ALL
    SELECT source,id,substr(tail,instr(tail,'; paths_json=')+length('; paths_json='))
    FROM suffix WHERE instr(tail,'; paths_json=')>0
), decoded AS (
    SELECT source,id,tail FROM suffix WHERE instr(tail,'; paths_json=')=0
      AND CASE WHEN json_valid(tail) THEN json_type(tail)='array' ELSE 0 END
      AND NOT EXISTS (SELECT 1 FROM json_each(CASE WHEN json_valid(tail) THEN tail ELSE '[]' END) WHERE type!='text')
)
UPDATE bridge_backfill SET payload=(SELECT json_object('paths',json(tail)) FROM decoded d
    WHERE d.source=bridge_backfill.source AND d.id=bridge_backfill.id)
WHERE kind='conflict_handoff';

UPDATE transition_log SET
    bridge_kind=(SELECT kind FROM bridge_backfill b WHERE b.source='log' AND b.id=transition_log.id),
    bridge_payload=(SELECT payload FROM bridge_backfill b WHERE b.source='log' AND b.id=transition_log.id);

-- Hook steps pin their committed log, which is stronger authority than saved prose.
UPDATE bridge_backfill SET
    kind=(SELECT t.bridge_kind FROM task_step s JOIN transition_log t
          ON t.id=json_extract(s.payload_json,'$.transition_log_id') WHERE s.id=bridge_backfill.id),
    payload=(SELECT t.bridge_payload FROM task_step s JOIN transition_log t
          ON t.id=json_extract(s.payload_json,'$.transition_log_id') WHERE s.id=bridge_backfill.id)
WHERE source='step' AND EXISTS (SELECT 1 FROM task_step s JOIN transition_log t
    ON t.id=json_extract(s.payload_json,'$.transition_log_id') WHERE s.id=bridge_backfill.id AND s.kind='hooks');

UPDATE task_step SET payload_json=json_set(payload_json,
    CASE WHEN kind IN ('cascade','hooks') THEN '$.bridge_kind'
         WHEN json_extract(payload_json,'$.operation')='engine_transition' THEN '$.arguments.bridge_kind'
         ELSE '$.arguments[2].bridge_kind' END,
    (SELECT kind FROM bridge_backfill b WHERE b.source='step' AND b.id=task_step.id),
    CASE WHEN kind IN ('cascade','hooks') THEN '$.bridge_payload'
         WHEN json_extract(payload_json,'$.operation')='engine_transition' THEN '$.arguments.bridge_payload'
         ELSE '$.arguments[2].bridge_payload' END,
    json((SELECT payload FROM bridge_backfill b WHERE b.source='step' AND b.id=task_step.id)))
WHERE id IN (SELECT id FROM bridge_backfill WHERE source='step');

UPDATE task_hook_checkpoint SET result_json=json_set(result_json,
    '$.Cascade.bridge_kind',(SELECT kind FROM bridge_backfill b WHERE b.source='checkpoint'
        AND b.id=task_hook_checkpoint.step_id || ':' || task_hook_checkpoint.hook_index),
    '$.Cascade.bridge_payload',json((SELECT payload FROM bridge_backfill b WHERE b.source='checkpoint'
        AND b.id=task_hook_checkpoint.step_id || ':' || task_hook_checkpoint.hook_index)))
WHERE step_id || ':' || hook_index IN (SELECT id FROM bridge_backfill WHERE source='checkpoint');
WITH RECURSIVE entries AS (
    SELECT *,row_number() OVER (PARTITION BY step_id ORDER BY object_path) AS position
    FROM bridge_backfill WHERE source='nested-step'
), rewritten(step_id,position,document) AS (
    SELECT DISTINCT s.id,0,s.payload_json FROM task_step s JOIN entries e ON e.step_id=s.id
    UNION ALL
    SELECT r.step_id,e.position,json_set(r.document,
        e.object_path || '.bridge_kind',e.kind,
        e.object_path || '.bridge_payload',json(e.payload))
    FROM rewritten r JOIN entries e ON e.step_id=r.step_id AND e.position=r.position+1
)
UPDATE task_step SET payload_json=(SELECT document FROM rewritten r
    WHERE r.step_id=task_step.id ORDER BY position DESC LIMIT 1)
WHERE id IN (SELECT step_id FROM entries);

DROP TABLE bridge_backfill;
