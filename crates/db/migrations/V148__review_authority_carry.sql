-- A passed review's authority can be carried across a purely mechanical
-- integration step: a clean rebase onto a moved target, or a Worker's repair of
-- a Forge-committed rebase conflict. The frozen review contract still names the
-- commit and base the reviewer saw, so each carry records the candidate that is
-- actually integrated. `lock_review_integration` honours the newest row only
-- while it references the contract execution of the current passed review; a
-- newer real review supersedes every earlier carry automatically.

CREATE TABLE review_authority_carry (
    id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL REFERENCES task(id) ON DELETE CASCADE,
    contract_execution_id TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    base_sha TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('clean_rebase', 'conflict_repair')),
    changed_paths_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX idx_review_authority_carry_task
    ON review_authority_carry (task_id, contract_execution_id, created_at);

-- Every Project persists its own copy of the workflow definition, so the new
-- default reaches no existing Project on its own. Put `carry_review_authority`
-- at the front of each stored `review` state's `on_enter` list, ahead of
-- `dispatch_role_agent`. The hook skips (and the reviewer is dispatched as
-- before) unless every carry condition holds. Definitions without an `on_enter`
-- array on `review` (human-approval presets) have nothing to dispatch and are
-- left alone.

UPDATE project
SET workflow_definition = json_set(
    workflow_definition,
    '$.states[' || (
        SELECT state.key
        FROM json_each(project.workflow_definition, '$.states') AS state
        WHERE json_extract(state.value, '$.name') = 'review'
    ) || '].hooks.on_enter',
    json((
        SELECT json_group_array(json(ordered.hook))
        FROM (
            SELECT 0 AS position,
                   '{"action":"carry_review_authority","params":{},"applies_to":"all","on_failure":"log"}' AS hook
            UNION ALL
            SELECT 1 + hook.key AS position, hook.value AS hook
            FROM json_each(
                project.workflow_definition,
                '$.states[' || (
                    SELECT state.key
                    FROM json_each(project.workflow_definition, '$.states') AS state
                    WHERE json_extract(state.value, '$.name') = 'review'
                ) || '].hooks.on_enter'
            ) AS hook
            ORDER BY position
        ) AS ordered
    ))
)
WHERE json_valid(workflow_definition)
  AND instr(workflow_definition, '"carry_review_authority"') = 0
  AND EXISTS (
      SELECT 1
      FROM json_each(project.workflow_definition, '$.states') AS state
      WHERE json_extract(state.value, '$.name') = 'review'
        AND json_type(state.value, '$.hooks.on_enter') = 'array'
  );
