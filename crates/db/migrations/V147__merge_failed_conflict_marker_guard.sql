-- `merge_failed` gained a blocking `require_conflict_markers_resolved`
-- before_exit guard: a Worker's conflict repair that still adds conflict
-- markers in a handed-off file goes straight back to the Worker instead of
-- spending a full review and being rebased under another layer of markers.
--
-- Every Project persists its own copy of the workflow definition, so the new
-- default alone reaches no existing Project. Append the guard to each stored
-- `merge_failed` state that does not already carry it. The guard is inert for
-- Tasks that were never handed a conflict, so custom definitions get it too.

UPDATE project
SET workflow_definition = json_set(
    workflow_definition,
    '$.states[' || (
        SELECT state.key
        FROM json_each(project.workflow_definition, '$.states') AS state
        WHERE json_extract(state.value, '$.name') = 'merge_failed'
    ) || '].hooks.before_exit[#]',
    json('{"action":"require_conflict_markers_resolved","params":{},"applies_to":"all","on_failure":"block"}')
)
WHERE json_valid(workflow_definition)
  AND instr(workflow_definition, '"require_conflict_markers_resolved"') = 0
  AND EXISTS (
      SELECT 1
      FROM json_each(project.workflow_definition, '$.states') AS state
      WHERE json_extract(state.value, '$.name') = 'merge_failed'
        AND json_type(state.value, '$.hooks.before_exit') = 'array'
  );
