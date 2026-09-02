-- Evidence freshness is defined against the milestone's current definition
-- revision. The per-requirement `check_definition_revision` pin was stamped
-- with the genesis baseline's own revision id, carried verbatim through every
-- later revise by the authoring Agent, and then compared for equality with
-- the current revision an attachment is captured under — so every attachment
-- captured after the first revise was permanently `evidence_context_stale`.
-- The pin is gone from the AcceptanceEvidenceRequirement contract; strip it
-- from persisted definition content so stored revisions keep parsing. The
-- immutability trigger is lifted only for this rewrite and restored verbatim.
DROP TRIGGER project_milestone_revision_immutable_update;

UPDATE project_milestone_revision
SET evidence_requirements_json = (
    SELECT json_group_array(json_remove(je.value, '$.check_definition_revision') ORDER BY je.key)
    FROM json_each(project_milestone_revision.evidence_requirements_json) AS je
)
WHERE evidence_requirements_json LIKE '%check_definition_revision%';

CREATE TRIGGER project_milestone_revision_immutable_update
BEFORE UPDATE ON project_milestone_revision
WHEN OLD.id IS NOT NEW.id
  OR OLD.milestone_id IS NOT NEW.milestone_id
  OR OLD.revision IS NOT NEW.revision
  OR OLD.base_revision IS NOT NEW.base_revision
  OR OLD.base_revision_id IS NOT NEW.base_revision_id
  OR OLD.display_label IS NOT NEW.display_label
  OR OLD.outcome IS NOT NEW.outcome
  OR OLD.included_scope_json IS NOT NEW.included_scope_json
  OR OLD.excluded_scope_json IS NOT NEW.excluded_scope_json
  OR OLD.charter_revision_id IS NOT NEW.charter_revision_id
  OR OLD.document_revisions_json IS NOT NEW.document_revisions_json
  OR OLD.task_selection_json IS NOT NEW.task_selection_json
  OR OLD.dependencies_json IS NOT NEW.dependencies_json
  OR OLD.risks_json IS NOT NEW.risks_json
  OR OLD.acceptance_checks_json IS NOT NEW.acceptance_checks_json
  OR OLD.evidence_requirements_json IS NOT NEW.evidence_requirements_json
  OR OLD.known_issues_json IS NOT NEW.known_issues_json
  OR OLD.change_summary IS NOT NEW.change_summary
  OR OLD.schema_version IS NOT NEW.schema_version
  OR OLD.render_version IS NOT NEW.render_version
  OR OLD.rendered_view IS NOT NEW.rendered_view
  OR OLD.content_digest IS NOT NEW.content_digest
  OR OLD.rendered_digest IS NOT NEW.rendered_digest
  OR OLD.author_type IS NOT NEW.author_type
  OR OLD.author_id IS NOT NEW.author_id
  OR OLD.source_refs_json IS NOT NEW.source_refs_json
  OR OLD.created_at IS NOT NEW.created_at
BEGIN
    SELECT RAISE(ABORT, 'Milestone definition revisions are immutable');
END;
