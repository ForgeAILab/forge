-- Drop the execution baseline. The Charter is the authority.
--
-- Forge pinned approved intent three times: the Project Charter revision, the
-- milestone definition revision, and an execution baseline revision that
-- mostly stored pointers back to the first two plus a release policy. The
-- policy was never interpreted -- `evaluate_readiness` reads acceptance checks
-- and evidence requirements from the milestone definition and nothing from the
-- policy's thirteen rule lists -- so the baseline enforced nothing while
-- creating a third artifact that had to be approved, activated, and
-- continuously reconciled against the two that already governed.
--
-- The cost was visible: a Project could deliver every acceptance check and
-- still be told it had "no active approved execution baseline", and roughly
-- two hundred lines of readiness code existed only to check that the baseline
-- agreed with the records it pointed at.
--
-- Readiness is now: every required acceptance check on the approved milestone
-- definition has a current passing result, and required evidence is attached.
-- Two immutable pins remain -- the Charter revision and the milestone
-- definition revision -- both digested into `readiness_digest` and both
-- re-verified at release.

PRAGMA foreign_keys = OFF;
BEGIN IMMEDIATE;

-- Triggers that read baseline columns or join baseline tables. They come down
-- before the columns and tables they reference and go back up below without
-- the baseline clauses.
DROP TRIGGER IF EXISTS project_decision_scope_guard;
DROP TRIGGER IF EXISTS project_milestone_check_result_scope_guard;
DROP TRIGGER IF EXISTS project_readiness_snapshot_scope_guard;
DROP TRIGGER IF EXISTS project_release_scope_guard;
DROP TRIGGER IF EXISTS project_task_governance_immutable_update;
DROP TRIGGER IF EXISTS project_task_governance_scope_guard_insert;
DROP TRIGGER IF EXISTS workspace_lease_active_renewal_guard;

-- Triggers that exist only to police the baseline tables.
DROP TRIGGER IF EXISTS execution_baseline_approval_integrity_guard;
DROP TRIGGER IF EXISTS execution_baseline_pointer_integrity_guard;
DROP TRIGGER IF EXISTS project_execution_baseline_approval_immutable_delete;
DROP TRIGGER IF EXISTS project_execution_baseline_approval_immutable_update;
DROP TRIGGER IF EXISTS project_execution_baseline_approval_lifecycle_guard;
DROP TRIGGER IF EXISTS project_execution_baseline_approval_scope_guard;
DROP TRIGGER IF EXISTS project_execution_baseline_pointer_scope_guard;
DROP TRIGGER IF EXISTS project_execution_baseline_revision_base_scope_guard;
DROP TRIGGER IF EXISTS project_execution_baseline_revision_immutable_delete;
DROP TRIGGER IF EXISTS project_execution_baseline_revision_immutable_update;
DROP TRIGGER IF EXISTS project_execution_baseline_revision_scope_guard;

DROP INDEX IF EXISTS idx_project_execution_baseline_active;
DROP INDEX IF EXISTS idx_project_execution_baseline_revision_history;
DROP INDEX IF EXISTS idx_project_task_governance_baseline;

-- Baseline columns on the records that survive.
ALTER TABLE project_readiness_snapshot DROP COLUMN baseline_id;
ALTER TABLE project_readiness_snapshot DROP COLUMN baseline_revision_id;
ALTER TABLE project_readiness_snapshot DROP COLUMN baseline_digest;
ALTER TABLE project_readiness_snapshot DROP COLUMN release_policy_revision;
ALTER TABLE project_readiness_snapshot DROP COLUMN release_policy_digest;

ALTER TABLE project_release DROP COLUMN baseline_id;
ALTER TABLE project_release DROP COLUMN baseline_revision_id;
ALTER TABLE project_release DROP COLUMN baseline_digest;
ALTER TABLE project_release DROP COLUMN release_policy_revision;
ALTER TABLE project_release DROP COLUMN release_policy_digest;

ALTER TABLE project_task_governance DROP COLUMN baseline_id;
ALTER TABLE project_task_governance DROP COLUMN baseline_revision_id;

ALTER TABLE project_milestone_check_result DROP COLUMN governing_baseline_revision_id;

ALTER TABLE project_decision DROP COLUMN baseline_revision_id;

-- The baseline itself.
DROP TABLE IF EXISTS execution_baseline_revision_integrity;
DROP TABLE IF EXISTS project_execution_baseline_approval;
DROP TABLE IF EXISTS project_execution_baseline_revision;
DROP TABLE IF EXISTS project_execution_baseline;

-- Recreated guards, baseline clauses removed and nothing else changed.

CREATE TRIGGER project_decision_scope_guard
BEFORE INSERT ON project_decision
BEGIN
    SELECT CASE
        WHEN NEW.charter_revision_id IS NOT NULL
         AND NOT EXISTS (
             SELECT 1
             FROM project_charter_revision r
             JOIN project_charter c ON c.id = r.charter_id
             WHERE r.id = NEW.charter_revision_id
               AND c.project_id = NEW.project_id
         ) THEN RAISE(ABORT, 'Project Decision Charter revision is cross-Project')
        WHEN NEW.supersedes_decision_id IS NOT NULL
         AND NOT EXISTS (
             SELECT 1 FROM project_decision prior
             WHERE prior.id = NEW.supersedes_decision_id
               AND prior.project_id = NEW.project_id
         ) THEN RAISE(ABORT, 'Project Decision supersession is cross-Project')
    END;
END;

CREATE TRIGGER project_milestone_check_result_scope_guard
BEFORE INSERT ON project_milestone_check_result
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM project_milestone_check c
            WHERE c.id = NEW.check_id
              AND c.project_id = NEW.project_id
              AND c.milestone_id = NEW.milestone_id
              AND c.definition_revision_id = NEW.definition_revision_id
              AND c.source_kind = NEW.source_kind
        ) THEN RAISE(ABORT, 'Milestone check result is cross-scope or mismatched')
        WHEN NEW.governing_charter_revision_id IS NOT NULL
         AND NOT EXISTS (
             SELECT 1
             FROM project_charter_revision cr
             JOIN project_charter c ON c.id = cr.charter_id
             WHERE cr.id = NEW.governing_charter_revision_id
               AND c.project_id = NEW.project_id
         ) THEN RAISE(ABORT, 'Milestone check governing Charter is cross-Project')
    END;
END;

CREATE TRIGGER project_readiness_snapshot_scope_guard
BEFORE INSERT ON project_readiness_snapshot
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM project_milestone
            WHERE id = NEW.milestone_id AND project_id = NEW.project_id
        ) THEN RAISE(ABORT, 'Readiness snapshot milestone is cross-Project')
        WHEN NOT EXISTS (
            SELECT 1 FROM project_milestone_revision
            WHERE id = NEW.definition_revision_id AND milestone_id = NEW.milestone_id
        ) THEN RAISE(ABORT, 'Readiness snapshot definition is not milestone-scoped')
    END;
END;

CREATE TRIGGER project_release_scope_guard
BEFORE INSERT ON project_release
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1 FROM project_milestone
            WHERE id = NEW.milestone_id AND project_id = NEW.project_id
        ) THEN RAISE(ABORT, 'Release milestone is cross-Project')
        WHEN NOT EXISTS (
            SELECT 1 FROM project_milestone_revision
            WHERE id = NEW.milestone_revision_id AND milestone_id = NEW.milestone_id
        ) THEN RAISE(ABORT, 'Release milestone revision is not milestone-scoped')
        WHEN NOT EXISTS (
            SELECT 1 FROM project_readiness_snapshot
            WHERE id = NEW.readiness_snapshot_id
              AND project_id = NEW.project_id
              AND milestone_id = NEW.milestone_id
              AND outcome = 'ready'
              AND readiness_digest = NEW.readiness_digest
        ) THEN RAISE(ABORT, 'Release readiness snapshot does not match exact digest')
        WHEN NEW.charter_revision_id IS NOT NULL
         AND NOT EXISTS (
            SELECT 1
            FROM project_charter c
            JOIN project_charter_revision cr ON cr.id = NEW.charter_revision_id
             AND cr.charter_id = c.id
            WHERE c.project_id = NEW.project_id
              AND c.current_approved_revision_id = cr.id
              AND cr.lifecycle = 'approved'
        ) THEN RAISE(ABORT, 'Release Charter revision is not the approved Project Charter')
        WHEN NEW.release_identifier != (
            SELECT milestone_key || '-r' || NEW.release_revision
            FROM project_milestone WHERE id = NEW.milestone_id
        ) THEN RAISE(ABORT, 'Release identifier must be Mxxx-rN for its milestone')
        WHEN NEW.release_revision != COALESCE((
            SELECT MAX(release_revision) + 1
            FROM project_release WHERE milestone_id = NEW.milestone_id
        ), 1) THEN RAISE(ABORT, 'Release revisions must be appended monotonically')
    END;
END;

CREATE TRIGGER project_task_governance_immutable_update
BEFORE UPDATE ON project_task_governance
WHEN OLD.task_id IS NOT NEW.task_id
  OR OLD.project_id IS NOT NEW.project_id
  OR OLD.charter_revision_id IS NOT NEW.charter_revision_id
  OR OLD.plan_item_id IS NOT NEW.plan_item_id
  OR OLD.milestone_id IS NOT NEW.milestone_id
  OR OLD.document_revisions_json IS NOT NEW.document_revisions_json
  OR OLD.capability_class IS NOT NEW.capability_class
  OR OLD.risk_class IS NOT NEW.risk_class
  OR OLD.replacement_of_task_id IS NOT NEW.replacement_of_task_id
  OR OLD.provenance_json IS NOT NEW.provenance_json
  OR OLD.created_at IS NOT NEW.created_at
BEGIN
    SELECT RAISE(ABORT, 'Task governance links are immutable');
END;

CREATE TRIGGER project_task_governance_scope_guard_insert
BEFORE INSERT ON project_task_governance
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (SELECT 1 FROM task WHERE id = NEW.task_id AND project_id = NEW.project_id)
        THEN RAISE(ABORT, 'Task governance link must belong to same Project')
        WHEN NEW.charter_revision_id IS NOT NULL
         AND NOT EXISTS (
             SELECT 1 FROM project_charter_revision r
             JOIN project_charter c ON c.id = r.charter_id
             WHERE r.id = NEW.charter_revision_id AND c.project_id = NEW.project_id
         ) THEN RAISE(ABORT, 'Task Charter governance link is cross-Project')
        WHEN NEW.milestone_id IS NOT NULL
         AND NOT EXISTS (
             SELECT 1 FROM project_milestone
             WHERE id = NEW.milestone_id AND project_id = NEW.project_id
         ) THEN RAISE(ABORT, 'Task milestone governance link is cross-Project')
    END;
END;

-- Lease authority is the current approved Charter plus a runnable governance
-- row. The read-only planning fallback no longer has to assert that no
-- baseline is present, because there is no baseline to assert about.
CREATE TRIGGER workspace_lease_active_renewal_guard
BEFORE UPDATE ON workspace_lease
WHEN OLD.status = 'active' AND NEW.status = 'active'
BEGIN
    SELECT CASE
        WHEN NEW.expires_at <= OLD.expires_at
          OR NEW.updated_at IS OLD.updated_at
        THEN RAISE(ABORT, 'Workspace lease renewal must extend expiry')
        WHEN EXISTS (
            SELECT 1 FROM project_agent_binding
            WHERE project_id = NEW.project_id
              AND identity_id = NEW.assigned_principal_id
              AND state = 'active'
        ) OR EXISTS (
            SELECT 1 FROM account_main_agent_binding
            WHERE identity_id = NEW.assigned_principal_id
              AND state = 'active'
        ) THEN RAISE(ABORT, 'Orchestration agents cannot receive Workspace leases')
        WHEN NOT EXISTS (
            SELECT 1
            FROM task t
            JOIN project p ON p.id = t.project_id
            JOIN execution e ON e.id = NEW.execution_id
            LEFT JOIN project_task_governance g
              ON g.task_id = t.id AND g.project_id = p.id
            WHERE t.id = NEW.task_id
              AND t.project_id = NEW.project_id
              AND t.version = NEW.task_version
              AND t.repo_id = NEW.repository_binding_id
              AND e.task_id = NEW.task_id
              AND e.status = 'running'
              AND e.agent_id = NEW.assigned_principal_id
              AND ((NEW.role = 'reviewer' AND e.role = 'reviewer')
                   OR (NEW.role = 'worker' AND e.role != 'reviewer'))
              AND (
                  (t.assignee_type = NEW.assigned_principal_type
                   AND t.assignee_id = NEW.assigned_principal_id)
                  OR EXISTS (
                      SELECT 1 FROM task_role_assignment ra
                      WHERE ra.task_id = NEW.task_id
                        AND ra.role_name = e.role
                        AND ra.assignee_type = NEW.assigned_principal_type
                        AND ra.assignee_id = NEW.assigned_principal_id
                  )
                  OR ((p.charter_status != 'charter_backed'
                       OR p.charter_setup_required != 0)
                      AND t.assignee_type IS NULL AND t.assignee_id IS NULL)
              )
              AND json_array_length(NEW.capabilities_json) = 1
              AND json_extract(NEW.capabilities_json, '$[0]') =
                  COALESCE(g.capability_class,
                    CASE WHEN t.task_type IN ('planning_task', 'discovery')
                         THEN 'repository_read' ELSE 'repository_write' END)
              AND (
                  p.charter_status != 'charter_backed'
                  OR p.charter_setup_required != 0
                  OR (
                      g.runnable = 1
                      AND g.charter_revision_id = p.current_charter_revision_id
                  )
                  OR (
                      g.runnable = 0
                      AND g.charter_revision_id = p.current_charter_revision_id
                      AND t.task_type IN ('planning_task', 'discovery')
                      AND g.capability_class IN
                          ('repository_read', 'read_only', 'discovery_read', 'planning_read')
                  )
              )
        ) THEN RAISE(ABORT, 'Workspace lease renewal authority is stale')
    END;
END;

COMMIT;
PRAGMA foreign_keys = ON;
