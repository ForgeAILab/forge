-- V117 made complete admission and current Charter authority mandatory for
-- every active Project-Agent binding. During the existing guarded Project
-- teardown, deleting Charter rows intentionally drives ON DELETE SET NULL on
-- those binding columns before the Project cascade removes the binding. Keep
-- the authority invariant everywhere except that explicit Project-scoped
-- teardown transaction, matching the admission-receipt immutability guard.

DROP TRIGGER project_binding_complete_authority_guard_insert;
DROP TRIGGER project_binding_complete_authority_guard_update;

CREATE TRIGGER project_binding_complete_authority_guard_insert
BEFORE INSERT ON project_agent_binding
WHEN NEW.state = 'active'
 AND NEW.charter_setup_required = 0
 AND NOT EXISTS (
     SELECT 1 FROM project_deletion_guard g WHERE g.project_id = NEW.project_id
 )
BEGIN
    SELECT CASE
        WHEN NEW.identity_id IS NULL OR NEW.profile_id IS NULL
          OR NEW.operating_skill_revision_id IS NULL
          OR length(trim(NEW.policy_revision)) = 0
          OR length(trim(NEW.policy_digest)) = 0
          OR NEW.charter_id IS NULL OR NEW.charter_revision_id IS NULL
          OR NEW.admission_receipt_id IS NULL OR NEW.charter_approval_id IS NULL
        THEN RAISE(ABORT, 'Active Project binding requires complete admission and Charter authority')
        WHEN NOT EXISTS (
            SELECT 1 FROM project_admission_receipt receipt
            WHERE receipt.id = NEW.admission_receipt_id
              AND receipt.project_id = NEW.project_id
        ) THEN RAISE(ABORT, 'Project binding admission receipt is not Project-scoped')
        WHEN NOT EXISTS (
            SELECT 1
            FROM project_charter_approval a
            WHERE a.id = NEW.charter_approval_id
              AND a.charter_id = NEW.charter_id
              AND a.revision_id = NEW.charter_revision_id
              AND a.lifecycle = 'consumed'
              AND a.consumed_project_id = NEW.project_id
        ) THEN RAISE(ABORT, 'Project binding Charter approval does not match binding authority')
        WHEN NOT EXISTS (
            SELECT 1
            FROM operating_skill skill
            JOIN operating_skill_revision sr
              ON sr.id = skill.current_revision_id
             AND sr.operating_skill_id = skill.id
            WHERE skill.skill_key = 'forge.project.orchestration/v1'
              AND skill.lifecycle = 'active'
              AND sr.id = NEW.operating_skill_revision_id
        ) THEN RAISE(ABORT, 'Project binding operating skill revision does not exist')
        WHEN EXISTS (
            SELECT 1 FROM project p
            WHERE p.id = NEW.project_id
              AND p.charter_status = 'charter_backed'
              AND p.charter_setup_required = 0
              AND (p.current_charter_id IS NOT NEW.charter_id
                   OR p.current_charter_revision_id IS NOT NEW.charter_revision_id)
        ) THEN RAISE(ABORT, 'Project binding Charter authority is not current')
    END;
END;

CREATE TRIGGER project_binding_complete_authority_guard_update
BEFORE UPDATE OF state, charter_setup_required, identity_id, profile_id,
    operating_skill_revision_id, policy_revision, policy_digest,
    charter_id, charter_revision_id, admission_receipt_id, charter_approval_id
ON project_agent_binding
WHEN NEW.state = 'active'
 AND NEW.charter_setup_required = 0
 AND NOT EXISTS (
     SELECT 1 FROM project_deletion_guard g WHERE g.project_id = NEW.project_id
 )
BEGIN
    SELECT CASE
        WHEN NEW.identity_id IS NULL OR NEW.profile_id IS NULL
          OR NEW.operating_skill_revision_id IS NULL
          OR length(trim(NEW.policy_revision)) = 0
          OR length(trim(NEW.policy_digest)) = 0
          OR NEW.charter_id IS NULL OR NEW.charter_revision_id IS NULL
          OR NEW.admission_receipt_id IS NULL OR NEW.charter_approval_id IS NULL
        THEN RAISE(ABORT, 'Active Project binding requires complete admission and Charter authority')
        WHEN NOT EXISTS (
            SELECT 1 FROM project_admission_receipt receipt
            WHERE receipt.id = NEW.admission_receipt_id
              AND receipt.project_id = NEW.project_id
        ) THEN RAISE(ABORT, 'Project binding admission receipt is not Project-scoped')
        WHEN NOT EXISTS (
            SELECT 1
            FROM project_charter_approval a
            WHERE a.id = NEW.charter_approval_id
              AND a.charter_id = NEW.charter_id
              AND a.revision_id = NEW.charter_revision_id
              AND a.lifecycle = 'consumed'
              AND a.consumed_project_id = NEW.project_id
        ) THEN RAISE(ABORT, 'Project binding Charter approval does not match binding authority')
        WHEN NOT EXISTS (
            SELECT 1
            FROM operating_skill skill
            JOIN operating_skill_revision sr
              ON sr.id = skill.current_revision_id
             AND sr.operating_skill_id = skill.id
            WHERE skill.skill_key = 'forge.project.orchestration/v1'
              AND skill.lifecycle = 'active'
              AND sr.id = NEW.operating_skill_revision_id
        ) THEN RAISE(ABORT, 'Project binding operating skill revision does not exist')
        WHEN EXISTS (
            SELECT 1 FROM project p
            WHERE p.id = NEW.project_id
              AND p.charter_status = 'charter_backed'
              AND p.charter_setup_required = 0
              AND (p.current_charter_id IS NOT NEW.charter_id
                   OR p.current_charter_revision_id IS NOT NEW.charter_revision_id)
        ) THEN RAISE(ABORT, 'Project binding Charter authority is not current')
    END;
END;
