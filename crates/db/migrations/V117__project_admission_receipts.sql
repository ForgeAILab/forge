-- Freeze the one-time Project admission proof separately from current
-- Project-Agent binding and Charter authority. Historical Genesis handoffs
-- remain immutable context; later bindings and Charter approvals point back
-- to this stable Project-owned receipt.

CREATE TABLE project_admission_receipt (
    id                          TEXT PRIMARY KEY,
    project_id                  TEXT NOT NULL UNIQUE REFERENCES project(id) ON DELETE CASCADE,
    source_kind                 TEXT NOT NULL
                                    CHECK (source_kind IN ('genesis_handoff', 'charter_adoption')),
    handoff_id                  TEXT,
    initial_charter_approval_id TEXT NOT NULL,
    initial_charter_id          TEXT NOT NULL,
    initial_charter_revision_id TEXT NOT NULL,
    payload_digest              TEXT NOT NULL CHECK (length(trim(payload_digest)) > 0),
    validation_schema_version   TEXT NOT NULL
                                    CHECK (length(trim(validation_schema_version)) > 0),
    validated_at                TEXT NOT NULL,
    created_at                  TEXT NOT NULL,
    CHECK (
        (source_kind = 'genesis_handoff' AND handoff_id IS NOT NULL)
        OR (source_kind = 'charter_adoption' AND handoff_id IS NULL)
    )
);

CREATE INDEX idx_project_admission_receipt_handoff
    ON project_admission_receipt(handoff_id)
    WHERE handoff_id IS NOT NULL;
CREATE INDEX idx_project_admission_receipt_initial_approval
    ON project_admission_receipt(initial_charter_approval_id);

CREATE TRIGGER project_admission_receipt_scope_guard_insert
BEFORE INSERT ON project_admission_receipt
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM project p
            JOIN project_charter c ON c.id = NEW.initial_charter_id
            JOIN project_charter_revision r
              ON r.id = NEW.initial_charter_revision_id
             AND r.charter_id = c.id
            JOIN project_charter_approval a
              ON a.id = NEW.initial_charter_approval_id
             AND a.charter_id = c.id
             AND a.revision_id = r.id
            WHERE p.id = NEW.project_id
              AND c.project_id = p.id
              AND a.lifecycle = 'consumed'
              AND a.consumed_project_id = p.id
              AND ((NEW.source_kind = 'genesis_handoff'
                    AND a.approval_type = 'project_creation'
                    AND c.genesis_session_id IS NOT NULL)
                   OR (NEW.source_kind = 'charter_adoption'
                    AND a.approval_type = 'adoption'
                    AND c.genesis_session_id IS NULL))
        ) THEN RAISE(ABORT, 'Project admission receipt source is not consumed Project authority')
        WHEN NEW.source_kind = 'genesis_handoff' AND NOT EXISTS (
            SELECT 1
            FROM agent_handoff h
            JOIN agent_chat target ON target.id = h.target_chat_id
            WHERE h.id = NEW.handoff_id
              AND target.kind = 'project'
              AND target.project_id = NEW.project_id
              AND json_valid(h.source_revisions_json)
              AND json_extract(h.source_revisions_json, '$.request.source_revisions_digest') = NEW.payload_digest
        ) THEN RAISE(ABORT, 'Genesis admission receipt handoff is not Project-scoped or digest-matched')
        WHEN NEW.source_kind = 'charter_adoption' AND NOT EXISTS (
            SELECT 1 FROM project_charter_approval a
            WHERE a.id = NEW.initial_charter_approval_id
              AND a.content_digest = NEW.payload_digest
        ) THEN RAISE(ABORT, 'Adoption admission receipt digest does not match approval')
    END;
END;

CREATE TRIGGER project_admission_receipt_immutable_update
BEFORE UPDATE ON project_admission_receipt
BEGIN
    SELECT RAISE(ABORT, 'Project admission receipts are immutable');
END;

CREATE TRIGGER project_admission_receipt_immutable_delete
BEFORE DELETE ON project_admission_receipt
WHEN NOT EXISTS (
    SELECT 1 FROM project_deletion_guard g WHERE g.project_id = OLD.project_id
)
BEGIN
    SELECT RAISE(ABORT, 'Project admission receipts are immutable');
END;

ALTER TABLE project_agent_binding ADD COLUMN admission_receipt_id TEXT;
ALTER TABLE project_agent_binding ADD COLUMN charter_approval_id TEXT;

CREATE INDEX idx_project_agent_binding_admission_receipt
    ON project_agent_binding(admission_receipt_id);
CREATE INDEX idx_project_agent_binding_charter_approval
    ON project_agent_binding(charter_approval_id);

-- Existing Genesis Projects already have a fully validated immutable handoff.
-- Copy the packet's frozen canonical fingerprint; do not reconstruct it from
-- Main Chat history during migration.
INSERT INTO project_admission_receipt (
    id, project_id, source_kind, handoff_id,
    initial_charter_approval_id, initial_charter_id,
    initial_charter_revision_id, payload_digest,
    validation_schema_version, validated_at, created_at
)
SELECT
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' ||
        substr('89ab', abs(random()) % 4 + 1, 1) ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))),
    p.id,
    'genesis_handoff',
    h.id,
    a.id,
    c.id,
    a.revision_id,
    json_extract(h.source_revisions_json, '$.request.source_revisions_digest'),
    'forge.project-admission/v1',
    COALESCE(a.consumed_at, h.updated_at, h.created_at),
    COALESCE(a.consumed_at, h.updated_at, h.created_at)
FROM project p
JOIN project_charter c
  ON c.id = p.current_charter_id
 AND c.project_id = p.id
JOIN product_genesis_session g
  ON g.id = c.genesis_session_id
 AND g.project_id = p.id
JOIN agent_handoff h
  ON h.id = g.handoff_id
JOIN project_charter_approval a
  ON a.charter_id = c.id
 AND a.approval_type = 'project_creation'
 AND a.lifecycle = 'consumed'
 AND a.consumed_project_id = p.id
WHERE p.charter_status = 'charter_backed'
  AND p.charter_setup_required = 0
  AND json_valid(h.source_revisions_json)
  AND length(trim(COALESCE(
      json_extract(h.source_revisions_json, '$.request.source_revisions_digest'), ''
  ))) > 0;

-- Adopted Projects intentionally have no Main handoff. Their exact consumed
-- adoption approval and Charter digest are the one-time admission proof.
INSERT INTO project_admission_receipt (
    id, project_id, source_kind, handoff_id,
    initial_charter_approval_id, initial_charter_id,
    initial_charter_revision_id, payload_digest,
    validation_schema_version, validated_at, created_at
)
SELECT
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' ||
        substr('89ab', abs(random()) % 4 + 1, 1) ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))),
    p.id,
    'charter_adoption',
    NULL,
    a.id,
    c.id,
    a.revision_id,
    a.content_digest,
    'forge.project-admission/v1',
    COALESCE(a.consumed_at, a.updated_at),
    COALESCE(a.consumed_at, a.updated_at)
FROM project p
JOIN project_charter c
  ON c.id = p.current_charter_id
 AND c.project_id = p.id
 AND c.genesis_session_id IS NULL
JOIN project_charter_approval a
  ON a.charter_id = c.id
 AND a.approval_type = 'adoption'
 AND a.lifecycle = 'consumed'
 AND a.consumed_project_id = p.id
WHERE p.charter_status = 'charter_backed'
  AND p.charter_setup_required = 0
  AND NOT EXISTS (
      SELECT 1 FROM project_admission_receipt receipt
      WHERE receipt.project_id = p.id
  );

-- Bind every safely inferable row to the stable Project admission and to the
-- consumed approval for the Charter revision that row already names. Active
-- defective replacement rows are completed by the bounded Rust reconciler
-- after migrations, because SQLite cannot reproduce the Profile policy hash.
UPDATE project_agent_binding
SET admission_receipt_id = (
        SELECT receipt.id FROM project_admission_receipt receipt
        WHERE receipt.project_id = project_agent_binding.project_id
    ),
    charter_approval_id = (
        SELECT a.id
        FROM project_charter_approval a
        WHERE a.consumed_project_id = project_agent_binding.project_id
          AND a.lifecycle = 'consumed'
          AND a.charter_id = COALESCE(
              project_agent_binding.charter_id,
              (SELECT p.current_charter_id FROM project p
               WHERE p.id = project_agent_binding.project_id)
          )
          AND a.revision_id = COALESCE(
              project_agent_binding.charter_revision_id,
              (SELECT p.current_charter_revision_id FROM project p
               WHERE p.id = project_agent_binding.project_id)
          )
        ORDER BY a.consumed_at DESC, a.updated_at DESC, a.id DESC
        LIMIT 1
    )
WHERE EXISTS (
    SELECT 1 FROM project_admission_receipt receipt
    WHERE receipt.project_id = project_agent_binding.project_id
);

CREATE TRIGGER project_binding_complete_authority_guard_insert
BEFORE INSERT ON project_agent_binding
WHEN NEW.state = 'active' AND NEW.charter_setup_required = 0
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
WHEN NEW.state = 'active' AND NEW.charter_setup_required = 0
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
