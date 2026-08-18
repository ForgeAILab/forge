-- V080 seeded Main operating-skill revision forge.main.project-discovery/v2@2
-- but left operating_skill.current_revision_id pointing at @1, so every
-- Main Agent Genesis turn failed the canonical-contract check with
-- "operating skill is not the canonical server contract". Repoint the skill
-- at the revision V080 seeded.
UPDATE operating_skill
SET current_revision_id = 'forge.main.project-discovery/v2@2',
    version = version + 1,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
WHERE id = 'forge.main.project-discovery/v2'
  AND current_revision_id IS NOT 'forge.main.project-discovery/v2@2';
