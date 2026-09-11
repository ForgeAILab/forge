-- Project ownership is also enforced through project_member by the
-- authenticated Project-management surfaces.  Older Projects persisted an
-- owner_id without the corresponding membership, so those owners could read
-- the Project but received a membership 404 when managing it.  Backfill only
-- an existing User and only when no membership exists; ownerless Projects,
-- dangling legacy owner labels, and existing memberships remain unchanged.
INSERT INTO project_member (
    id, project_id, user_id, role, created_at, updated_at
)
SELECT
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' ||
        substr('89ab', 1 + (abs(random()) % 4), 1) ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))),
    project.id,
    project.owner_id,
    'owner',
    project.created_at,
    project.updated_at
FROM project
JOIN user ON user.id = project.owner_id
WHERE project.owner_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM project_member
      WHERE project_member.project_id = project.id
        AND project_member.user_id = project.owner_id
  );
