-- Local-only repositories have no remote. Rebuild V029's NOT NULL column
-- without changing repository identities or references from other tables.
PRAGMA foreign_keys = OFF;
PRAGMA legacy_alter_table = ON;

BEGIN IMMEDIATE;

CREATE TABLE repo_new (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    remote_url      TEXT,
    local_path      TEXT,
    work_mode       TEXT NOT NULL DEFAULT 'direct_merge' CHECK(work_mode IN ('direct_merge','pull_request')),
    default_branch  TEXT NOT NULL DEFAULT 'main',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

INSERT INTO repo_new (
    id, project_id, name, remote_url, local_path, work_mode,
    default_branch, created_at, updated_at
)
SELECT
    id, project_id, name, remote_url, local_path, work_mode,
    default_branch, created_at, updated_at
FROM repo;

DROP TABLE repo;
ALTER TABLE repo_new RENAME TO repo;
CREATE INDEX idx_repo_project ON repo(project_id);

UPDATE repo SET remote_url = NULL WHERE trim(remote_url) = '';
-- SQLite's default trim only removes ASCII spaces.
UPDATE repo SET remote_url = NULL
WHERE trim(remote_url, char(9, 10, 11, 12, 13, 32, 133, 160, 5760,
    8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200, 8201, 8202,
    8232, 8233, 8239, 8287, 12288)) = '';

COMMIT;

PRAGMA legacy_alter_table = OFF;
PRAGMA foreign_keys = ON;
