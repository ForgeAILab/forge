-- V138's Unreleased changelog entry promises: "Workspaces and Workspace
-- leases continue to pin the exact repository used by an execution
-- attempt," and that deleting a Repo no longer cascades into deleting
-- Project data. Tasks were fixed (V138 dropped `task.repo_id`), but
-- `workspace.repo_id` still carried `ON DELETE CASCADE` to `repo(id)`
-- (see V021), so `DELETE /api/v1/repos/{id}` silently destroyed every
-- Workspace row for that Repo -- its branch name, worktree path, and the
-- Workspace-level repository pin the changelog says survives.
--
-- `ON DELETE RESTRICT` is not an option: deleting a Repo must keep
-- succeeding so the owning Project can fall back to
-- repository-setup-required (see `sync_repository_pause` in
-- crates/services/src/task_dispatcher/repo_pause_sync.rs). `ON DELETE SET
-- NULL` is not an option either: it would null the exact pin we're
-- required to retain, and the column is NOT NULL.
--
-- Instead, `workspace.repo_id` follows the same provenance pattern already
-- used by review/PR/evidence/release records elsewhere in the schema:
-- historical repository identity is kept as plain data on the row, not as
-- a live foreign key. The Repo can disappear; the Workspace remembers
-- which Repo it was created against.
--
-- SQLite cannot alter a foreign key in place, so this rebuilds the table:
-- create workspace_new without the repo_id FK, copy every row, drop the
-- old table, rename, and recreate the one index V021 attached
-- (idx_workspace_task). No trigger is defined on `workspace` itself in any
-- prior migration. Every other column, type, and constraint is unchanged.
--
-- `legacy_alter_table` matters here. Since SQLite 3.25 an `ALTER TABLE ...
-- RENAME TO` reparses every other object in the schema so it can rewrite
-- references to the renamed table. `workspace_lease_scope_guard_insert`
-- (V138) is a trigger on `workspace_lease` whose body joins `workspace`,
-- and at the moment of the rename the old `workspace` has already been
-- dropped -- so the reparse fails with
--   error in trigger workspace_lease_scope_guard_insert: no such table: main.workspace
-- and takes the whole migration down. Turning on legacy rename semantics
-- for the rebuild skips that rewrite, which is exactly what we want: the
-- trigger already names `workspace`, and after the rename that name is
-- correct again. This is the standard SQLite table-rebuild procedure.

PRAGMA foreign_keys = OFF;
PRAGMA legacy_alter_table = ON;

CREATE TABLE workspace_new (
    id              TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL UNIQUE REFERENCES task(id) ON DELETE CASCADE,
    repo_id         TEXT NOT NULL,
    worktree_path   TEXT NOT NULL,
    branch          TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('creating', 'ready', 'error', 'cleaning', 'cleaned')),
    before_sha      TEXT,
    error           TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    cleanup_after   TEXT
);

INSERT INTO workspace_new (
    id, task_id, repo_id, worktree_path, branch, status,
    before_sha, error, created_at, updated_at, cleanup_after
)
SELECT
    id, task_id, repo_id, worktree_path, branch, status,
    before_sha, error, created_at, updated_at, cleanup_after
FROM workspace;

DROP TABLE workspace;
ALTER TABLE workspace_new RENAME TO workspace;

PRAGMA legacy_alter_table = OFF;

CREATE INDEX idx_workspace_task ON workspace(task_id);

PRAGMA foreign_keys = ON;
