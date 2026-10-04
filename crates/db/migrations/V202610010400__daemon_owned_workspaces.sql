CREATE TABLE repo_location (
    id                  TEXT PRIMARY KEY,
    -- Like workspace.repo_id after V139, this is a historical repository pin.
    -- Deleting a Repo must retain its locations/placements for owner cleanup.
    repo_id             TEXT NOT NULL,
    owner_kind          TEXT NOT NULL CHECK (owner_kind IN ('server', 'daemon')),
    daemon_id           TEXT REFERENCES daemon(id) ON DELETE RESTRICT,
    runtime_id          TEXT REFERENCES runtime(id) ON DELETE RESTRICT,
    path                TEXT NOT NULL,
    kind                TEXT NOT NULL CHECK (kind IN ('primary_checkout', 'managed_clone', 'shared_mount')),
    is_default          INTEGER NOT NULL DEFAULT 0 CHECK (is_default IN (0, 1)),
    status              TEXT NOT NULL DEFAULT 'unverified' CHECK (status IN ('unverified', 'ready', 'unavailable', 'invalid')),
    last_verified_at    TEXT,
    last_error          TEXT,
    version             INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    CHECK (owner_kind = 'server' OR (daemon_id IS NOT NULL AND runtime_id IS NOT NULL))
);
CREATE INDEX idx_repo_location_repo ON repo_location(repo_id, created_at, id);
CREATE INDEX idx_repo_location_daemon ON repo_location(daemon_id, status);

CREATE TABLE workspace_placement (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL UNIQUE REFERENCES workspace(id) ON DELETE CASCADE,
    task_id             TEXT NOT NULL REFERENCES task(id) ON DELETE CASCADE,
    agent_id            TEXT REFERENCES agent_identity(id) ON DELETE SET NULL,
    owner_kind          TEXT NOT NULL CHECK (owner_kind IN ('server', 'daemon')),
    daemon_id           TEXT REFERENCES daemon(id) ON DELETE RESTRICT,
    runtime_id          TEXT REFERENCES runtime(id) ON DELETE RESTRICT,
    repo_location_id    TEXT NOT NULL REFERENCES repo_location(id) ON DELETE CASCADE,
    execution_daemon_id TEXT REFERENCES daemon(id) ON DELETE SET NULL,
    workspace_handle    TEXT,
    generation          INTEGER NOT NULL DEFAULT 1 CHECK (generation >= 1),
    state               TEXT NOT NULL CHECK (state IN ('reserved', 'preparing', 'ready', 'disconnected', 'cleaning', 'cleaned', 'failed')),
    selected_by         TEXT NOT NULL CHECK (selected_by IN ('scheduler', 'pin', 'inherited', 'backfill')),
    selection_reason    TEXT NOT NULL CHECK (json_valid(selection_reason)),
    reserved_until      TEXT,
    disconnected_at     TEXT,
    failure_cause       TEXT CHECK (failure_cause IN (
                            'placement_unavailable', 'prepare_failed', 'owner_disconnected',
                            'owner_disconnected_timeout', 'owner_lost_execution',
                            'stale_generation', 'wrong_owner'
                        )),
    version             INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    CHECK ((owner_kind = 'server' AND daemon_id IS NULL AND runtime_id IS NULL)
        OR (owner_kind = 'daemon' AND daemon_id IS NOT NULL AND runtime_id IS NOT NULL))
);
CREATE INDEX idx_workspace_placement_daemon_state ON workspace_placement(daemon_id, state);
CREATE INDEX idx_workspace_placement_state ON workspace_placement(state, created_at, id);
CREATE INDEX idx_workspace_placement_location ON workspace_placement(repo_location_id, state);

INSERT INTO repo_location (
    id, repo_id, owner_kind, path, kind, is_default, status, created_at, updated_at
)
SELECT
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
    lower(substr(hex(randomblob(2)), 2, 3)) || '-' ||
    substr('89ab', 1 + (abs(random() % 4)), 1) ||
    lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))),
    id, 'server', local_path, 'primary_checkout', 1, 'ready', created_at, updated_at
FROM repo
WHERE local_path IS NOT NULL;

-- Remote repositories have no local_path; V139 also permits a Workspace whose
-- historical Repo was deleted. Preserve those placements too. Existing Task
-- worktrees live at <root>/<task_id>/<repo_name>, and their cache at
-- <root>/.repos/<repo_id>. Infer only that recorded layout, without scanning the
-- filesystem. An unfamiliar layout retains its recorded worktree as an
-- unverified location; the embedded backend must resolve/verify the source on
-- first use, as it does for managed clones. These are not new ready candidates.
WITH legacy_sources AS (
    SELECT
        w.repo_id,
        CASE WHEN instr(replace(w.worktree_path, '\', '/'), '/' || w.task_id || '/') > 0
            THEN substr(replace(w.worktree_path, '\', '/'), 1,
                        instr(replace(w.worktree_path, '\', '/'), '/' || w.task_id || '/') - 1)
                 || '/.repos/' || w.repo_id
            ELSE w.worktree_path
        END AS path,
        min(w.created_at) AS created_at,
        max(w.updated_at) AS updated_at
    FROM workspace w
    WHERE w.status <> 'cleaned'
      AND NOT EXISTS (SELECT 1 FROM repo_location l WHERE l.repo_id = w.repo_id)
    GROUP BY w.repo_id, path
)
INSERT INTO repo_location (
    id, repo_id, owner_kind, path, kind, is_default, status, created_at, updated_at
)
SELECT
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
    lower(substr(hex(randomblob(2)), 2, 3)) || '-' ||
    substr('89ab', 1 + (abs(random() % 4)), 1) ||
    lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))),
    repo_id, 'server', path, 'managed_clone', 0, 'unverified', created_at, updated_at
FROM legacy_sources;

-- Workspace statuses were introduced in V003 (and preserved through V139):
-- creating -> preparing; ready -> ready; error -> failed; cleaning -> cleaning.
-- Already-cleaned rows have no physical workspace and receive no placement.
INSERT INTO workspace_placement (
    id, workspace_id, task_id, agent_id, owner_kind, repo_location_id,
    workspace_handle, generation, state, selected_by, selection_reason,
    failure_cause, reserved_until, created_at, updated_at
)
SELECT
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
    lower(substr(hex(randomblob(2)), 2, 3)) || '-' ||
    substr('89ab', 1 + (abs(random() % 4)), 1) ||
    lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))),
    w.id, w.task_id,
    (SELECT e.agent_id FROM execution e WHERE e.workspace_id = w.id
     ORDER BY e.created_at DESC, e.id DESC LIMIT 1),
    'server', l.id, w.worktree_path, 1,
    CASE w.status
        WHEN 'creating' THEN 'preparing'
        WHEN 'ready' THEN 'ready'
        WHEN 'error' THEN 'failed'
        WHEN 'cleaning' THEN 'cleaning'
    END,
    'backfill', json_object('rule', 'backfill', 'rejected_candidates', json('[]'),
                            'workspace_status', w.status),
    CASE WHEN w.status = 'error' THEN 'prepare_failed' END,
    CASE WHEN w.status = 'creating' THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+10 minutes') END,
    w.created_at, w.updated_at
FROM workspace w
JOIN repo_location l ON l.repo_id = w.repo_id AND (
    l.kind = 'primary_checkout'
    OR l.path = CASE WHEN instr(replace(w.worktree_path, '\', '/'), '/' || w.task_id || '/') > 0
        THEN substr(replace(w.worktree_path, '\', '/'), 1,
                    instr(replace(w.worktree_path, '\', '/'), '/' || w.task_id || '/') - 1)
             || '/.repos/' || w.repo_id
        ELSE w.worktree_path
    END
)
WHERE w.status <> 'cleaned';
