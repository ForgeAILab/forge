CREATE TABLE project_machine_readiness (
    project_id TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    owner_kind TEXT NOT NULL CHECK (owner_kind IN ('server', 'daemon')),
    daemon_id TEXT NOT NULL DEFAULT '',
    runtime_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('ready', 'not_ready', 'unknown')),
    checks_digest TEXT NOT NULL,
    failing_checks_json TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(failing_checks_json)),
    scope_covered TEXT NOT NULL DEFAULT 'full' CHECK (scope_covered IN ('machine', 'full')),
    role TEXT,
    workspace_id TEXT,
    checked_at TEXT,
    next_check_at TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (project_id, owner_kind, daemon_id, runtime_id),
    CHECK ((owner_kind = 'server' AND daemon_id = '' AND runtime_id = '')
        OR (owner_kind = 'daemon' AND daemon_id <> '' AND runtime_id <> ''))
);

-- Older pauses have no machine: they belong to the server host. The runner
-- fills the digest using the same canonical Rust helper as new records.
INSERT INTO project_machine_readiness (
    project_id, owner_kind, daemon_id, runtime_id, status, checks_digest,
    failing_checks_json, role, workspace_id, checked_at, next_check_at
)
SELECT p.id,
    COALESCE(json_extract(p.environment_pause_json, '$.machine.owner_kind'), 'server'),
    COALESCE(json_extract(p.environment_pause_json, '$.machine.daemon_id'), ''),
    COALESCE(json_extract(p.environment_pause_json, '$.machine.runtime_id'), ''),
    'not_ready', '',
    COALESCE((SELECT json_group_array(json_object('name', c.value, 'output_tail',
        COALESCE(json_extract(p.environment_pause_json, '$.output'), '')))
        FROM json_each(p.environment_pause_json, '$.checks') c), '[]'),
    json_extract(p.environment_pause_json, '$.role'),
    json_extract(p.environment_pause_json, '$.workspace_id'),
    COALESCE(json_extract(p.environment_pause_json, '$.last_checked_at'), p.paused_at),
    COALESCE(json_extract(p.environment_pause_json, '$.next_check_at'), p.paused_at)
FROM (SELECT id, paused_at, system_pause_reason, CASE WHEN json_valid(environment_pause_json) THEN environment_pause_json ELSE '{}' END AS environment_pause_json FROM project) p
WHERE p.paused_at IS NOT NULL AND p.system_pause_reason = 'environment_not_ready';
