CREATE TABLE project_machine_readiness (
    project_id TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    owner_kind TEXT NOT NULL CHECK (owner_kind IN ('server', 'daemon')),
    daemon_id TEXT NOT NULL DEFAULT '',
    runtime_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('ready', 'not_ready', 'unknown')),
    checks_digest TEXT NOT NULL,
    failing_checks_json TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(failing_checks_json)),
    check_results_json TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(check_results_json)),
    output_tail TEXT NOT NULL DEFAULT '',
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

-- Infer older pauses from their recorded workspace placement, falling back to
-- the host only when no placement exists. The runner fills canonical digests;
-- malformed Project settings retain an unknown row instead of failing startup.
INSERT INTO project_machine_readiness (
    project_id, owner_kind, daemon_id, runtime_id, status, checks_digest,
    failing_checks_json, output_tail, role, workspace_id, checked_at, next_check_at
)
SELECT p.id,
    COALESCE(json_extract(p.environment_pause_json, '$.machine.owner_kind'), w.owner_kind, 'server'),
    CASE WHEN COALESCE(json_extract(p.environment_pause_json, '$.machine.owner_kind'), w.owner_kind, 'server') = 'daemon' THEN COALESCE(json_extract(p.environment_pause_json, '$.machine.daemon_id'), w.daemon_id, '') ELSE '' END,
    CASE WHEN COALESCE(json_extract(p.environment_pause_json, '$.machine.owner_kind'), w.owner_kind, 'server') = 'daemon' THEN COALESCE(json_extract(p.environment_pause_json, '$.machine.runtime_id'), w.runtime_id, '') ELSE '' END,
    'not_ready', '',
    COALESCE((SELECT json_group_array(json_object('name', c.value, 'output_tail',
        COALESCE(json_extract(p.environment_pause_json, '$.output'), '')))
        FROM json_each(p.environment_pause_json, '$.checks') c), '[]'),
    COALESCE(json_extract(p.environment_pause_json, '$.output'), ''),
    json_extract(p.environment_pause_json, '$.role'),
    json_extract(p.environment_pause_json, '$.workspace_id'),
    COALESCE(json_extract(p.environment_pause_json, '$.last_checked_at'), p.paused_at),
    COALESCE(json_extract(p.environment_pause_json, '$.next_check_at'), p.paused_at)
FROM (SELECT id, paused_at, system_pause_reason, CASE WHEN json_valid(environment_pause_json) THEN environment_pause_json ELSE '{}' END AS environment_pause_json FROM project) p
LEFT JOIN workspace_placement w ON w.workspace_id = json_extract(p.environment_pause_json, '$.workspace_id')
WHERE p.paused_at IS NOT NULL AND p.system_pause_reason = 'environment_not_ready';

CREATE INDEX idx_project_machine_readiness_due ON project_machine_readiness(next_check_at) WHERE status = 'not_ready';
