-- Every command a Project Agent runs in its verification checkout is recorded
-- as an observation: what ran, how it ended, a digest of everything it
-- printed, and the session that ran it. A task_validation result the Agent
-- records must cite at least one observation newer than the delivered work,
-- so a settled check always points at something the Agent executed itself
-- rather than at a Task's report of what it did.
CREATE TABLE project_command_observation (
    id                  TEXT PRIMARY KEY,
    project_id          TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    actor_identity_id   TEXT NOT NULL,
    scope_type          TEXT NOT NULL,
    scope_id            TEXT NOT NULL,
    session_id          TEXT NOT NULL,
    turn_id             TEXT,
    program             TEXT NOT NULL,
    args_json           TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(args_json)),
    exit_code           INTEGER,
    success             INTEGER NOT NULL CHECK (success IN (0, 1)),
    output_digest       TEXT NOT NULL,
    stdout_excerpt      TEXT NOT NULL DEFAULT '',
    stderr_excerpt      TEXT NOT NULL DEFAULT '',
    created_at          TEXT NOT NULL
);

CREATE INDEX idx_project_command_observation_actor
    ON project_command_observation(project_id, actor_identity_id, created_at);
